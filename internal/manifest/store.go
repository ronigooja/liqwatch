package manifest

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

var ErrNotFound = errors.New("manifest file not found")

type SQLiteStore struct{ db *sql.DB }

func Open(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(10000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &SQLiteStore{db: db}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLiteStore) init() error {
	_, err := s.db.Exec(`PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS files (
 id INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE, group_name TEXT NOT NULL,
 hour_start INTEGER NOT NULL, msg_count INTEGER NOT NULL, bytes INTEGER NOT NULL,
 sha256 TEXT NOT NULL, closed_at INTEGER NOT NULL, state TEXT NOT NULL,
 remote_path TEXT NOT NULL DEFAULT '', chunk_count INTEGER NOT NULL DEFAULT 0,
 verified_at INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS gaps (stream TEXT NOT NULL, start_ns INTEGER NOT NULL, end_ns INTEGER NOT NULL, cause TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS drops (window_start INTEGER NOT NULL, channel TEXT NOT NULL, count INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);`)
	return err
}

func (s *SQLiteStore) RegisterClosed(f File) error {
	if f.State == "" {
		f.State = StateClosed
	}
	_, err := s.db.Exec(`INSERT INTO files(path,group_name,hour_start,msg_count,bytes,sha256,closed_at,state,remote_path,chunk_count,verified_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(path) DO UPDATE SET group_name=excluded.group_name,hour_start=excluded.hour_start,msg_count=excluded.msg_count,bytes=excluded.bytes,sha256=excluded.sha256,closed_at=excluded.closed_at`,
		f.Path, f.Group, f.HourStart.UnixNano(), f.MsgCount, f.Bytes, f.SHA256, f.ClosedAt.UnixNano(), f.State, f.RemotePath, f.ChunkCount, nanos(f.VerifiedAt))
	return err
}

func (s *SQLiteStore) ListByState(state State) ([]File, error) {
	rows, err := s.db.Query(`SELECT id,path,group_name,hour_start,msg_count,bytes,sha256,closed_at,state,remote_path,chunk_count,verified_at FROM files WHERE state=? ORDER BY id`, state)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []File
	for rows.Next() {
		f, err := scanFile(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) SetState(id int64, state State) error {
	var old State
	if err := s.db.QueryRow(`SELECT state FROM files WHERE id=?`, id).Scan(&old); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if !validTransition(old, state) {
		return fmt.Errorf("invalid manifest transition %s -> %s", old, state)
	}
	_, err := s.db.Exec(`UPDATE files SET state=? WHERE id=?`, state, id)
	return err
}

func (s *SQLiteStore) MarkVerified(id int64, remotePath string, chunks int, at time.Time) error {
	var state State
	if err := s.db.QueryRow(`SELECT state FROM files WHERE id=?`, id).Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if state != StateUploaded {
		return fmt.Errorf("cannot verify state %s", state)
	}
	if _, err := s.db.Exec(`UPDATE files SET state=?,remote_path=?,chunk_count=?,verified_at=? WHERE id=?`, StateVerified, remotePath, chunks, at.UnixNano(), id); err != nil {
		return err
	}
	_, err := s.db.Exec(`INSERT INTO settings(key,value) VALUES('first_verified_date',?) ON CONFLICT(key) DO NOTHING`, at.UTC().Format("2006-01-02"))
	return err
}

func (s *SQLiteStore) SetSHA256(id int64, sum string) error {
	_, err := s.db.Exec(`UPDATE files SET sha256=? WHERE id=? AND sha256=''`, sum, id)
	return err
}

func (s *SQLiteStore) SetUploadMetadata(id int64, remotePath string, chunks int) error {
	_, err := s.db.Exec(`UPDATE files SET remote_path=?,chunk_count=? WHERE id=? AND state IN (?,?)`, remotePath, chunks, id, StateUploading, StateUploaded)
	return err
}

func (s *SQLiteStore) DeletableFiles(age time.Duration, now time.Time) ([]File, error) {
	rows, err := s.db.Query(`SELECT id,path,group_name,hour_start,msg_count,bytes,sha256,closed_at,state,remote_path,chunk_count,verified_at FROM files WHERE state=? AND verified_at>0 AND verified_at<=? ORDER BY id`, StateVerified, now.Add(-age).UnixNano())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []File
	for rows.Next() {
		f, err := scanFile(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) MarkDeleted(id int64, age time.Duration, now time.Time) error {
	var state State
	var verified int64
	if err := s.db.QueryRow(`SELECT state,verified_at FROM files WHERE id=?`, id).Scan(&state, &verified); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if state != StateVerified || verified == 0 || time.Unix(0, verified).Add(age).After(now) {
		return errors.New("file is not eligible for deletion")
	}
	_, err := s.db.Exec(`UPDATE files SET state=? WHERE id=? AND state=? AND verified_at>0 AND verified_at<=?`, StateDeleted, id, StateVerified, now.Add(-age).UnixNano())
	return err
}

func (s *SQLiteStore) AddGap(stream string, startNs, endNs int64, cause string) error {
	_, err := s.db.Exec(`INSERT INTO gaps VALUES(?,?,?,?)`, stream, startNs, endNs, cause)
	return err
}
func (s *SQLiteStore) AddDrop(start int64, channel string, count int64) error {
	_, err := s.db.Exec(`INSERT INTO drops VALUES(?,?,?)`, start, channel, count)
	return err
}
func (s *SQLiteStore) Close() error { return s.db.Close() }

func (s *SQLiteStore) FirstVerifiedDate() (time.Time, error) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM settings WHERE key='first_verified_date'`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse("2006-01-02", v)
	return t, err
}
func nanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}
func validTransition(a, b State) bool {
	order := map[State]int{StateOpen: 0, StateClosed: 1, StateUploading: 2, StateUploaded: 3, StateVerified: 4, StateDeleted: 5}
	return b != StateOpen && order[b] == order[a]+1
}

type scanner interface{ Scan(...any) error }

func scanFile(r scanner) (File, error) {
	var f File
	var hs, ca, va int64
	var st string
	err := r.Scan(&f.ID, &f.Path, &f.Group, &hs, &f.MsgCount, &f.Bytes, &f.SHA256, &ca, &st, &f.RemotePath, &f.ChunkCount, &va)
	f.HourStart = time.Unix(0, hs)
	f.ClosedAt = time.Unix(0, ca)
	f.State = State(st)
	if va != 0 {
		f.VerifiedAt = time.Unix(0, va)
	}
	return f, err
}
