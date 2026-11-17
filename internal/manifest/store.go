package manifest

import (
	"database/sql"
	"errors"
	"fmt"
	_ "modernc.org/sqlite"
	"time"
)

var ErrNotFound = errors.New("manifest pack not found")

type SQLiteStore struct{ db *sql.DB }

func Open(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(10000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &SQLiteStore{db}
	if err = s.init(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}
func (s *SQLiteStore) init() error {
	_, err := s.db.Exec(`PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS packs (id INTEGER PRIMARY KEY, venue TEXT NOT NULL, symbol TEXT NOT NULL, day INTEGER NOT NULL, path TEXT NOT NULL UNIQUE, bytes INTEGER NOT NULL, sha256 TEXT NOT NULL, member_count INTEGER NOT NULL, state TEXT NOT NULL, remote_path TEXT NOT NULL DEFAULT '', closed_at INTEGER NOT NULL, verified_at INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS pack_members (pack_id INTEGER NOT NULL REFERENCES packs(id), name TEXT NOT NULL, bytes INTEGER NOT NULL, sha256 TEXT NOT NULL, PRIMARY KEY(pack_id,name));
CREATE TABLE IF NOT EXISTS gaps (stream TEXT NOT NULL, start_ns INTEGER NOT NULL, end_ns INTEGER NOT NULL, cause TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS drops (window_start INTEGER NOT NULL, channel TEXT NOT NULL, count INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);`)
	if err != nil {
		return err
	}
	// Upgrade existing manifests; old members retain unknown (zero) mtime.
	rows, err := s.db.Query(`PRAGMA table_info(pack_members)`)
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def any
		if err = rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); err != nil {
			rows.Close()
			return err
		}
		found = found || name == "mtime_ns"
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if !found {
		_, err = s.db.Exec(`ALTER TABLE pack_members ADD COLUMN mtime_ns INTEGER NOT NULL DEFAULT 0`)
	}
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`CREATE TABLE IF NOT EXISTS local_deletions (pack_id INTEGER NOT NULL REFERENCES packs(id), name TEXT NOT NULL, bytes INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, PRIMARY KEY(pack_id,name));`)
	return err
}
func (s *SQLiteStore) RegisterPack(p Pack) error {
	if p.State == "" {
		p.State = StateClosed
	}
	if p.ClosedAt.IsZero() {
		p.ClosedAt = time.Now().UTC()
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	r, err := tx.Exec(`INSERT INTO packs(venue,symbol,day,path,bytes,sha256,member_count,state,remote_path,closed_at,verified_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(path) DO NOTHING`, p.Venue, p.Symbol, p.Day, p.Path, p.Bytes, p.SHA256, len(p.Members), p.State, p.RemotePath, nanos(p.ClosedAt), nanos(p.VerifiedAt))
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return tx.Commit()
	}
	id, err := r.LastInsertId()
	if err != nil {
		return err
	}
	for _, m := range p.Members {
		if _, err = tx.Exec(`INSERT INTO pack_members(pack_id,name,bytes,sha256,mtime_ns) VALUES(?,?,?,?,?)`, id, m.Name, m.Bytes, m.SHA256, m.MtimeNS); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *SQLiteStore) KnownPack(path string) (bool, error) {
	var n int
	err := s.db.QueryRow(`SELECT count(*) FROM packs WHERE path=?`, path).Scan(&n)
	return n > 0, err
}
func (s *SQLiteStore) ListByState(st State) ([]Pack, error) {
	rows, err := s.db.Query(`SELECT id,venue,symbol,day,path,bytes,sha256,member_count,state,remote_path,closed_at,verified_at FROM packs WHERE state=? ORDER BY id`, st)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Pack
	for rows.Next() {
		var p Pack
		var closed, verified int64
		if err = rows.Scan(&p.ID, &p.Venue, &p.Symbol, &p.Day, &p.Path, &p.Bytes, &p.SHA256, &p.MemberCount, &p.State, &p.RemotePath, &closed, &verified); err != nil {
			return nil, err
		}
		if closed != 0 {
			p.ClosedAt = time.Unix(0, closed)
		}
		if verified != 0 {
			p.VerifiedAt = time.Unix(0, verified)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *SQLiteStore) Members(id int64) ([]Member, error) {
	rows, err := s.db.Query(`SELECT name,bytes,sha256,mtime_ns FROM pack_members WHERE pack_id=? ORDER BY name`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Member
	for rows.Next() {
		var m Member
		if err = rows.Scan(&m.Name, &m.Bytes, &m.SHA256, &m.MtimeNS); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// BeginLocalDeletion atomically persists the exact verified metadata and intent.
func (s *SQLiteStore) BeginLocalDeletion(id int64, members []Member) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`DELETE FROM local_deletions WHERE pack_id=?`, id); err != nil {
		return err
	}
	for _, m := range members {
		if _, err = tx.Exec(`INSERT INTO local_deletions VALUES(?,?,?,?)`, id, m.Name, m.Bytes, m.MtimeNS); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`DELETE FROM settings WHERE key=?`, fmt.Sprintf("local-removing:%d", id)); err != nil {
		return err
	}
	r, err := tx.Exec(`UPDATE packs SET state=? WHERE id=? AND state=?`, StateLocalDeleting, id, StateLocalVerified)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("pack is not locally verified")
	}
	return tx.Commit()
}

func (s *SQLiteStore) DeletionMembers(id int64) ([]Member, error) {
	rows, err := s.db.Query(`SELECT name,bytes,mtime_ns FROM local_deletions WHERE pack_id=? ORDER BY name`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ms []Member
	for rows.Next() {
		var m Member
		if err := rows.Scan(&m.Name, &m.Bytes, &m.MtimeNS); err != nil {
			return nil, err
		}
		ms = append(ms, m)
	}
	return ms, rows.Err()
}
func (s *SQLiteStore) SetState(id int64, st State) error {
	var old State
	err := s.db.QueryRow(`SELECT state FROM packs WHERE id=?`, id).Scan(&old)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	order := map[State]int{StateOpen: 0, StateClosed: 1, StateUploading: 2, StateUploaded: 3, StateVerified: 4, StateDeleted: 5}
	a, ok := order[old]
	b, valid := order[st]
	localDelete := old == StateLocalDeleting && (st == StateLocalDeleted || st == StateLocalVerified) || old == StateLocalDeleted && st == StatePruned
	if !localDelete && (!ok || !valid || b != a+1 || st == StateVerified) {
		return fmt.Errorf("invalid manifest transition %s -> %s", old, st)
	}
	res, err := s.db.Exec(`UPDATE packs SET state=? WHERE id=? AND state=?`, st, id, old)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("concurrent state transition")
	}
	return err
}
func (s *SQLiteStore) SetSHA256(id int64, sum string) error {
	_, err := s.db.Exec(`UPDATE packs SET sha256=? WHERE id=? AND sha256=''`, sum, id)
	return err
}

// MarkLocalVerified never advances an uploaded/remote-verified pack.
func (s *SQLiteStore) MarkLocalVerified(id int64, at time.Time) error {
	if at.IsZero() {
		return errors.New("verification time required")
	}
	r, err := s.db.Exec(`UPDATE packs SET state=?,verified_at=? WHERE id=? AND state IN (?,?)`, StateLocalVerified, nanos(at), id, StateClosed, StateLocalVerified)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("pack is not locally closed/verified")
	}
	return err
}
func (s *SQLiteStore) SetUploadMetadata(id int64, path string) error {
	_, err := s.db.Exec(`UPDATE packs SET remote_path=? WHERE id=? AND state IN (?,?)`, path, id, StateUploading, StateUploaded)
	return err
}
func (s *SQLiteStore) MarkVerified(id int64, at time.Time) error {
	if at.IsZero() {
		return errors.New("verification time required")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	r, err := tx.Exec(`UPDATE packs SET state=?,verified_at=? WHERE id=? AND state=?`, StateVerified, nanos(at), id, StateUploaded)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("pack is not uploaded")
	}
	_, err = tx.Exec(`INSERT INTO settings(key,value) VALUES('first_verified_date',?) ON CONFLICT(key) DO NOTHING`, at.UTC().Format("2006-01-02"))
	if err != nil {
		return err
	}
	return tx.Commit()
}
func (s *SQLiteStore) SetSetting(k, v string) error {
	_, err := s.db.Exec(`INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, k, v)
	return err
}
func (s *SQLiteStore) GetSetting(k string) (string, error) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM settings WHERE key=?`, k).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}
func (s *SQLiteStore) IncrSetting(k string) error {
	_, err := s.db.Exec(`INSERT INTO settings(key,value) VALUES(?, '1') ON CONFLICT(key) DO UPDATE SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT)`, k)
	return err
}
func (s *SQLiteStore) FirstVerifiedDate() (time.Time, error) {
	v, err := s.GetSetting("first_verified_date")
	if err != nil || v == "" {
		return time.Time{}, err
	}
	return time.Parse("2006-01-02", v)
}
func (s *SQLiteStore) AddGap(stream string, start, end int64, cause string) error {
	_, err := s.db.Exec(`INSERT INTO gaps VALUES(?,?,?,?)`, stream, start, end, cause)
	return err
}
func (s *SQLiteStore) AddDrop(start int64, channel string, count int64) error {
	_, err := s.db.Exec(`INSERT INTO drops VALUES(?,?,?)`, start, channel, count)
	return err
}
func (s *SQLiteStore) Close() error { return s.db.Close() }
func nanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}
