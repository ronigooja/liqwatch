package manifest

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestLocalStatesAndLegacyMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE pack_members(pack_id INTEGER NOT NULL, name TEXT NOT NULL, bytes INTEGER NOT NULL, sha256 TEXT NOT NULL, PRIMARY KEY(pack_id,name))`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	members := []Member{{Name: "a.jsonl", Bytes: 3, SHA256: "abc", MtimeNS: 123}}
	if err = s.RegisterPack(Pack{Venue: "v", Symbol: "s", Day: 20260101, Path: "v/s/20260101.tar", Members: members}); err != nil {
		t.Fatal(err)
	}
	ps, err := s.ListByState(StateClosed)
	if err != nil || len(ps) != 1 {
		t.Fatal(ps, err)
	}
	id := ps[0].ID
	got, err := s.Members(id)
	if err != nil || !reflect.DeepEqual(got, members) {
		t.Fatal(got, err)
	}
	if err = s.SetState(id, StateLocalVerified); err == nil {
		t.Fatal("verification bypass")
	}
	if err = s.MarkVerified(id, time.Now()); err == nil {
		t.Fatal("local became remote verified")
	}
	if err = s.MarkLocalVerified(id, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err = s.SetState(id, StateDeleted); err == nil {
		t.Fatal("local became remote deleted")
	}
	if err = s.SetState(id, StateUploading); err == nil {
		t.Fatal("local became uploading")
	}
	if err = s.MarkLocalVerified(id, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err = s.SetState(id, StateLocalDeleted); err == nil {
		t.Fatal("deletion intent bypass")
	}
	if err = s.BeginLocalDeletion(id, members); err != nil {
		t.Fatal(err)
	}
	wantEvidence := []Member{{Name: "a.jsonl", Bytes: 3, MtimeNS: 123}}
	if got, err := s.DeletionMembers(id); err != nil || !reflect.DeepEqual(got, wantEvidence) {
		t.Fatal(got, err)
	}
	if err = s.SetState(id, StateLocalDeleted); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkLocalVerified(id, time.Now()); err == nil {
		t.Fatal("deleted became verified")
	}
	ps, err = s.ListByState(StateLocalDeleted)
	if err != nil || len(ps) != 1 || ps[0].VerifiedAt.IsZero() {
		t.Fatal(ps, err)
	}
}
