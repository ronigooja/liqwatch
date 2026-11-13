package manifest

import (
	"path/filepath"
	"testing"
	"time"
)

func TestPacks(t *testing.T) {
	s, e := Open(filepath.Join(t.TempDir(), "manifest.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	p := Pack{Venue: "v", Symbol: "s", Day: 20260101, Path: "v/s/20260101.tar.zst", Bytes: 12, SHA256: "abc", Members: []Member{{Name: "a", Bytes: 2, SHA256: "hash"}}}
	if e = s.RegisterPack(p); e != nil {
		t.Fatal(e)
	}
	p.Bytes = 99
	p.Members = nil
	if e = s.RegisterPack(p); e != nil {
		t.Fatal(e)
	}
	known, e := s.KnownPack(p.Path)
	if e != nil || !known {
		t.Fatal(known, e)
	}
	list, e := s.ListByState(StateClosed)
	if e != nil || len(list) != 1 || list[0].Bytes != 12 || list[0].MemberCount != 1 {
		t.Fatal(list, e)
	}
	id := list[0].ID
	m, e := s.Members(id)
	if e != nil || len(m) != 1 || m[0].SHA256 != "hash" {
		t.Fatal(m, e)
	}
	if s.MarkVerified(id, time.Now()) == nil || s.SetState(id, StateDeleted) == nil {
		t.Fatal("invalid transition accepted")
	}
	for _, st := range []State{StateUploading, StateUploaded} {
		if e = s.SetState(id, st); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.MarkVerified(id, time.Now()); e != nil {
		t.Fatal(e)
	}
	if e = s.SetState(id, StateDeleted); e != nil {
		t.Fatal(e)
	}
	if e = s.IncrSetting("x"); e != nil {
		t.Fatal(e)
	}
	if e = s.IncrSetting("x"); e != nil {
		t.Fatal(e)
	}
	v, e := s.GetSetting("x")
	if e != nil || v != "2" {
		t.Fatal(v, e)
	}
}
