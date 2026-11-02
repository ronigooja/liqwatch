package manifest

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDeletionRequiresVerifiedAge(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "manifest.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now().UTC().Truncate(time.Nanosecond)
	f := File{Path: "20261103/a", Group: "trade", HourStart: now, Bytes: 10, SHA256: "x", ClosedAt: now}
	if err := s.RegisterClosed(f); err != nil {
		t.Fatal(err)
	}
	files, _ := s.ListByState(StateClosed)
	if len(files) != 1 {
		t.Fatal("missing file")
	}
	if err := s.MarkDeleted(files[0].ID, time.Hour, now.Add(2*time.Hour)); err == nil {
		t.Fatal("unverified file deleted")
	}
	if err := s.SetState(files[0].ID, StateUploading); err != nil {
		t.Fatal(err)
	}
	if err := s.SetState(files[0].ID, StateUploaded); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkVerified(files[0].ID, "raw/a", 1, now); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDeleted(files[0].ID, time.Hour, now.Add(30*time.Minute)); err == nil {
		t.Fatal("young file deleted")
	}
	if err := s.MarkDeleted(files[0].ID, time.Hour, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.ListByState(StateDeleted); len(got) != 1 {
		t.Fatal("file was not deleted")
	}
}

func TestTransitionsOnlyForward(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "manifest.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now()
	if err := s.RegisterClosed(File{Path: "x", Group: "g", HourStart: now, ClosedAt: now}); err != nil {
		t.Fatal(err)
	}
	f, _ := s.ListByState(StateClosed)
	if err := s.SetState(f[0].ID, StateVerified); err == nil {
		t.Fatal("skipped transition accepted")
	}
	if err := s.SetState(f[0].ID, StateUploading); err != nil {
		t.Fatal(err)
	}
	if err := s.SetState(f[0].ID, StateClosed); err == nil {
		t.Fatal("backward transition accepted")
	}
}
