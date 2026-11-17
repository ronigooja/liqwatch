package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ronigooja/liqwatch/internal/manifest"
	"github.com/ronigooja/liqwatch/internal/pack"
)

func resetDeletion(t *testing.T) {
	t.Helper()
	old, remove := deletionPoint, removeDeletion
	t.Cleanup(func() { deletionPoint = old; removeDeletion = remove })
}

func TestDeletionCrashConverges(t *testing.T) {
	for _, point := range []string{"state", "rename", "recheck", "partial", "removed"} {
		t.Run(point, func(t *testing.T) {
			resetDeletion(t)
			cfg, s, _, raw, packed := localFixture(t, pack.StoredTar, true)
			deletionPoint = func(p string) error {
				if p == point {
					return errors.New("injected crash")
				}
				return nil
			}
			if point == "partial" {
				removeDeletion = func(dir string) error {
					entries, err := os.ReadDir(dir)
					if err != nil {
						return err
					}
					if err = os.Remove(filepath.Join(dir, entries[0].Name())); err != nil {
						return err
					}
					return errors.New("injected partial removal")
				}
			}
			if err := runLocal(context.Background(), cfg, s, localNow); err == nil {
				t.Fatal("crash not injected")
			}
			ps, err := s.ListByState(manifest.StateLocalDeleting)
			if err != nil || len(ps) != 1 {
				t.Fatalf("intent missing: %+v %v", ps, err)
			}
			deletionPoint = func(string) error { return nil }
			removeDeletion = os.RemoveAll
			for i := 0; i < 2; i++ {
				if err := runLocal(context.Background(), cfg, s, localNow); err != nil {
					t.Fatal(err)
				}
			}
			for _, path := range []string{raw, raw + ".deleting"} {
				if ok, err := exists(path); err != nil || ok {
					t.Fatal(path, ok, err)
				}
			}
			if _, err = os.Stat(packed); err != nil {
				t.Fatal(err)
			}
			ps, err = s.ListByState(manifest.StateLocalDeleted)
			if err != nil || len(ps) != 1 {
				t.Fatal(ps, err)
			}
		})
	}
}

func TestDeletionRenameProtectsNewPath(t *testing.T) {
	resetDeletion(t)
	cfg, s, _, raw, _ := localFixture(t, pack.StoredTar)
	deletionPoint = func(point string) error {
		if point == "rename" {
			writeLocal(t, filepath.Join(raw, "new.jsonl"), "new data")
		}
		return nil
	}
	if err := runLocal(context.Background(), cfg, s, localNow); err != nil {
		t.Fatal(err)
	}
	want := []string{"v/", "v/s/", "v/s/20261116/", "v/s/20261116/new.jsonl=new data"}
	if got := listing(t, filepath.Join(cfg.DataDir, "raw")); !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%q want=%q", got, want)
	}
}

func TestDeletionRecheckAndRestore(t *testing.T) {
	for _, change := range []string{"growth", "appeared", "vanished", "mtime", "collision"} {
		t.Run(change, func(t *testing.T) {
			resetDeletion(t)
			cfg, s, p, raw, _ := localFixture(t, pack.StoredTar)
			ms, err := s.Members(p.ID)
			if err != nil {
				t.Fatal(err)
			}
			deletionPoint = func(point string) error {
				if point != "rename" {
					return nil
				}
				dir := raw + ".deleting"
				file := filepath.Join(dir, ms[0].Name)
				switch change {
				case "growth", "collision":
					writeLocal(t, file, "changed unverified bytes")
				case "appeared":
					writeLocal(t, filepath.Join(dir, "extra.jsonl"), "extra")
				case "vanished":
					if err := os.Remove(file); err != nil {
						t.Fatal(err)
					}
				case "mtime":
					if err := os.Chtimes(file, localNow, localNow); err != nil {
						t.Fatal(err)
					}
				}
				if change == "collision" {
					if err := os.MkdirAll(raw, 0750); err != nil {
						t.Fatal(err)
					}
				}
				return nil
			}
			got, err := localLog(t, func() error { return runLocal(context.Background(), cfg, s, localNow) })
			if err == nil || !strings.Contains(got, "WARNING local deletion blocked") || !strings.Contains(got, "changed deletion") {
				t.Fatal(got, err)
			}
			path := raw
			if change == "collision" {
				path += ".deleting"
				if !strings.Contains(got, "original exists") {
					t.Fatal(got)
				}
				entries, err := os.ReadDir(raw)
				if err != nil || len(entries) != 0 {
					t.Fatal(entries, err)
				}
			}
			if ok, err := exists(path); err != nil || !ok {
				t.Fatal(path, ok, err)
			}
			state := manifest.StateLocalVerified
			if change == "collision" {
				state = manifest.StateLocalDeleting
			}
			ps, err := s.ListByState(state)
			if err != nil || len(ps) != 1 {
				t.Fatal(ps, err)
			}
		})
	}
}

func TestRecoveryNeverMarksExistingDeletingDeleted(t *testing.T) {
	resetDeletion(t)
	cfg, s, _, raw, _ := localFixture(t, pack.StoredTar)
	deletionPoint = func(point string) error {
		if point == "rename" {
			return errors.New("crash")
		}
		return nil
	}
	if err := runLocal(context.Background(), cfg, s, localNow); err == nil {
		t.Fatal("expected crash")
	}
	writeLocal(t, filepath.Join(raw+".deleting", "late.jsonl"), "unverified")
	writeLocal(t, filepath.Join(raw, "new.jsonl"), "new path")
	deletionPoint = func(string) error { return nil }
	if err := runLocal(context.Background(), cfg, s, localNow); err == nil {
		t.Fatal("recovery accepted changed directory")
	}
	ps, err := s.ListByState(manifest.StateLocalDeleting)
	if err != nil || len(ps) != 1 {
		t.Fatal(ps, err)
	}
	if b, err := os.ReadFile(filepath.Join(raw+".deleting", "late.jsonl")); err != nil || string(b) != "unverified" {
		t.Fatal(string(b), err)
	}
}
