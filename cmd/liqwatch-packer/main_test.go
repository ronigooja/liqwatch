package main

import (
	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	root := t.TempDir()
	deleting := filepath.Join(root, "raw/v/BTC/20260101.deleting")
	if err := os.MkdirAll(deleting, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(deleting, "trade.jsonl"), []byte("must not pack"), 0640); err != nil {
		t.Fatal(err)
	}
	day := time.Now().UTC().AddDate(0, 0, -1).Format("20060102")
	for _, sym := range []string{"BTC", "gap"} {
		dir := filepath.Join(root, "raw", "v", sym, day)
		if e := os.MkdirAll(dir, 0755); e != nil {
			t.Fatal(e)
		}
		p := filepath.Join(dir, "trade.a.jsonl")
		if e := os.WriteFile(p, []byte("test"), 0644); e != nil {
			t.Fatal(e)
		}
		old := time.Now().Add(-time.Hour)
		if e := os.Chtimes(p, old, old); e != nil {
			t.Fatal(e)
		}
	}
	cfg := config.Config{DataDir: root, Pack: config.Pack{Unit: "tar_then_zstd", Level: 3, QuietMinutes: 10}}
	if e := run(cfg); e != nil {
		t.Fatal(e)
	}
	if e := run(cfg); e != nil {
		t.Fatal(e)
	}
	s, e := manifest.Open(filepath.Join(root, "manifest", "manifest.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	p, e := s.ListByState(manifest.StateClosed)
	if e != nil || len(p) != 2 {
		t.Fatal(p, e)
	}
}

func TestSeparateOutput(t *testing.T) {
	root, output := t.TempDir(), t.TempDir()
	raw := filepath.Join(root, "raw", "v", "BTC", "20260101", "trade.a.jsonl")
	if err := os.MkdirAll(filepath.Dir(raw), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(raw, []byte("test"), 0640); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(raw, old, old); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{DataDir: root, Pack: config.Pack{Dir: output, Unit: "tar_then_zstd", Level: 3, QuietMinutes: 10}}
	if err := run(cfg); err != nil {
		t.Fatal(err)
	}
	s, err := manifest.Open(filepath.Join(output, "manifest", "manifest.db"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.ListByState(manifest.StateClosed)
	if err != nil {
		t.Fatal(err)
	}
	var members []manifest.Member
	if len(p) == 1 {
		members, err = s.Members(p[0].ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	wantMembers := []manifest.Member{{Name: "trade.a.jsonl", Bytes: 4, SHA256: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"}}
	wantMembers[0].MtimeNS = old.UnixNano()
	if len(p) != 1 || p[0].Path != "v/BTC/20260101.tar.zst" || !reflect.DeepEqual(members, wantMembers) {
		t.Fatalf("manifest = %+v", p)
	}
	files := func(dir string) []string {
		var result []string
		err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				rel, _ := filepath.Rel(dir, path)
				result = append(result, filepath.ToSlash(rel))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(result)
		return result
	}
	if got := files(root); !reflect.DeepEqual(got, []string{"raw/v/BTC/20260101/trade.a.jsonl"}) {
		t.Fatalf("raw tree = %v", got)
	}
	if got := files(output); !reflect.DeepEqual(got, []string{"manifest/manifest.db", "packs/v/BTC/20260101.tar.zst"}) {
		t.Fatalf("output tree = %v", got)
	}
	for _, rel := range []string{"manifest", "packs", "packs/v", "packs/v/BTC"} {
		info, err := os.Stat(filepath.Join(output, rel))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0750 {
			t.Fatalf("directory %s mode=%o, want 750", rel, info.Mode().Perm())
		}
	}
}
