package main

import (
	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	root := t.TempDir()
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
