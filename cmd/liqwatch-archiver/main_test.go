package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
	"github.com/ronigooja/liqwatch/internal/webdav"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeDAV struct {
	mu    sync.Mutex
	files map[string][]byte
	empty bool
	puts  map[string]int
}

func (d *fakeDAV) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch r.Method {
	case "MKCOL", "PROPFIND":
		w.WriteHeader(201)
	case "PUT":
		b, _ := io.ReadAll(r.Body)
		d.puts[r.URL.Path]++
		if d.empty && strings.HasSuffix(r.URL.Path, ".tar.zst") {
			b = nil
		}
		d.files[r.URL.Path] = b
		w.WriteHeader(201)
	case "HEAD":
		b, ok := d.files[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(b)))
	case "GET":
		b, ok := d.files[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write(b)
	}
}
func setup(t *testing.T, empty bool) (config.Config, *manifest.SQLiteStore, *webdav.Client, *fakeDAV, manifest.Pack) {
	t.Helper()
	root := t.TempDir()
	s, e := manifest.Open(filepath.Join(root, "db"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	d := &fakeDAV{files: map[string][]byte{}, puts: map[string]int{}, empty: empty}
	server := httptest.NewServer(d)
	t.Cleanup(server.Close)
	dav := webdav.New(server.URL, "", "")
	dav.Retries = 1
	cfg := config.Config{DataDir: root, Archive: config.Archive{MaxPackMB: 400, FullVerifyDays: 7}, Pack: config.Pack{Unit: "tar_then_zstd", Level: 3}}
	data := []byte("pack content")
	sum := sha256.Sum256(data)
	p := manifest.Pack{Venue: "v", Symbol: "s", Day: 20260101, Path: "v/s/20260101.tar.zst", Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), Members: []manifest.Member{{Name: "trade.a.jsonl", Bytes: 1, SHA256: "x"}}}
	if e = os.MkdirAll(filepath.Dir(filepath.Join(root, "packs", p.Path)), 0755); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(root, "packs", p.Path), data, 0644); e != nil {
		t.Fatal(e)
	}
	if e = os.MkdirAll(filepath.Join(root, "raw", "v", "s", "20260101"), 0755); e != nil {
		t.Fatal(e)
	}
	if e = s.RegisterPack(p); e != nil {
		t.Fatal(e)
	}
	ps, e := s.ListByState(manifest.StateClosed)
	if e != nil {
		t.Fatal(e)
	}
	p = ps[0]
	if e = s.SetState(p.ID, manifest.StateUploading); e != nil {
		t.Fatal(e)
	}
	return cfg, s, dav, d, p
}
func TestArchiveAndDelete(t *testing.T) {
	cfg, s, dav, d, p := setup(t, false)
	if e := archiveOne(context.Background(), cfg, s, dav, p); e != nil {
		t.Fatal(e)
	}
	if d.puts["/raw/v/s/20260101.tar.zst"] != 1 {
		t.Fatal(d.puts)
	}
	if e := deleteFiles(cfg, s); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(filepath.Join(cfg.DataDir, "packs", p.Path)); e != nil {
		t.Fatal(e)
	}
	if e := verifyFiles(context.Background(), cfg, s, dav); e != nil {
		t.Fatal(e)
	}
	if e := deleteFiles(cfg, s); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(filepath.Join(cfg.DataDir, "packs", p.Path)); !os.IsNotExist(e) {
		t.Fatal(e)
	}
	if _, e := os.Stat(filepath.Join(cfg.DataDir, "raw", "v", "s", "20260101")); !os.IsNotExist(e) {
		t.Fatal(e)
	}
}
func TestZeroPackAndOversize(t *testing.T) {
	cfg, s, dav, _, p := setup(t, true)
	cfg.Archive.MaxPackMB = 0
	if e := archiveOne(context.Background(), cfg, s, dav, p); e == nil {
		t.Fatal("zero remote accepted")
	}
	v, e := s.GetSetting("pack_oversize_count")
	if e != nil || v != "1" {
		t.Fatal(v, e)
	}
	if e = deleteFiles(cfg, s); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(cfg.DataDir, "packs", p.Path)); e != nil {
		t.Fatal(e)
	}
}
func TestRepairMeta(t *testing.T) {
	cfg, s, dav, d, p := setup(t, false)
	if e := archiveOne(context.Background(), cfg, s, dav, p); e != nil {
		t.Fatal(e)
	}
	d.files["/raw/v/s/20260101.meta.json"] = nil
	if e := ensureMeta(context.Background(), cfg, s, dav, p); e != nil {
		t.Fatal(e)
	}
	if len(d.files["/raw/v/s/20260101.meta.json"]) == 0 {
		t.Fatal("not repaired")
	}
	_ = time.Now()
}
