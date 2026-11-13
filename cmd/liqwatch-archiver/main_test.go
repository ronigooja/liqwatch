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
	mu      sync.Mutex
	files   map[string][]byte
	empty   bool
	puts    map[string]int
	gets    map[string]int
	heads   map[string]int
	failPut string
}

func (d *fakeDAV) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch r.Method {
	case "MKCOL", "PROPFIND":
		w.WriteHeader(201)
	case "PUT":
		if r.URL.Path == d.failPut {
			w.WriteHeader(500)
			return
		}
		b, _ := io.ReadAll(r.Body)
		d.puts[r.URL.Path]++
		if d.empty && strings.HasSuffix(r.URL.Path, ".tar.zst") {
			b = nil
		}
		d.files[r.URL.Path] = b
		w.WriteHeader(201)
	case "HEAD":
		d.heads[r.URL.Path]++
		b, ok := d.files[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(b)))
	case "GET":
		d.gets[r.URL.Path]++
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
	d := &fakeDAV{files: map[string][]byte{}, puts: map[string]int{}, gets: map[string]int{}, heads: map[string]int{}, empty: empty}
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
	if n, e := deleteFiles(cfg, s); e != nil || n != 0 {
		t.Fatal(n, e)
	}
	if _, e := os.Stat(filepath.Join(cfg.DataDir, "packs", p.Path)); e != nil {
		t.Fatal(e)
	}
	if n, e := verifyFiles(context.Background(), cfg, s, dav); e != nil || n != 0 {
		t.Fatal(n, e)
	}
	if n, e := deleteFiles(cfg, s); e != nil || n != 0 {
		t.Fatal(n, e)
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
	if n, e := deleteFiles(cfg, s); e != nil || n != 0 {
		t.Fatal(n, e)
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

func TestSampleSizeAndDeletion(t *testing.T) {
	cfg, s, dav, d, p := setup(t, false)
	if err := archiveOne(context.Background(), cfg, s, dav, p); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSetting("first_verified_date", time.Now().UTC().AddDate(0, 0, -30).Format("2006-01-02")); err != nil {
		t.Fatal(err)
	}
	cfg.Archive.VerifySamplePercent = 0
	remote := "/" + remotePath(p)
	beforeGet, beforeHead := d.gets[remote], d.heads[remote]
	if n, err := verifyFiles(context.Background(), cfg, s, dav); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if d.gets[remote] != beforeGet || d.heads[remote] != beforeHead+1 {
		t.Fatal("expected only HEAD", d.gets, d.heads)
	}
	// A second uploaded pack with a wrong remote size remains uploaded and its files remain.
	p2 := p
	p2.ID = 0
	p2.Symbol = "bad"
	p2.Path = "v/bad/20260101.tar.zst"
	p2.State = manifest.StateClosed
	if err := os.MkdirAll(filepath.Dir(filepath.Join(cfg.DataDir, "packs", p2.Path)), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.DataDir, "packs", p2.Path), []byte("pack content"), 0644); err != nil {
		t.Fatal(err)
	}
	raw2 := filepath.Join(cfg.DataDir, "raw", "v", "bad", "20260101")
	if err := os.MkdirAll(raw2, 0755); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterPack(p2); err != nil {
		t.Fatal(err)
	}
	ps, err := s.ListByState(manifest.StateClosed)
	if err != nil || len(ps) != 1 {
		t.Fatal(ps, err)
	}
	p2 = ps[0]
	if err = s.SetState(p2.ID, manifest.StateUploading); err != nil {
		t.Fatal(err)
	}
	if err = archiveOne(context.Background(), cfg, s, dav, p2); err != nil {
		t.Fatal(err)
	}
	d.files["/"+remotePath(p2)] = []byte("bad")
	if n, err := verifyFiles(context.Background(), cfg, s, dav); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if n, err := deleteFiles(cfg, s); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if _, err = os.Stat(filepath.Join(cfg.DataDir, "packs", p.Path)); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(cfg.DataDir, "packs", p2.Path)); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(raw2); err != nil {
		t.Fatal(err)
	}
	uploaded, err := s.ListByState(manifest.StateUploaded)
	if err != nil || len(uploaded) != 1 || uploaded[0].ID != p2.ID {
		t.Fatal(uploaded, err)
	}
}

func TestFullAndSampledDownloads(t *testing.T) {
	cfg, s, dav, d, p := setup(t, false)
	if err := archiveOne(context.Background(), cfg, s, dav, p); err != nil {
		t.Fatal(err)
	}
	remote := "/" + remotePath(p)
	for _, tc := range []struct {
		full      bool
		percent   int
		downloads int
	}{{true, 0, 1}, {false, 100, 1}, {false, 0, 0}} {
		cfg.Archive.VerifySamplePercent = tc.percent
		before := d.gets[remote]
		if err := verifyOne(context.Background(), cfg, s, dav, p, tc.full); err != nil {
			t.Fatal(err)
		}
		if got := d.gets[remote] - before; got != tc.downloads {
			t.Fatalf("full=%v sample=%d: downloads=%d", tc.full, tc.percent, got)
		}
	}
}

func TestRunContinuesAfterUploadFailure(t *testing.T) {
	root := t.TempDir()
	d := &fakeDAV{files: map[string][]byte{}, puts: map[string]int{}, gets: map[string]int{}, heads: map[string]int{}, failPut: "/raw/v/a/20260101.tar.zst"}
	server := httptest.NewServer(d)
	defer server.Close()
	if err := os.MkdirAll(filepath.Join(root, "manifest"), 0755); err != nil {
		t.Fatal(err)
	}
	db := filepath.Join(root, "manifest", "manifest.db")
	s, err := manifest.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("pack")
	sum := sha256.Sum256(data)
	for _, sym := range []string{"a", "b"} {
		p := manifest.Pack{Venue: "v", Symbol: sym, Day: 20260101, Path: filepath.Join("v", sym, "20260101.tar.zst"), Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
		if err = os.MkdirAll(filepath.Dir(filepath.Join(root, "packs", p.Path)), 0755); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, "packs", p.Path), data, 0644); err != nil {
			t.Fatal(err)
		}
		if err = s.RegisterPack(p); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	cfg := config.Config{DataDir: root, Archive: config.Archive{RemoteRoot: server.URL, MaxPackMB: 400, FullVerifyDays: 7, VerifySamplePercent: 10}, Pack: config.Pack{Level: 3}}
	if err = run(context.Background(), cfg); err == nil {
		t.Fatal("expected run failure")
	}
	if d.puts["/raw/v/b/20260101.tar.zst"] != 1 {
		t.Fatal(d.puts)
	}
	s, err = manifest.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	pending, err := s.ListByState(manifest.StateUploading)
	if err != nil || len(pending) != 1 || pending[0].Symbol != "a" {
		t.Fatal(pending, err)
	}
	deleted, err := s.ListByState(manifest.StateDeleted)
	if err != nil || len(deleted) != 1 || deleted[0].Symbol != "b" {
		t.Fatal(deleted, err)
	}
}
