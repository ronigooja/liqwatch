package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
)

func TestStoredTarRemoteRoundTrip(t *testing.T) {
	root := t.TempDir()
	d := &fakeDAV{files: map[string][]byte{}, puts: map[string]int{}, gets: map[string]int{}, heads: map[string]int{}}
	server := httptest.NewServer(d)
	defer server.Close()
	data := []byte("stored tar fixture")
	sum := sha256.Sum256(data)
	p := manifest.Pack{Venue: "v", Symbol: "s", Day: 20260101, Path: "v/s/20260101.tar", Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), Members: []manifest.Member{{Name: "trade.a.jsonl.zst", Bytes: 7, SHA256: "stored-hash"}}}
	path := filepath.Join(root, "packs", p.Path)
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "manifest"), 0750); err != nil {
		t.Fatal(err)
	}
	s, err := manifest.Open(filepath.Join(root, "manifest", "manifest.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RegisterPack(p); err != nil {
		t.Fatal(err)
	}
	s.Close()
	cfg := config.Config{DataDir: root, Pack: config.Pack{Unit: "tar_then_zstd", Level: 3}, Archive: config.Archive{RemoteRoot: server.URL, MaxPackMB: 400, FullVerifyDays: 7}}
	if err = run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	meta := fmt.Sprintf("{\"size\":%d,\"sha256\":%q,\"members\":[{\"name\":\"trade.a.jsonl.zst\",\"bytes\":7,\"sha256\":\"stored-hash\"}],\"unit\":\"stored_tar\",\"level\":0}", len(data), p.SHA256)
	want := map[string][]byte{"/raw/v/s/20260101.tar": data, "/raw/v/s/20260101.meta.json": []byte(meta)}
	if !reflect.DeepEqual(d.files, want) {
		t.Fatalf("remote=%q want=%q", d.files, want)
	}
}
