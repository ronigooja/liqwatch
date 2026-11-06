package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/wicker/wicker/internal/manifest"
	"github.com/wicker/wicker/internal/webdav"
)

type fakeDAV struct {
	mu       sync.Mutex
	files    map[string][]byte
	emptyPUT int // the first N PUTs are accepted but stored empty
}

func (d *fakeDAV) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch r.Method {
	case http.MethodPut:
		b, _ := io.ReadAll(r.Body)
		if d.emptyPUT > 0 {
			d.emptyPUT--
			b = nil
		}
		d.files[r.URL.Path] = b
		w.WriteHeader(http.StatusCreated)
	case http.MethodGet:
		b, ok := d.files[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write(b)
	}
}

func newFake(t *testing.T, empty int) (*webdav.Client, *fakeDAV) {
	d := &fakeDAV{files: map[string][]byte{}, emptyPUT: empty}
	srv := httptest.NewServer(d)
	t.Cleanup(srv.Close)
	c := webdav.New(srv.URL, "u", "p")
	c.Retries = 1
	return c, d
}

func TestPutVerifiedRepairsEmptyStoredObject(t *testing.T) {
	c, d := newFake(t, 1)
	body := []byte(`{"size":1}`)
	if err := putVerified(context.Background(), c, "raw/x.meta.json", body); err != nil {
		t.Fatal(err)
	}
	if string(d.files["/raw/x.meta.json"]) != string(body) {
		t.Fatalf("stored %q", d.files["/raw/x.meta.json"])
	}
}

func TestPutVerifiedFailsWhenStorageKeepsLosingData(t *testing.T) {
	c, _ := newFake(t, 10)
	if err := putVerified(context.Background(), c, "raw/x.meta.json", []byte("abc")); err == nil {
		t.Fatal("must report failure after the retries")
	}
}

func TestEnsureMetaRewritesEmptyObjectFromManifest(t *testing.T) {
	c, d := newFake(t, 0)
	d.files["/raw/20261105/f.jsonl.zst.meta.json"] = nil
	f := manifest.File{RemotePath: "raw/20261105/f.jsonl.zst", ChunkCount: 1, Bytes: 7, SHA256: "ab"}
	if err := ensureMeta(context.Background(), c, f); err != nil {
		t.Fatal(err)
	}
	want := `{"size":7,"sha256":"ab","chunks":["f.jsonl.zst.part0000"]}`
	if got := string(d.files["/raw/20261105/f.jsonl.zst.meta.json"]); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
