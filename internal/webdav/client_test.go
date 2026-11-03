package webdav

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestPutResumeAndBasicAuth(t *testing.T) {
	var mu sync.Mutex
	files := map[string][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != "user" || p != "pass" {
			http.Error(w, "unauthorized", 401)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if r.Method == "MKCOL" {
			w.WriteHeader(201)
			return
		}
		if r.Method == "PROPFIND" {
			w.WriteHeader(207)
			return
		}
		if r.Method == "HEAD" {
			b, ok := files[r.URL.Path]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", fmtInt(len(b)))
			return
		}
		if r.Method == "PUT" {
			b, _ := io.ReadAll(r.Body)
			files[r.URL.Path] = b
			w.WriteHeader(201)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	c := New(srv.URL, "user", "pass")
	ctx := context.Background()
	if err := c.MkdirAll(ctx, "raw/20261103"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "raw/20261103/a.part0000", strings.NewReader("abc"), 3); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Stat(ctx, "raw/20261103/a.part0000"); err != nil || n != 3 {
		t.Fatalf("stat %d %v", n, err)
	}
}

func TestMkdirAllFailsWhenParentMissing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "MKCOL" {
			w.WriteHeader(405)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	c := New(srv.URL+"/gone/wicker", "u", "p")
	c.Retries = 1
	if err := c.MkdirAll(context.Background(), "raw/20261103"); err == nil {
		t.Fatal("MkdirAll must fail when the directory cannot be created")
	}
}

func fmtInt(n int) string { return strconv.Itoa(n) }
