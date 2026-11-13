package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestOnce(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/exchangeInfo" {
			fmt.Fprint(w, `{"symbols":[]}`)
		} else {
			fmt.Fprint(w, `{}`)
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	config := filepath.Join(dir, "config.toml")
	text := fmt.Sprintf("data_dir = %q\n[poller]\nbase_url = %q\nbrackets_url = %q\n", dir, server.URL, server.URL+"/brackets")
	if err := os.WriteFile(config, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{"-config", config, "-once"}, server.Client()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "raw", "poll-state.json")); err != nil {
		t.Fatal(err)
	}
}
