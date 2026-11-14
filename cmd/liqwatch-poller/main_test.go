package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

func TestUnknownVenueAndUsage(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(config, []byte(fmt.Sprintf("data_dir = %q\n", dir)), 0600); err != nil {
		t.Fatal(err)
	}
	err := run(context.Background(), []string{"-config", config, "-venue", "nosuch", "-once"}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), `unknown venue "nosuch"`) || !strings.Contains(err.Error(), "binance") {
		t.Fatal(err)
	}
	var out bytes.Buffer
	old := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w
	err = run(context.Background(), []string{"-h"}, http.DefaultClient)
	w.Close()
	os.Stderr = old
	out.ReadFrom(r)
	if err == nil || !strings.Contains(out.String(), "registered venues: ") || !strings.Contains(out.String(), "binance") {
		t.Fatalf("usage %q err %v", out.String(), err)
	}
}

func TestBinanceStateFileAndBadVenueTable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"symbols":[]}`) }))
	defer server.Close()
	dir := t.TempDir()
	config := filepath.Join(dir, "config.toml")
	text := fmt.Sprintf("data_dir = %q\n[collect]\nmap_path = %q\n[poller]\nbase_url = %q\n", dir, filepath.Join("..", "..", "config", "venue-map.toml"), server.URL)
	if err := os.WriteFile(config, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	// binance does not read the map, keeps poll-state.json
	if err := run(context.Background(), []string{"-config", config, "-once"}, server.Client()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "raw", "poll-state.json")); err != nil {
		t.Fatal(err)
	}
	// invalid -config venue table is rejected
	bad := text + "[poller.venues.nope]\nrps = 1\n"
	if err := os.WriteFile(config, []byte(bad), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{"-config", config, "-once"}, server.Client()); err == nil {
		t.Fatal("unknown poller venue accepted")
	}
}
