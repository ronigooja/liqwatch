package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/poller"
)

func TestPollerConfiguredFlush(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(p, []byte("[record]\ncompress=true\nflush='120ms'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(p)
	if err != nil {
		t.Fatal(err)
	}
	s, err := newRecordSink(dir, cfg.Record)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	at := time.Now().UTC()
	s.Write(poller.Record{Venue: "binance", Symbol: "A", Channel: "test", RecvWall: at.UnixNano(), RecvMono: 12, Payload: []byte(`{"x":1}`)})
	path := filepath.Join(dir, "binance/A", at.Format("20060102"), "test.p.jsonl.zst")
	want := []byte(fmt.Sprintf("{\"v\":2,\"rw\":%d,\"rm\":12,\"meta\":null,\"p\":{\"x\":1}}\n", at.UnixNano()))
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		b, _ := os.ReadFile(path)
		if len(b) > 0 {
			got, err := dec.DecodeAll(b, nil)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("whole record %q: %v", got, err)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("configured tick did not publish before close")
}
