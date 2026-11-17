package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/rawlog"
)

func TestCollectorConfiguredFlush(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(p, []byte("[record]\ncompress=true\nflush='120ms'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(p)
	if err != nil {
		t.Fatal(err)
	}
	w, err := newRecordWriter(cfg, dir, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	defer func() { cancel(); <-w.Done() }()
	at := time.Now().UTC()
	w.Write(rawlog.GroupTrade, rawlog.Record{Symbol: "A", Channel: "trade", RecvWal: at.UnixNano(), RecvMon: 12, CSeq: 7, Payload: []byte(`{"x":1}`)})
	path := filepath.Join(dir, "binance/A", at.Format("20060102"), "trade.jsonl.zst")
	want := []byte(fmt.Sprintf("{\"v\":2,\"wseq\":1,\"cseq\":7,\"rw\":%d,\"rm\":12,\"p\":{\"x\":1}}\n", at.UnixNano()))
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
