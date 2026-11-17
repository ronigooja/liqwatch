package rawlog

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/ronigooja/liqwatch/internal/config"
)

// decodePrefix deliberately uses the independent library, not the production reader.
func decodePrefix(b []byte) ([]byte, error) {
	d, err := zstd.NewReader(bytes.NewReader(b), zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	defer d.Close()
	p, err := io.ReadAll(d)
	if i := bytes.LastIndexByte(p, '\n'); i >= 0 {
		p = p[:i+1]
	} else {
		p = nil
	}
	return p, err
}

func TestCompressedWholeFileGolden(t *testing.T) {
	for _, max := range []int{1, 4096} {
		t.Run(fmt.Sprint(max), func(t *testing.T) {
			dir := t.TempDir()
			w, err := NewWriter(Options{Dir: dir, Compress: true, MaxOpenFiles: max})
			if err != nil {
				t.Fatal(err)
			}
			stop := run(w)
			at := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
			for _, sym := range []string{"A", "B", "A"} {
				w.Write(GroupTrade, Record{Symbol: sym, Channel: "trade", RecvWal: at.UnixNano(), RecvMon: 12, CSeq: 7, Payload: []byte(`{"x":1}`)})
			}
			stop()
			p := filepath.Join(dir, "binance/A/20261117/trade.jsonl.zst")
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodePrefix(b)
			if err != nil {
				t.Fatal(err)
			}
			line := func(n int) string {
				return fmt.Sprintf("{\"v\":2,\"wseq\":%d,\"cseq\":7,\"rw\":1794873600000000000,\"rm\":12,\"p\":{\"x\":1}}\n", n)
			}
			want := line(1) + line(3)
			if string(got) != want {
				t.Fatalf("got %s want %s", got, want)
			}
			// Every byte cut must expose only a whole-record prefix. A partial
			// frame is reported by zstd, while exact frame boundaries are valid.
			for i := 1; i < len(b); i++ {
				prefix, tail := decodePrefix(b[:i])
				if !strings.HasPrefix(want, string(prefix)) {
					t.Fatalf("offset %d: %q (%v)", i, prefix, tail)
				}
				if len(prefix) == len(got) && tail == nil {
					t.Fatalf("missing frame termination detected at %d", i)
				}
			}
		})
	}
}

func TestCompressedTickPublishesBeforeClose(t *testing.T) {
	dir := t.TempDir()
	pConfig := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(pConfig, []byte("[record]\ncompress=true\nflush='10ms'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(pConfig)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(Options{Dir: dir, Compress: cfg.Record.Compress, FlushEvery: cfg.Record.FlushEvery(5 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	defer func() { cancel(); <-w.Done() }()
	at := time.Now().UTC()
	w.Write(GroupTrade, Record{Symbol: "A", Channel: "trade", RecvWal: at.UnixNano(), Payload: []byte(`{"x":1}`)})
	p := filepath.Join(dir, "binance/A", at.Format("20060102"), "trade.jsonl.zst")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b, _ := os.ReadFile(p)
		if len(b) > 0 {
			got, err := decodePrefix(b)
			want := fmt.Sprintf("{\"v\":2,\"wseq\":1,\"cseq\":0,\"rw\":%d,\"rm\":0,\"p\":{\"x\":1}}\n", at.UnixNano())
			if err == nil && string(got) == want {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("flush tick did not publish a finished frame")
}

func TestPlainByteGolden(t *testing.T) {
	w, err := NewWriter(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	stop := run(w)
	w.Write(GroupTrade, Record{Symbol: "A", Channel: "trade", RecvWal: 1794873600000000000, RecvMon: 12, CSeq: 7, Payload: []byte(`{"x":1}`)})
	stop()
	b, err := os.ReadFile(filepath.Join(w.opts.Dir, "binance/A/20261117/trade.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "{\"v\":2,\"wseq\":1,\"cseq\":7,\"rw\":1794873600000000000,\"rm\":12,\"p\":{\"x\":1}}\n" {
		t.Fatalf("%q", b)
	}
}

func TestEveryFrameTruncation(t *testing.T) {
	enc, err := NewFrameEncoder(1)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	line := []byte("{\"v\":2,\"p\":{\"x\":1}}\n")
	w := FrameWriter{Encoder: enc, Dst: &b}
	if _, err := w.Write(line); err != nil {
		t.Fatal(err)
	}
	boundary := b.Len()
	if _, err := w.Write(line); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= b.Len(); i++ {
		got, err := decodePrefix(b.Bytes()[:i])
		if !bytes.HasPrefix(bytes.Repeat(line, 2), got) {
			t.Fatalf("offset %d garbage %q", i, got)
		}
		if i == boundary || i == b.Len() {
			if err != nil {
				t.Fatalf("complete frame %d: %v", i, err)
			}
		} else if err == nil {
			t.Fatalf("offset %d missing truncated-tail indication", i)
		}
	}
}
