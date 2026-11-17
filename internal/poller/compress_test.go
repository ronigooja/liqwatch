package poller

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestCompressedSinkGolden(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileSinkCompressed(dir, time.Hour, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	r := Record{Venue: "okx", Symbol: "BTC", Channel: "oi", RecvWall: 1794873600000000000, RecvMono: 12, Payload: []byte(`{"x":1}`)}
	s.Write(r)
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	s.Write(r)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "okx/BTC/20261117/oi.p.jsonl.zst"))
	if err != nil {
		t.Fatal(err)
	}
	d, err := zstd.NewReader(bytes.NewReader(b), zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	got, err := io.ReadAll(d)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("{\"v\":2,\"rw\":1794873600000000000,\"rm\":12,\"meta\":null,\"p\":{\"x\":1}}\n"), 2)
	if !bytes.Equal(got, want) {
		t.Fatalf("got %s want %s", got, want)
	}
}
