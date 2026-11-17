package rawlog

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// TestWriterMeasurement is an opt-in build-host experiment, not a memory assertion.
// Run each count in a fresh process: LIQWATCH_MEASURE_FILES=4096 (or 13380)
// go test -count=1 -run '^TestWriterMeasurement$' -v ./internal/rawlog.
func TestWriterMeasurement(t *testing.T) {
	count := 0
	fmt.Sscan(os.Getenv("LIQWATCH_MEASURE_FILES"), &count)
	if count == 0 {
		t.Skip("opt-in build-host measurement")
	}
	w, err := NewWriter(Options{Dir: t.TempDir(), Compress: true, QueueSize: 10000, MaxOpenFiles: 4096, FlushEvery: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	stop := run(w)
	at := time.Now()
	for i := 0; i < count; i++ {
		w.Write(GroupTrade, Record{Symbol: fmt.Sprintf("S%d", i), Channel: "trade", RecvWal: at.UnixNano(), Payload: []byte(`{"price":"123.45","qty":"1.23"}`)})
	}
	// A final sentinel ensures all prior files were opened before RSS is sampled.
	w.Write(GroupTrade, Record{Symbol: "SENTINEL", Channel: "trade", RecvWal: at.UnixNano(), Payload: bytes.Repeat([]byte(" "), 8192)})
	p := filepath.Join(w.opts.Dir, "binance/SENTINEL", at.UTC().Format("20060102"), "trade.jsonl.zst")
	deadline := time.Now().Add(time.Minute)
	for {
		st, e := os.Stat(p)
		if e == nil && st.Size() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sentinel timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(status), "\n") {
		if strings.HasPrefix(line, "VmHWM:") || strings.HasPrefix(line, "VmRSS:") {
			t.Log(line)
		}
	}
	t.Logf("files=%d max_open=4096 encoder_count=1 heap_alloc=%d heap_sys=%d", count, m.HeapAlloc, m.HeapSys)
	stop()
}

// Synthetic frame-policy comparison: 60 five-second batches, 100 whole v2 lines
// per batch. Not an exchange-data ratio or a recorder CPU measurement.
func TestFramePolicyMeasurement(t *testing.T) {
	if os.Getenv("LIQWATCH_MEASURE_FILES") == "" {
		t.Skip("opt-in build-host measurement")
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	encProbe, err := NewFrameEncoder(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (FrameWriter{Encoder: encProbe, Dst: &bytes.Buffer{}}).Write(bytes.Repeat([]byte("real-looking envelope payload\n"), 4096)); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	t.Logf("encoder_warmed_heap_delta=%d", int64(after.HeapAlloc)-int64(before.HeapAlloc))
	runtime.KeepAlive(encProbe)
	var batches [][]byte
	for b := 0; b < 60; b++ {
		var p bytes.Buffer
		for i := 0; i < 100; i++ {
			fmt.Fprintf(&p, "{\"v\":2,\"wseq\":%d,\"cseq\":%d,\"rw\":%d,\"rm\":%d,\"p\":{\"s\":\"BTCUSDT\",\"p\":\"%d.12\",\"q\":\"0.123\",\"T\":%d}}\n", b*100+i, b*100+i, 1794873600000000000+int64(b)*5000000000, int64(b)*5000000000, 90000+i%17, 1794873600000+b*5000)
		}
		batches = append(batches, p.Bytes())
	}
	for _, mode := range []string{"stream_flush_5s", "independent_5s", "independent_4KiB"} {
		enc, err := NewFrameEncoder(1)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		enc.Reset(&out)
		raw := 0
		for _, p := range batches {
			raw += len(p)
			switch mode {
			case "stream_flush_5s":
				enc.Write(p)
				enc.Flush()
			case "independent_5s":
				if _, err = (FrameWriter{Encoder: enc, Dst: &out}).Write(p); err != nil {
					t.Fatal(err)
				}
			case "independent_4KiB":
				for len(p) > 0 {
					n := min(4096, len(p))
					if _, err = (FrameWriter{Encoder: enc, Dst: &out}).Write(p[:n]); err != nil {
						t.Fatal(err)
					}
					p = p[n:]
				}
			}
		}
		if mode == "stream_flush_5s" {
			enc.Close()
		}
		d, err := zstd.NewReader(bytes.NewReader(out.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		d.Close()
		t.Logf("mode=%s records=6000 raw=%d compressed=%d ratio=%.4f", mode, raw, out.Len(), float64(raw)/float64(out.Len()))
	}
}
