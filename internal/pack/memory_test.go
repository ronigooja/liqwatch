package pack

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// Fixture creation is deliberately outside allocation and heap measurements.
func memoryFixture(tb testing.TB, count, size int) (string, Options) {
	tb.Helper()
	root := tb.TempDir()
	dir := filepath.Join(root, "v", "s", "20260101")
	if err := os.MkdirAll(dir, 0750); err != nil {
		tb.Fatal(err)
	}
	block := make([]byte, 32<<10)
	// Deterministic, non-zero data; repeated blocks model compressible raw logs.
	var x uint32 = 1
	for i := range block {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		block[i] = byte(x)
	}
	for i := 0; i < count; i++ {
		p := filepath.Join(dir, fmt.Sprintf("trade.%04d.jsonl", i))
		f, err := os.Create(p)
		if err != nil {
			tb.Fatal(err)
		}
		for left := size; left > 0; {
			n := min(left, len(block))
			if _, err := f.Write(block[:n]); err != nil {
				tb.Fatal(err)
			}
			left -= n
		}
		if err := f.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	return root, Options{PackRoot: filepath.Join(root, "packs"), Now: time.Now().Add(24 * time.Hour)}
}

// Samples live Go heap, not RSS/cgroup usage. Include the final heap sample too.
func measureHeap(fn func()) uint64 {
	runtime.GC()
	var initial runtime.MemStats
	runtime.ReadMemStats(&initial)
	peak := initial.HeapAlloc
	stop, done := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	sample := func() {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		mu.Lock()
		peak = max(peak, m.HeapAlloc)
		mu.Unlock()
	}
	go func() {
		defer close(done)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				sample()
			case <-stop:
				return
			}
		}
	}()
	fn()
	close(stop)
	<-done
	sample()
	return peak - initial.HeapAlloc
}

func BenchmarkPackMemory(b *testing.B) {
	for _, tc := range []struct {
		name        string
		count, size int
	}{
		{"tiny", 1000, 4096}, {"64MiB", 2, 64 << 20}, {"256MiB", 2, 256 << 20},
	} {
		for _, unit := range []string{TarThenZstd, ZstdThenTar} {
			b.Run(tc.name+"/"+unit, func(b *testing.B) {
				raw, opts := memoryFixture(b, tc.count, tc.size)
				opts.Unit = unit
				b.ReportAllocs()
				b.SetBytes(int64(tc.count * tc.size))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					opts.PackRoot = filepath.Join(raw, fmt.Sprintf("packs-%d", i))
					b.StartTimer()
					peak := measureHeap(func() {
						if _, err := Pack(raw, "v", "s", "20260101", opts); err != nil {
							b.Fatal(err)
						}
					})
					b.ReportMetric(float64(peak), "peak-heap-B")
				}
			})
		}
	}
}
