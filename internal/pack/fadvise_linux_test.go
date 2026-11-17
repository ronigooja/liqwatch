//go:build linux

package pack

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func residentBytes(t *testing.T, path string) int64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	m, err := unix.Mmap(int(f.Fd()), 0, int(info.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Munmap(m)
	vec := make([]byte, (len(m)+os.Getpagesize()-1)/os.Getpagesize())
	// x/sys/unix v0.49.0 exposes SYS_MINCORE but no Mincore wrapper.
	_, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&m[0])), uintptr(len(m)), uintptr(unsafe.Pointer(&vec[0])))
	runtime.KeepAlive(m)
	runtime.KeepAlive(vec)
	if errno != 0 {
		t.Fatal(errno)
	}
	var pages int64
	for _, v := range vec {
		if v&1 != 0 {
			pages++
		}
	}
	return pages * int64(os.Getpagesize())
}

func evictPath(t *testing.T, path string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	dropFileCache(f)
	f.Close()
}

func TestPackSourceCacheResidency(t *testing.T) {
	const size = 256 << 20
	raw, day, opts := cacheFixture(t)
	chunk := make([]byte, 1<<20)
	for i := range chunk {
		chunk[i] = byte(i*17 + i/1024)
	}
	on := cacheMember(t, raw, day, "source.jsonl", chunk, size/len(chunk))
	// Preflight distinguishes unsupported filesystems (e.g. tmpfs) from a
	// regression in Pack. Sources are synced by fixture setup, never by Pack.
	evictPath(t, on)
	if n := residentBytes(t, on); n > size/10 {
		t.Skipf("filesystem/kernel does not honor DONTNEED: %d/%d resident", n, size)
	}
	controlRaw, controlDay, controlOpts := cacheFixture(t)
	control := cacheMember(t, controlRaw, controlDay, "source.jsonl", chunk, size/len(chunk))
	replaceCacheHook(t, func(*os.File) {})
	if _, err := Pack(controlRaw, "v", "s", controlDay, controlOpts); err != nil {
		t.Fatal(err)
	}
	controlN := residentBytes(t, control)
	// Wide thresholds tolerate readahead and incidental reclaim; a cold control
	// is inconclusive, not a pass. Never skip a failed hooked Pack after preflight.
	if controlN < size*8/10 {
		t.Skipf("control not resident enough: %d/%d (<80%%); memory pressure makes comparison inconclusive", controlN, size)
	}
	dropSourceCache = dropFileCache
	if _, err := Pack(raw, "v", "s", day, opts); err != nil {
		t.Fatal(err)
	}
	onN := residentBytes(t, on)
	t.Logf("source bytes=%d control_resident=%d hook_resident=%d thresholds control>=80%% hook<10%%", size, controlN, onN)
	if onN >= size/10 {
		t.Fatalf("hook left %d/%d bytes resident (require <10%%)", onN, size)
	}
}

func cacheMemoryStat() (int64, int64) {
	// systemd's unified cgroup path is visible inside the transient service.
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return -1, -1
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, "0::") {
			continue
		}
		s, err := os.ReadFile(filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(line, "0::"), "memory.stat"))
		if err != nil {
			return -1, -1
		}
		var file, slab int64
		for _, row := range strings.Split(string(s), "\n") {
			fields := strings.Fields(row)
			if len(fields) != 2 {
				continue
			}
			n, _ := strconv.ParseInt(fields[1], 10, 64)
			if fields[0] == "file" {
				file = n
			}
			if fields[0] == "slab" {
				slab = n
			}
		}
		return file, slab
	}
	return -1, -1
}

// Opt in on a Linux build machine, not the recorder:
// LIQWATCH_CACHE_MEASURE=1 go test -run '^TestPackCacheMeasurement$' -v ./internal/pack
func TestPackCacheMeasurement(t *testing.T) {
	if os.Getenv("LIQWATCH_CACHE_MEASURE") != "1" {
		t.Skip("set LIQWATCH_CACHE_MEASURE=1 for the 600 MiB build-machine measurement")
	}
	raw, day, opts := cacheFixture(t)
	chunk := make([]byte, 64<<10)
	for i := range chunk {
		chunk[i] = byte(i*17 + i/1024)
	}
	var paths []string
	for i := range 1024 {
		paths = append(paths, cacheMember(t, raw, day, fmt.Sprintf("small-%04d.jsonl", i), chunk, 1))
	}
	for i := range 4 {
		paths = append(paths, cacheMember(t, raw, day, fmt.Sprintf("large-%d.jsonl", i), chunk, 134*16))
	}
	replaceCacheHook(t, dropFileCache)
	for _, enabled := range []bool{false, true} {
		for _, p := range paths {
			evictPath(t, p)
		}
		opts.PackRoot = t.TempDir()
		if enabled {
			dropSourceCache = dropFileCache
		} else {
			dropSourceCache = func(*os.File) {}
		}
		baseline, _ := cacheMemoryStat()
		stop := make(chan struct{})
		peakCh := make(chan int64)
		go func() {
			peak := baseline
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				n, _ := cacheMemoryStat()
				if n > peak {
					peak = n
				}
				select {
				case <-stop:
					peakCh <- peak
					return
				case <-ticker.C:
				}
			}
		}()
		start := time.Now()
		result, err := Pack(raw, "v", "s", day, opts)
		elapsed := time.Since(start)
		close(stop)
		peak := <-peakCh
		if err != nil {
			t.Fatal(err)
		}
		after, slab := cacheMemoryStat()
		var resident int64
		for _, p := range paths {
			resident += residentBytes(t, p)
		}
		t.Logf("hook=%t input_bytes=%d members=%d output_bytes=%d seconds=%.3f source_resident=%d cgroup_file_baseline=%d peak=%d after=%d peak_growth=%d slab_after=%d", enabled, 600<<20, len(paths), result.Bytes, elapsed.Seconds(), resident, baseline, peak, after, peak-baseline, slab)
		if enabled && resident >= (600<<20)/10 {
			t.Fatal("measurement hook left >=10% source pages resident")
		}
	}
}
