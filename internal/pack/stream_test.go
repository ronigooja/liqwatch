package pack

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// legacyPack reproduces the old whole-file compression and tar headers, not the
// new encoder factory. This exercises both directions of reader compatibility.
func legacyPack(t *testing.T, files map[string][]byte, unit string) []byte {
	t.Helper()
	var out bytes.Buffer
	var dst io.Writer = &out
	var outer *zstd.Encoder
	var err error
	if unit == TarThenZstd {
		outer, err = zstd.NewWriter(dst, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)))
		if err != nil {
			t.Fatal(err)
		}
		dst = outer
	}
	tw := tar.NewWriter(dst)
	for name, data := range files {
		if unit == ZstdThenTar {
			e, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)))
			if err != nil {
				t.Fatal(err)
			}
			data = e.EncodeAll(data, nil)
			e.Close()
			name += ".zst"
		}
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0640, Size: int64(len(data))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if outer != nil {
		if err := outer.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return out.Bytes()
}

func TestLegacyCompatibility(t *testing.T) {
	for _, unit := range []string{TarThenZstd, ZstdThenTar} {
		t.Run(unit, func(t *testing.T) {
			files := map[string][]byte{"trade.0000.jsonl": bytes.Repeat([]byte("old raw\n"), 700)}
			old := legacyPack(t, files, unit)
			members, err := readMembers(bytes.NewReader(old), unit)
			if err != nil {
				t.Fatal(err)
			}
			raw, opts := memoryFixture(t, 1, len(files["trade.0000.jsonl"]))
			opts.Unit = unit
			if err := os.WriteFile(filepath.Join(raw, "v", "s", "20260101", "trade.0000.jsonl"), files["trade.0000.jsonl"], 0640); err != nil {
				t.Fatal(err)
			}
			result, err := Pack(raw, "v", "s", "20260101", opts)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(members, result.Members) {
				t.Fatalf("legacy members %v, new %v", members, result.Members)
			}
			// Existing reader accepts new bytes; old archive is preserved on retry.
			f, err := os.Open(filepath.Join(opts.PackRoot, result.Path))
			if err != nil {
				t.Fatal(err)
			}
			got, err := readMembers(f, unit)
			f.Close()
			if err != nil || !reflect.DeepEqual(got, members) {
				t.Fatalf("new reader: %v %v", got, err)
			}
			if err := os.WriteFile(filepath.Join(opts.PackRoot, result.Path), old, 0640); err != nil {
				t.Fatal(err)
			}
			again, err := Pack(raw, "v", "s", "20260101", opts)
			sum := sha256.Sum256(old)
			if err != nil || !again.Skipped || again.SHA256 != hex.EncodeToString(sum[:]) {
				t.Fatalf("legacy retry: %+v %v", again, err)
			}
		})
	}
}

func TestChangedWhileReading(t *testing.T) {
	for _, unit := range []string{TarThenZstd, ZstdThenTar} {
		for _, grow := range []bool{false, true} {
			t.Run(unit+map[bool]string{true: "/grow", false: "/truncate"}[grow], func(t *testing.T) {
				raw, opts := memoryFixture(t, 1, 128<<10)
				opts.Unit = unit
				result, err := pack(raw, "v", "s", "20260101", opts, func(dst io.Writer, src *os.File, size int64) (int64, error) {
					info, err := src.Stat()
					if err != nil {
						t.Fatal(err)
					}
					first, err := io.CopyN(dst, src, 4096)
					if err != nil {
						t.Fatal(err)
					}
					next := size / 2
					if grow {
						next = size + 4096
					}
					if err := os.Truncate(src.Name(), next); err != nil {
						t.Fatal(err)
					}
					// Restore timestamps so only the byte-count guard detects change.
					if err := os.Chtimes(src.Name(), info.ModTime(), info.ModTime()); err != nil {
						t.Fatal(err)
					}
					n, err := copyMember(dst, src, size-first)
					if !errors.Is(err, ErrIncomplete) {
						t.Errorf("copy size check: n=%d size=%d err=%v", n, size, err)
					}
					return first + n, err
				})
				if err == nil {
					t.Fatalf("changed member accepted: %+v", result)
				}
				// Pack cannot publish or return success to the manifest caller.
				if err := filepath.WalkDir(opts.PackRoot, func(p string, d os.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if !d.IsDir() {
						t.Errorf("left output %s", p)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestLargeFileBoundedHeap(t *testing.T) {
	for _, unit := range []string{TarThenZstd, ZstdThenTar} {
		t.Run(unit, func(t *testing.T) {
			raw, opts := memoryFixture(t, 2, 256<<20)
			opts.Unit = unit
			peak := measureHeap(func() {
				if _, err := Pack(raw, "v", "s", "20260101", opts); err != nil {
					t.Fatal(err)
				}
			})
			// 96 MiB tolerates GC/sampling/race overhead, but excludes one 256 MiB read.
			if peak > 96<<20 {
				t.Fatalf("peak heap growth %d exceeds 96 MiB", peak)
			}
			t.Logf("peak heap growth: %d bytes", peak)
		})
	}
}

func TestEncoderLevelAndReuse(t *testing.T) {
	e, err := acquireEncoder(3)
	if err != nil {
		t.Fatal(err)
	}
	// Compare compressed bytes against independently constructed level-3 encoder
	// with the documented memory options (not against a compression-size guess).
	want, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)), zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20), zstd.WithLowerEncoderMem(true))
	if err != nil {
		t.Fatal(err)
	}
	defer want.Close()
	data := bytes.Repeat([]byte("{\"trade\":12345,\"price\":9876.5,\"symbol\":\"BTCUSDT\"}\n"), 10000)
	// Vary records and their repeated substrings: a single repeated line can
	// produce identical frames at different compression levels.
	var x uint32 = 7
	for i := range data {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		if i%101 < 40 {
			data[i] = byte(x % 24)
		}
	}
	if !bytes.Equal(e.EncodeAll(data, nil), want.EncodeAll(data, nil)) {
		t.Fatal("encoder is not configured at level 3")
	}
	encoders.Put(e)
	again, err := acquireEncoder(3)
	if err != nil {
		t.Fatal(err)
	}
	// sync.Pool may discard entries at GC, so identity is deliberately not asserted.
	if !bytes.Equal(again.EncodeAll(data, nil), want.EncodeAll(data, nil)) {
		t.Fatal("reused encoder configuration changed")
	}
	again.Close()
}
