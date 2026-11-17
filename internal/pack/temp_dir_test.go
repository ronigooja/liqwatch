package pack

import (
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestTempFilesBesideFinalPack(t *testing.T) {
	for _, unit := range []string{TarThenZstd, ZstdThenTar} {
		t.Run(unit, func(t *testing.T) {
			raw, output := t.TempDir(), t.TempDir()
			dir := filepath.Join(raw, "v/s/20260101")
			if err := os.MkdirAll(dir, 0750); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "trade.a.jsonl"), []byte("test"), 0640); err != nil {
				t.Fatal(err)
			}
			opts := Options{PackRoot: output, Unit: unit, Now: time.Now().Add(24 * time.Hour)}
			calls := 0
			_, err := pack(raw, "v", "s", "20260101", opts, func(dst io.Writer, src *os.File, size int64) (int64, error) {
				calls++
				finalDir := filepath.Join(output, "v/s")
				var patterns []string
				for _, pattern := range []string{".pack-*", ".member-*"} {
					matches, err := filepath.Glob(filepath.Join(finalDir, pattern))
					if err != nil {
						t.Fatal(err)
					}
					for _, path := range matches {
						if filepath.Dir(path) != finalDir {
							t.Fatal(path)
						}
						patterns = append(patterns, pattern)
					}
				}
				sort.Strings(patterns)
				want := []string{".pack-*"}
				if unit == ZstdThenTar {
					want = []string{".member-*", ".pack-*"}
				}
				if !reflect.DeepEqual(patterns, want) {
					t.Fatalf("temporary files=%v want=%v", patterns, want)
				}
				return copyMember(dst, src, size)
			})
			if err != nil || calls != 1 {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
}
