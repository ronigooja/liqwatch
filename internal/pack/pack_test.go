package pack

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestPack(t *testing.T) {
	for _, unit := range []string{TarThenZstd, ZstdThenTar} {
		t.Run(unit, func(t *testing.T) {
			root := t.TempDir()
			raw := filepath.Join(root, "raw")
			out := filepath.Join(root, "packs")
			now := time.Now().UTC()
			day := now.AddDate(0, 0, -1).Format("20060102")
			dir := filepath.Join(raw, "v", "s", day)
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
			want := map[string]string{"trade.a.jsonl": "first\n", "mark.b.jsonl": "second\n"}
			for n, v := range want {
				p := filepath.Join(dir, n)
				if err := os.WriteFile(p, []byte(v), 0644); err != nil {
					t.Fatal(err)
				}
				old := now.Add(-time.Hour)
				if err := os.Chtimes(p, old, old); err != nil {
					t.Fatal(err)
				}
			}
			opts := Options{PackRoot: out, Unit: unit, Now: now}
			result, err := Pack(raw, "v", "s", day, opts)
			if err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(filepath.Join(out, result.Path))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			var r io.Reader = f
			if unit == TarThenZstd {
				d, e := zstd.NewReader(f)
				if e != nil {
					t.Fatal(e)
				}
				defer d.Close()
				r = d
			}
			tr := tar.NewReader(r)
			found := map[string]string{}
			for {
				h, e := tr.Next()
				if errors.Is(e, io.EOF) {
					break
				}
				if e != nil {
					t.Fatal(e)
				}
				name := h.Name
				if strings.Contains(name, "/") {
					t.Fatal(name)
				}
				var contents io.Reader = tr
				if unit == ZstdThenTar {
					name = strings.TrimSuffix(name, ".zst")
					d, e := zstd.NewReader(tr)
					if e != nil {
						t.Fatal(e)
					}
					defer d.Close()
					contents = d
				}
				b, e := io.ReadAll(contents)
				if e != nil {
					t.Fatal(e)
				}
				found[name] = string(b)
			}
			if len(found) != len(want) {
				t.Fatal(found)
			}
			for _, m := range result.Members {
				sum := sha256.Sum256([]byte(want[m.Name]))
				if found[m.Name] != want[m.Name] || m.Bytes != int64(len(want[m.Name])) || m.SHA256 != hex.EncodeToString(sum[:]) {
					t.Fatal(m)
				}
			}
			opts.Level = 9
			again, err := Pack(raw, "v", "s", day, opts)
			if err != nil || !again.Skipped || again.SHA256 != result.SHA256 {
				t.Fatalf("idempotency: %+v %v", again, err)
			}
			if _, err = Pack(raw, "v", "s", now.Format("20060102"), opts); !errors.Is(err, ErrIncomplete) {
				t.Fatal(err)
			}
			p := filepath.Join(dir, "trade.a.jsonl")
			if err = os.Chtimes(p, now, now); err != nil {
				t.Fatal(err)
			}
			if _, err = Pack(raw, "v", "s", day, opts); !errors.Is(err, ErrIncomplete) {
				t.Fatal(err)
			}
			if err = RemoveRaw(raw, "v", "s", day); err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		})
	}
}

func TestUnsafe(t *testing.T) {
	for _, s := range []string{"..", "x/y", "x\\y", ""} {
		if err := RemoveRaw(t.TempDir(), s, "s", "20260101"); err == nil {
			t.Fatal(s)
		}
		if _, err := Pack("/tmp", "v", s, "20260101", Options{}); err == nil {
			t.Fatal(s)
		}
	}
}
