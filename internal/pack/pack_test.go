package pack

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
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
			want := map[string]string{
				"trade.a.jsonl":   "{\"v\":2,\"wseq\":1,\"cseq\":7,\"rw\":42,\"rm\":3,\"p\":{\"t\":40,\"cts\":41,\"sample\":{\"channel\":\"trade\",\"symbol\":\"s\"}}}\n",
				"funding.p.jsonl": "{\"v\":2,\"rw\":42,\"rm\":3,\"meta\":{\"last_row_timestamp\":40,\"snapshot\":false},\"p\":{\"timepoint\":40,\"fundingRate\":\"0.0001\"}}\n",
			}
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
			ext := ".tar.zst"
			if unit == ZstdThenTar {
				ext = ".tar"
			}
			if result.Path != filepath.Join("v", "s", day+ext) {
				t.Fatalf("identity lost: %s", result.Path)
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
				if _, ok := want[name]; !ok {
					t.Fatalf("unexpected member %s", name)
				}
				if _, duplicate := found[name]; duplicate {
					t.Fatalf("duplicate member %s", name)
				}
				if unit == TarThenZstd && h.Size != int64(len(b)) {
					t.Fatalf("header size %d, bytes %d", h.Size, len(b))
				}
				if string(b) != want[name] {
					t.Fatalf("member bytes differ: %s", name)
				}
				found[name] = string(b)
				// Restore the archive's venue/symbol/date plus the member's channel/slot.
				extracted := filepath.Join(root, "extracted", "v", "s", day, name)
				if err := os.MkdirAll(filepath.Dir(extracted), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(extracted, b, 0644); err != nil {
					t.Fatal(err)
				}
				rel, err := filepath.Rel(filepath.Join(root, "extracted"), extracted)
				if err != nil {
					t.Fatal(err)
				}
				parts := strings.Split(rel, string(filepath.Separator))
				if !reflect.DeepEqual(parts, []string{"v", "s", day, name}) {
					t.Fatal(parts)
				}
				channel := strings.Split(strings.TrimSuffix(parts[3], ".jsonl"), ".")[0]
				if channel != "trade" && channel != "funding" {
					t.Fatal(channel)
				}
			}
			if len(found) != len(want) {
				t.Fatal(found)
			}
			if len(result.Members) != len(want) {
				t.Fatalf("manifest member count: %d", len(result.Members))
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
