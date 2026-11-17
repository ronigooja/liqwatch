package pack

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Compressed-only and transition days store exactly the source bytes.
func TestCompressedMembersStoredHashes(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		root := t.TempDir()
		dir := filepath.Join(root, "raw", "v", "s", "20260101")
		if err := os.MkdirAll(dir, 0750); err != nil {
			t.Fatal(err)
		}
		e, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		if err != nil {
			t.Fatal(err)
		}
		plain := []byte("{\"v\":2,\"p\":{\"price\":\"123.4500\"}}\n")
		stored := e.EncodeAll(plain, nil)
		e.Close()
		want := map[string][]byte{"trade.a.jsonl.zst": stored}
		if mixed {
			want["funding.p.jsonl"] = plain
		}
		now := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
		for name, b := range want {
			p := filepath.Join(dir, name)
			if err := os.WriteFile(p, b, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(p, now.Add(-time.Hour), now.Add(-time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		opts := Options{PackRoot: filepath.Join(root, "packs"), Now: now, Unit: TarThenZstd}
		r, err := Pack(filepath.Join(root, "raw"), "v", "s", "20260101", opts)
		if err != nil {
			t.Fatal(err)
		}
		if r.Unit != StoredTar || r.Path != filepath.Join("v", "s", "20260101.tar") {
			t.Fatalf("compressed input recompressed: %+v", r)
		}
		f, err := os.Open(filepath.Join(opts.PackRoot, r.Path))
		if err != nil {
			t.Fatal(err)
		}
		tr := tar.NewReader(f)
		for range want {
			h, err := tr.Next()
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(b, want[h.Name]) {
				t.Fatalf("stored member differs: %s", h.Name)
			}
		}
		if _, err := tr.Next(); err != io.EOF {
			t.Fatalf("extra member: %v", err)
		}
		f.Close()
		if len(r.Members) != len(want) {
			t.Fatal(r.Members)
		}
		for _, m := range r.Members {
			sum := sha256.Sum256(want[m.Name])
			if m.Bytes != int64(len(want[m.Name])) || m.SHA256 != hex.EncodeToString(sum[:]) {
				t.Fatal(m)
			}
		}
		r, err = Pack(filepath.Join(root, "raw"), "v", "s", "20260101", opts)
		if err != nil || !r.Skipped {
			t.Fatalf("idempotence: %+v %v", r, err)
		}
	}
}
