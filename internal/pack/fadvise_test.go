package pack

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func cacheFixture(t *testing.T) (string, string, Options) {
	t.Helper()
	root := t.TempDir()
	raw := filepath.Join(root, "raw")
	day := "20260101"
	if err := os.MkdirAll(filepath.Join(raw, "v", "s", day), 0750); err != nil {
		t.Fatal(err)
	}
	return raw, day, Options{PackRoot: filepath.Join(root, "packs"), Now: time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)}
}

func cacheMember(t *testing.T, raw, day, name string, data []byte, repeats int) string {
	t.Helper()
	path := filepath.Join(raw, "v", "s", day, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for range repeats {
		if _, err := f.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	old := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	return path
}

func replaceCacheHook(t *testing.T, hook func(*os.File)) {
	t.Helper()
	old := dropSourceCache
	dropSourceCache = hook
	t.Cleanup(func() { dropSourceCache = old })
}

func TestSourceCacheHookExactFilesAfterRead(t *testing.T) {
	raw, day, opts := cacheFixture(t)
	b := cacheMember(t, raw, day, "b.jsonl", []byte("second\n"), 1)
	a := cacheMember(t, raw, day, "a.jsonl", []byte("first\n"), 1)
	var got []string
	replaceCacheHook(t, func(f *os.File) {
		pos, err := f.Seek(0, io.SeekCurrent)
		info, statErr := f.Stat()
		if err != nil || statErr != nil {
			t.Fatalf("hook needs open source: %v %v", err, statErr)
		}
		if pos != info.Size() {
			t.Errorf("hook before full read: %s offset=%d size=%d", f.Name(), pos, info.Size())
		}
		got = append(got, f.Name())
	})
	if _, err := Pack(raw, "v", "s", day, opts); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{a, b}) {
		t.Fatalf("hook files: got %q want %q", got, []string{a, b})
	}
}

func TestCacheHookPreservesWholePack(t *testing.T) {
	for _, unit := range []string{TarThenZstd, ZstdThenTar} {
		t.Run(unit, func(t *testing.T) {
			raw, day, opts := cacheFixture(t)
			opts.Unit = unit
			cacheMember(t, raw, day, "a.jsonl", []byte("{\"trade\":123}\n"), 3000)
			cacheMember(t, raw, day, "b.jsonl", []byte("{\"mark\":456}\n"), 2000)
			replaceCacheHook(t, func(*os.File) {})
			without, err := Pack(raw, "v", "s", day, opts)
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(opts.PackRoot, without.Path))
			if err != nil {
				t.Fatal(err)
			}
			opts.PackRoot = t.TempDir()
			dropSourceCache = dropFileCache
			with, err := Pack(raw, "v", "s", day, opts)
			if err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(filepath.Join(opts.PackRoot, with.Path))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) || !reflect.DeepEqual(without, with) {
				t.Fatalf("hook changed whole pack or manifest: %+v %+v", without, with)
			}
		})
	}
}
