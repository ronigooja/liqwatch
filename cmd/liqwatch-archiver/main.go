package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
	"github.com/ronigooja/liqwatch/internal/webdav"
)

type meta struct {
	Size   int64    `json:"size"`
	SHA256 string   `json:"sha256"`
	Chunks []string `json:"chunks"`
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "/etc/liqwatch/config.toml", "configuration file")
	flag.Parse()
	var cfg config.Config
	b, err := os.ReadFile(configPath)
	if err != nil {
		fatal(err)
	}
	if err = toml.Unmarshal(b, &cfg); err != nil {
		fatal(err)
	}
	if err = run(context.Background(), cfg); err != nil {
		fatal(err)
	}
}

func run(ctx context.Context, cfg config.Config) error {
	if cfg.Archive.ChunkMB <= 0 {
		return errors.New("archive chunk_mb must be positive")
	}
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, "manifest"), 0o750); err != nil {
		return err
	}
	store, err := manifest.Open(filepath.Join(cfg.DataDir, "manifest", "manifest.db"))
	if err != nil {
		return err
	}
	defer store.Close()
	user, pass := os.Getenv(config.EnvWebDAVUser), os.Getenv(config.EnvWebDAVPass)
	dav := webdav.New(cfg.Archive.RemoteRoot, user, pass)
	dav.MaxKBps = cfg.Archive.MaxKBps
	for _, state := range []manifest.State{manifest.StateClosed, manifest.StateUploading} {
		files, err := store.ListByState(state)
		if err != nil {
			return err
		}
		for _, f := range files {
			if state == manifest.StateClosed {
				if err = store.SetState(f.ID, manifest.StateUploading); err != nil {
					return err
				}
			}
			if err = archiveOne(ctx, cfg, store, dav, f); err != nil {
				return err
			}
		}
	}
	if err = verifyFiles(ctx, cfg, store, dav); err != nil {
		return err
	}
	return deleteFiles(cfg, store)
}

func archiveOne(ctx context.Context, cfg config.Config, store manifest.Store, dav *webdav.Client, f manifest.File) error {
	local := filepath.Join(cfg.DataDir, "raw", filepath.FromSlash(f.Path))
	in, err := os.Open(local)
	if err != nil {
		return err
	}
	defer in.Close()
	h := sha256.New()
	n, err := io.Copy(h, in)
	if err != nil {
		return err
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if f.SHA256 != "" && f.SHA256 != sum {
		return errors.New("local file checksum mismatch")
	}
	if f.SHA256 == "" {
		if s, ok := store.(*manifest.SQLiteStore); ok {
			if err = s.SetSHA256(f.ID, sum); err != nil {
				return err
			}
		}
	}
	if _, err = in.Seek(0, io.SeekStart); err != nil {
		return err
	}
	chunkSize := int64(cfg.Archive.ChunkMB) * 1024 * 1024
	count := int((n + chunkSize - 1) / chunkSize)
	dir := pathDate(f.Path)
	base := filepath.Base(f.Path)
	remoteDir := filepath.ToSlash(filepath.Join("raw", dir))
	if err = dav.MkdirAll(ctx, remoteDir); err != nil {
		return err
	}
	chunks := make([]string, 0, count)
	buf := make([]byte, chunkSize)
	for i := 0; i < count; i++ {
		want := chunkSize
		if rem := n - int64(i)*chunkSize; rem < want {
			want = rem
		}
		if _, err = io.ReadFull(in, buf[:want]); err != nil {
			return err
		}
		name := fmt.Sprintf("%s.part%04d", base, i)
		remote := filepath.ToSlash(filepath.Join(remoteDir, name))
		chunks = append(chunks, name)
		size, e := dav.Stat(ctx, remote)
		if e == nil && size == want {
			continue
		}
		if e == nil && size != want { /* overwrite below */
		}
		if err = putBytes(ctx, dav, remote, buf[:want]); err != nil {
			return err
		}
	}
	m := meta{Size: n, SHA256: sum, Chunks: chunks}
	body, _ := json.Marshal(m)
	metaName := filepath.ToSlash(filepath.Join(remoteDir, base+".meta.json"))
	if err = putVerified(ctx, dav, metaName, body); err != nil {
		return err
	}
	if s, ok := store.(*manifest.SQLiteStore); ok {
		if err = s.SetUploadMetadata(f.ID, filepath.ToSlash(filepath.Join(remoteDir, base)), count); err != nil {
			return err
		}
	}
	if err = store.SetState(f.ID, manifest.StateUploaded); err != nil {
		return err
	}
	return nil
}

func putBytes(ctx context.Context, dav *webdav.Client, name string, b []byte) error {
	return dav.Put(ctx, name, bytes.NewReader(b), int64(len(b)))
}

// putVerified uploads small metadata and reads it back; the storage once accepted a PUT and kept
// a zero-byte file, which the size check on data chunks does not cover.
func putVerified(ctx context.Context, dav *webdav.Client, name string, b []byte) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if err = putBytes(ctx, dav, name, b); err != nil {
			continue
		}
		var got []byte
		var r io.ReadCloser
		if r, _, err = dav.Get(ctx, name); err != nil {
			continue
		}
		got, err = io.ReadAll(r)
		r.Close()
		if err == nil && bytes.Equal(got, b) {
			return nil
		}
		err = fmt.Errorf("metadata read-back mismatch for %s (%d of %d bytes)", name, len(got), len(b))
	}
	return err
}

func pathDate(p string) string {
	parts := strings.Split(filepath.ToSlash(p), "/")
	for _, x := range parts {
		if len(x) == 8 {
			return x
		}
	}
	return time.Now().UTC().Format("20060102")
}

func verifyFiles(ctx context.Context, cfg config.Config, store manifest.Store, dav *webdav.Client) error {
	files, err := store.ListByState(manifest.StateUploaded)
	if err != nil {
		return err
	}
	first := time.Time{}
	if s, ok := store.(interface{ FirstVerifiedDate() (time.Time, error) }); ok {
		first, err = s.FirstVerifiedDate()
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	full := first.IsZero() || time.Since(first.AddDate(0, 0, cfg.Archive.FullVerifyDays)) < 0
	for _, f := range files {
		if err = verifyOne(ctx, cfg, dav, f, full); err != nil {
			return err
		}
		if err = store.MarkVerified(f.ID, filepath.ToSlash(filepath.Join("raw", pathDate(f.Path), filepath.Base(f.Path))), f.ChunkCount, time.Now().UTC()); err != nil {
			return err
		}
	}
	return nil
}

func verifyOne(ctx context.Context, cfg config.Config, dav *webdav.Client, f manifest.File, full bool) error {
	if f.RemotePath == "" {
		f.RemotePath = filepath.ToSlash(filepath.Join("raw", pathDate(f.Path), filepath.Base(f.Path)))
	}
	if f.ChunkCount <= 0 {
		return errors.New("missing chunk count")
	}
	if err := ensureMeta(ctx, dav, f); err != nil {
		return err
	}
	chunkSize := int64(cfg.Archive.ChunkMB) * 1024 * 1024
	h := sha256.New()
	var total int64
	for i := 0; i < f.ChunkCount; i++ {
		name := fmt.Sprintf("%s.part%04d", f.RemotePath, i)
		size, err := dav.Stat(ctx, name)
		if err != nil {
			return err
		}
		want := chunkSize
		if rem := f.Bytes - int64(i)*chunkSize; rem < want {
			want = rem
		}
		if size != want {
			return fmt.Errorf("remote chunk size mismatch")
		}
		if full || i == 0 {
			r, _, err := dav.Get(ctx, name)
			if err != nil {
				return err
			}
			_, err = io.Copy(h, r)
			r.Close()
			if err != nil {
				return err
			}
		}
		total += size
	}
	if full && (total != f.Bytes || hex.EncodeToString(h.Sum(nil)) != f.SHA256) {
		return errors.New("remote verification checksum mismatch")
	}
	return nil
}

// ensureMeta makes sure the .meta.json next to the chunks is readable and matches the manifest. The
// data chunks are the truth, so a bad metadata object is rewritten from the manifest.
func ensureMeta(ctx context.Context, dav *webdav.Client, f manifest.File) error {
	if f.SHA256 == "" {
		return nil
	}
	name := f.RemotePath + ".meta.json"
	chunks := make([]string, f.ChunkCount)
	for i := range chunks {
		chunks[i] = fmt.Sprintf("%s.part%04d", filepath.Base(f.RemotePath), i)
	}
	want, _ := json.Marshal(meta{Size: f.Bytes, SHA256: f.SHA256, Chunks: chunks})
	if r, _, err := dav.Get(ctx, name); err == nil {
		got, _ := io.ReadAll(r)
		r.Close()
		if bytes.Equal(got, want) {
			return nil
		}
	}
	return putVerified(ctx, dav, name, want)
}

func deleteFiles(cfg config.Config, store manifest.Store) error {
	files, err := store.DeletableFiles(time.Duration(cfg.Archive.DeleteAfterH)*time.Hour, time.Now())
	if err != nil {
		return err
	}
	for _, f := range files {
		if err = store.MarkDeleted(f.ID, time.Duration(cfg.Archive.DeleteAfterH)*time.Hour, time.Now()); err != nil {
			return err
		}
		if err = os.Remove(filepath.Join(cfg.DataDir, "raw", filepath.FromSlash(f.Path))); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
