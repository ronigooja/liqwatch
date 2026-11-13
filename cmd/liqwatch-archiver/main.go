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
	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
	"github.com/ronigooja/liqwatch/internal/pack"
	"github.com/ronigooja/liqwatch/internal/webdav"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type meta struct {
	Size    int64             `json:"size"`
	SHA256  string            `json:"sha256"`
	Members []manifest.Member `json:"members"`
	Unit    string            `json:"unit"`
	Level   int               `json:"level"`
}

func main() {
	p := flag.String("config", "/etc/liqwatch/config.toml", "configuration file")
	flag.Parse()
	cfg, err := config.Load(*p)
	if err == nil {
		err = run(context.Background(), *cfg)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(ctx context.Context, cfg config.Config) error {
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, "manifest"), 0750); err != nil {
		return err
	}
	s, err := manifest.Open(filepath.Join(cfg.DataDir, "manifest", "manifest.db"))
	if err != nil {
		return err
	}
	defer s.Close()
	dav := webdav.New(cfg.Archive.RemoteRoot, os.Getenv(config.EnvWebDAVUser), os.Getenv(config.EnvWebDAVPass))
	dav.MaxKBps = cfg.Archive.MaxKBps
	for _, st := range []manifest.State{manifest.StateClosed, manifest.StateUploading} {
		ps, err := s.ListByState(st)
		if err != nil {
			return err
		}
		for _, p := range ps {
			if st == manifest.StateClosed {
				if err = s.SetState(p.ID, manifest.StateUploading); err != nil {
					return err
				}
			}
			if err = archiveOne(ctx, cfg, s, dav, p); err != nil {
				return err
			}
		}
	}
	if err = verifyFiles(ctx, cfg, s, dav); err != nil {
		return err
	}
	return deleteFiles(cfg, s)
}
func remotePath(p manifest.Pack) string {
	return path.Join("raw", p.Venue, p.Symbol, path.Base(filepath.ToSlash(p.Path)))
}
func metaPath(p manifest.Pack) string {
	return path.Join("raw", p.Venue, p.Symbol, fmt.Sprintf("%08d.meta.json", p.Day))
}
func metadata(cfg config.Config, s manifest.Store, p manifest.Pack) ([]byte, error) {
	members, err := s.Members(p.ID)
	if err != nil {
		return nil, err
	}
	if members == nil {
		members = []manifest.Member{}
	}
	unit := cfg.Pack.Unit
	if strings.HasSuffix(p.Path, ".tar.zst") {
		unit = pack.TarThenZstd
	} else {
		unit = pack.ZstdThenTar
	}
	level := cfg.Pack.Level
	return json.Marshal(meta{Size: p.Bytes, SHA256: p.SHA256, Members: members, Unit: unit, Level: level})
}
func archiveOne(ctx context.Context, cfg config.Config, s manifest.Store, dav *webdav.Client, p manifest.Pack) error {
	local := filepath.Join(cfg.DataDir, "packs", filepath.FromSlash(p.Path))
	f, err := os.Open(local)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	f.Close()
	if err != nil {
		return err
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if n != p.Bytes || p.SHA256 != "" && p.SHA256 != sum {
		return errors.New("local pack checksum or size mismatch")
	}
	if p.SHA256 == "" {
		if err = s.SetSHA256(p.ID, sum); err != nil {
			return err
		}
		p.SHA256 = sum
	}
	if n > int64(cfg.Archive.MaxPackMB)*1024*1024 {
		log.Printf("warning: oversized pack %s (%d bytes)", p.Path, n)
		if err = s.IncrSetting("pack_oversize_count"); err != nil {
			return err
		}
	}
	remote := remotePath(p)
	if err = dav.MkdirAll(ctx, path.Dir(remote)); err != nil {
		return err
	}
	if err = dav.PutFile(ctx, remote, local, n); err != nil {
		return err
	}
	size, err := dav.Stat(ctx, remote)
	if err != nil {
		return err
	}
	if size != n {
		return fmt.Errorf("remote pack size mismatch: got %d want %d", size, n)
	}
	body, err := metadata(cfg, s, p)
	if err != nil {
		return err
	}
	if err = putVerified(ctx, dav, metaPath(p), body); err != nil {
		return err
	}
	if err = s.SetUploadMetadata(p.ID, remote); err != nil {
		return err
	}
	return s.SetState(p.ID, manifest.StateUploaded)
}
func putVerified(ctx context.Context, dav *webdav.Client, name string, b []byte) error {
	var err error
	for i := 0; i < 3; i++ {
		if err = dav.Put(ctx, name, bytes.NewReader(b), int64(len(b))); err != nil {
			continue
		}
		var size int64
		size, err = dav.Stat(ctx, name)
		if err != nil {
			continue
		}
		if size != int64(len(b)) {
			err = errors.New("remote metadata size mismatch")
			continue
		}
		var r io.ReadCloser
		r, _, err = dav.Get(ctx, name)
		if err != nil {
			continue
		}
		var got []byte
		got, err = io.ReadAll(r)
		r.Close()
		if err == nil && bytes.Equal(got, b) {
			return nil
		}
		err = errors.New("metadata read-back mismatch")
	}
	return err
}
func ensureMeta(ctx context.Context, cfg config.Config, s manifest.Store, dav *webdav.Client, p manifest.Pack) error {
	want, err := metadata(cfg, s, p)
	if err != nil {
		return err
	}
	if r, _, e := dav.Get(ctx, metaPath(p)); e == nil {
		got, e := io.ReadAll(r)
		r.Close()
		if e == nil && bytes.Equal(got, want) {
			return nil
		}
	}
	return putVerified(ctx, dav, metaPath(p), want)
}
func verifyFiles(ctx context.Context, cfg config.Config, s *manifest.SQLiteStore, dav *webdav.Client) error {
	ps, err := s.ListByState(manifest.StateUploaded)
	if err != nil {
		return err
	}
	first, err := s.FirstVerifiedDate()
	if err != nil {
		return err
	}
	full := first.IsZero() || time.Now().Before(first.AddDate(0, 0, cfg.Archive.FullVerifyDays))
	for _, p := range ps {
		if err = verifyOne(ctx, cfg, s, dav, p, full); err != nil {
			return err
		}
		if err = s.MarkVerified(p.ID, time.Now().UTC()); err != nil {
			return err
		}
	}
	return nil
}
func verifyOne(ctx context.Context, cfg config.Config, s manifest.Store, dav *webdav.Client, p manifest.Pack, full bool) error {
	if err := ensureMeta(ctx, cfg, s, dav, p); err != nil {
		return err
	}
	remote := p.RemotePath
	if remote == "" {
		remote = remotePath(p)
	}
	size, err := dav.Stat(ctx, remote)
	if err != nil {
		return err
	}
	if size != p.Bytes {
		return errors.New("remote pack size mismatch")
	} // The previous sampler read the first chunk; a pack has one chunk, so verify its full hash.
	r, _, err := dav.Get(ctx, remote)
	if err != nil {
		return err
	}
	h := sha256.New()
	_, err = io.Copy(h, r)
	r.Close()
	if err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != p.SHA256 {
		return errors.New("remote verification checksum mismatch")
	}
	return nil
}
func deleteFiles(cfg config.Config, s manifest.Store) error {
	ps, err := s.ListByState(manifest.StateVerified)
	if err != nil {
		return err
	}
	for _, p := range ps {
		if p.VerifiedAt.IsZero() {
			return errors.New("missing verification time")
		}
		if err = pack.RemoveRaw(filepath.Join(cfg.DataDir, "raw"), p.Venue, p.Symbol, fmt.Sprintf("%08d", p.Day)); err != nil {
			return err
		}
		if err = os.Remove(filepath.Join(cfg.DataDir, "packs", filepath.FromSlash(p.Path))); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err = s.SetState(p.ID, manifest.StateDeleted); err != nil {
			return err
		}
	}
	return nil
}
