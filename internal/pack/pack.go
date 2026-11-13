// Package pack creates offline daily archives of uncompressed raw files.
package pack

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	TarThenZstd = "tar_then_zstd"
	ZstdThenTar = "zstd_then_tar"
)

var ErrIncomplete = errors.New("raw day is not complete")

type Options struct {
	PackRoot    string
	Unit        string
	Level       int
	QuietPeriod time.Duration
	Now         time.Time
}

type Member struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type Result struct {
	Path    string
	Bytes   int64
	SHA256  string
	Members []Member
	Skipped bool
}

func safe(s string) bool {
	return s != "" && s != "." && s != ".." && filepath.Base(s) == s && !strings.ContainsAny(s, `/\`)
}

func validate(venue, symbol, day string) error {
	if !safe(venue) || !safe(symbol) || len(day) != 8 {
		return errors.New("invalid venue, symbol or day")
	}
	if _, err := time.Parse("20060102", day); err != nil {
		return err
	}
	return nil
}

// Pack writes an atomic daily archive. Path in the result is relative to PackRoot.
func Pack(rawRoot, venue, symbol, day string, opts Options) (Result, error) {
	var result Result
	if err := validate(venue, symbol, day); err != nil {
		return result, err
	}
	if opts.Unit == "" {
		opts.Unit = TarThenZstd
	}
	if opts.Unit != TarThenZstd && opts.Unit != ZstdThenTar {
		return result, errors.New("invalid compression unit")
	}
	if opts.Level == 0 {
		opts.Level = 3
	}
	if opts.QuietPeriod == 0 {
		opts.QuietPeriod = 10 * time.Minute
	}
	if opts.QuietPeriod < 0 {
		return result, errors.New("invalid quiet period")
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	if day >= opts.Now.UTC().Format("20060102") {
		return result, ErrIncomplete
	}
	if opts.PackRoot == "" {
		return result, errors.New("pack root is required")
	}
	dir := filepath.Join(rawRoot, venue, symbol, day)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return result, err
	}
	if len(entries) == 0 {
		return result, ErrIncomplete
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			return result, err
		}
		if !info.Mode().IsRegular() {
			return result, fmt.Errorf("non-regular member %s", e.Name())
		}
		if !info.ModTime().Before(opts.Now.Add(-opts.QuietPeriod)) {
			return result, ErrIncomplete
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	ext := ".tar.zst"
	if opts.Unit == ZstdThenTar {
		ext = ".tar"
	}
	result.Path = filepath.Join(venue, symbol, day+ext)
	dst := filepath.Join(opts.PackRoot, result.Path)
	if err := os.MkdirAll(filepath.Dir(dst), 0750); err != nil {
		return result, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".pack-*")
	if err != nil {
		return result, err
	}
	defer os.Remove(tmp.Name())
	var output io.Writer = tmp
	var outer *zstd.Encoder
	if opts.Unit == TarThenZstd {
		outer, err = zstd.NewWriter(tmp, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(opts.Level)))
		if err != nil {
			tmp.Close()
			return result, err
		}
		output = outer
	}
	tw := tar.NewWriter(output)
	for _, e := range entries {
		name := e.Name()
		path := filepath.Join(dir, name)
		before, statErr := os.Stat(path)
		if statErr != nil {
			tmp.Close()
			return result, statErr
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			tmp.Close()
			return result, readErr
		}
		after, statErr := os.Stat(path)
		if statErr != nil {
			tmp.Close()
			return result, statErr
		}
		if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) || !after.ModTime().Before(opts.Now.Add(-opts.QuietPeriod)) {
			tmp.Close()
			return result, ErrIncomplete
		}
		sum := sha256.Sum256(data)
		result.Members = append(result.Members, Member{Name: name, Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])})
		if opts.Unit == ZstdThenTar {
			encoder, encErr := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(opts.Level)))
			if encErr != nil {
				tmp.Close()
				return result, encErr
			}
			data = encoder.EncodeAll(data, nil)
			encoder.Close()
			name += ".zst"
		}
		if err = tw.WriteHeader(&tar.Header{Name: name, Mode: 0640, Size: int64(len(data))}); err == nil {
			_, err = tw.Write(data)
		}
		if err != nil {
			tmp.Close()
			return result, err
		}
	}
	if err = tw.Close(); err == nil && outer != nil {
		err = outer.Close()
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return result, err
	}
	f, err := os.Open(tmp.Name())
	if err != nil {
		return result, err
	}
	h := sha256.New()
	result.Bytes, err = io.Copy(h, f)
	f.Close()
	if err != nil {
		return result, err
	}
	result.SHA256 = hex.EncodeToString(h.Sum(nil))
	if existing, openErr := os.Open(dst); openErr == nil {
		members, readErr := readMembers(existing, opts.Unit)
		if readErr == nil && reflect.DeepEqual(members, result.Members) {
			if _, err := existing.Seek(0, io.SeekStart); err != nil {
				existing.Close()
				return result, err
			}
			old := sha256.New()
			result.Bytes, err = io.Copy(old, existing)
			existing.Close()
			if err != nil {
				return result, err
			}
			result.SHA256 = hex.EncodeToString(old.Sum(nil))
			result.Skipped = true
			return result, nil
		}
		existing.Close()
	} else if !errors.Is(openErr, os.ErrNotExist) {
		return result, openErr
	}
	if err = os.Rename(tmp.Name(), dst); err != nil {
		return result, err
	}
	return result, nil
}

func readMembers(in io.Reader, unit string) ([]Member, error) {
	if unit == TarThenZstd {
		decoder, err := zstd.NewReader(in)
		if err != nil {
			return nil, err
		}
		defer decoder.Close()
		in = decoder
	}
	tr := tar.NewReader(in)
	var members []Member
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag != tar.TypeReg || !safe(hdr.Name) {
			return nil, errors.New("invalid archive member")
		}
		name := hdr.Name
		var r io.Reader = tr
		if unit == ZstdThenTar {
			if !strings.HasSuffix(name, ".zst") {
				return nil, errors.New("invalid compressed member")
			}
			name = strings.TrimSuffix(name, ".zst")
			decoder, err := zstd.NewReader(tr)
			if err != nil {
				return nil, err
			}
			defer decoder.Close()
			r = decoder
		}
		h := sha256.New()
		n, err := io.Copy(h, r)
		if err != nil {
			return nil, err
		}
		members = append(members, Member{Name: name, Bytes: n, SHA256: hex.EncodeToString(h.Sum(nil))})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
	return members, nil
}

// RemoveRaw deletes a verified archive's raw day directory.
func RemoveRaw(rawRoot, venue, symbol, day string) error {
	if err := validate(venue, symbol, day); err != nil {
		return err
	}
	return os.RemoveAll(filepath.Join(rawRoot, venue, symbol, day))
}
