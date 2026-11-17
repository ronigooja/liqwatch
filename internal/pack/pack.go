// Package pack creates offline daily archives of plain or compressed raw files.
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
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/ronigooja/liqwatch/internal/rawlog"
)

const (
	TarThenZstd = "tar_then_zstd"
	ZstdThenTar = "zstd_then_tar"
	StoredTar   = "stored_tar"
)

var ErrIncomplete = errors.New("raw day is not complete")

// A sequential caller reuses one encoder across packs; concurrent callers never
// share an encoder. The pool may be cleared by GC without affecting correctness.
var encoders sync.Pool

// Tests replace this hook only while no packs are running.
var dropSourceCache = dropFileCache

type packEncoder struct {
	*zstd.Encoder
	level int
}

func acquireEncoder(level int) (*packEncoder, error) {
	if cached := encoders.Get(); cached != nil {
		e := cached.(*packEncoder)
		if e.level == level {
			return e, nil
		}
		e.Close()
	}
	e, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)),
		zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20), zstd.WithLowerEncoderMem(true))
	if err != nil {
		return nil, err
	}
	return &packEncoder{e, level}, nil
}

// Read through EOF rather than limiting input: growth and truncation must fail,
// even when timestamps have been restored by the writer.
func copyMember(dst io.Writer, src *os.File, size int64) (int64, error) {
	return copyMemberBuffer(dst, src, size, make([]byte, 32<<10))
}

func copyMemberBuffer(dst io.Writer, src *os.File, size int64, buf []byte) (int64, error) {
	// Hide File.WriteTo so io.CopyBuffer uses the pack's reusable buffer.
	n, err := io.CopyBuffer(dst, struct{ io.Reader }{src}, buf)
	if n != size || errors.Is(err, tar.ErrWriteTooLong) {
		return n, ErrIncomplete
	}
	return n, err
}

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
	Unit    string
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
	buf := make([]byte, 32<<10)
	return pack(rawRoot, venue, symbol, day, opts, func(dst io.Writer, src *os.File, size int64) (int64, error) {
		return copyMemberBuffer(dst, src, size, buf)
	})
}

func pack(rawRoot, venue, symbol, day string, opts Options, copyFile func(io.Writer, *os.File, int64) (int64, error)) (Result, error) {
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
	hasCompressed := false
	for _, e := range entries {
		if !rawlog.IsRawPath(e.Name()) {
			return result, fmt.Errorf("unsupported raw member %s", e.Name())
		}
		hasCompressed = hasCompressed || strings.HasSuffix(e.Name(), ".jsonl.zst")
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
	// Transition days preserve every source byte, including plain members.
	// No compressed input is sent through another compressor.
	if hasCompressed {
		opts.Unit = StoredTar
	}
	result.Unit = opts.Unit
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	ext := ".tar.zst"
	if opts.Unit != TarThenZstd {
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
	defer tmp.Close()
	encoder, err := acquireEncoder(opts.Level)
	if err != nil {
		return result, err
	}
	defer func() {
		encoder.Close()
		encoder.Reset(nil) // Do not retain files or tar writers in the pool.
		encoders.Put(encoder)
	}()
	var output io.Writer = tmp
	var outer *zstd.Encoder
	if opts.Unit == TarThenZstd {
		outer = encoder.Encoder
		outer.Reset(tmp)
		output = outer
	}
	// Compressed member sizes are unknown until compression finishes. Spool one
	// member on disk (not in RAM) before writing its tar header.
	var spool *os.File
	if opts.Unit == ZstdThenTar {
		spool, err = os.CreateTemp(filepath.Dir(dst), ".member-*")
		if err != nil {
			return result, err
		}
		defer os.Remove(spool.Name())
		defer spool.Close()
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
		f, openErr := os.Open(path)
		if openErr != nil {
			return result, openErr
		}
		h := sha256.New()
		var memberOutput io.Writer = tw
		if opts.Unit == ZstdThenTar {
			if err := spool.Truncate(0); err != nil {
				f.Close()
				return result, err
			}
			if _, err := spool.Seek(0, io.SeekStart); err != nil {
				f.Close()
				return result, err
			}
			encoder.Reset(spool)
			memberOutput = encoder
		} else if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0640, Size: before.Size()}); err != nil {
			f.Close()
			return result, err
		}
		n, readErr := copyFile(io.MultiWriter(memberOutput, h), f, before.Size())
		if readErr == nil {
			// Past-day sources are clean; never sync the recorder's files.
			dropSourceCache(f)
		}
		closeErr := f.Close()
		if readErr != nil {
			return result, readErr
		}
		if closeErr != nil {
			return result, closeErr
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
		result.Members = append(result.Members, Member{Name: name, Bytes: n, SHA256: hex.EncodeToString(h.Sum(nil))})
		if opts.Unit == ZstdThenTar {
			if err := encoder.Close(); err != nil {
				return result, err
			}
			size, err := spool.Seek(0, io.SeekCurrent)
			if err != nil {
				return result, err
			}
			if _, err := spool.Seek(0, io.SeekStart); err != nil {
				return result, err
			}
			if err := tw.WriteHeader(&tar.Header{Name: name + ".zst", Mode: 0640, Size: size}); err != nil {
				return result, err
			}
			if _, err := io.Copy(tw, spool); err != nil {
				return result, err
			}
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
	// The writer was synced above. Drop after hashing to avoid warming it again.
	dropFileCache(f)
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
		var memberDecoder *zstd.Decoder
		if unit == ZstdThenTar {
			if !strings.HasSuffix(name, ".zst") {
				return nil, errors.New("invalid compressed member")
			}
			name = strings.TrimSuffix(name, ".zst")
			decoder, err := zstd.NewReader(tr)
			if err != nil {
				return nil, err
			}
			memberDecoder = decoder
			r = decoder
		}
		h := sha256.New()
		n, err := io.Copy(h, r)
		if memberDecoder != nil {
			memberDecoder.Close()
		}
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
