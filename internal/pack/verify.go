package pack

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// ErrBlocked identifies a policy exclusion, not corrupt data or an I/O failure.
var ErrBlocked = errors.New("archive blocked by rule")

// ReadLimiter paces physical reads with a bounded one-buffer burst, across files.
type ReadLimiter struct {
	Context context.Context
	KBps    int
	next    time.Time
}

type pacedReader struct {
	file    *os.File
	limiter *ReadLimiter
}

func (r pacedReader) Read(b []byte) (int, error) {
	l := r.limiter
	if err := l.Context.Err(); err != nil {
		return 0, err
	}
	if len(b) > 32<<10 {
		b = b[:32<<10]
	}
	if l.KBps > 0 && time.Now().Before(l.next) {
		t := time.NewTimer(time.Until(l.next))
		defer t.Stop()
		select {
		case <-l.Context.Done():
			return 0, l.Context.Err()
		case <-t.C:
		}
	}
	n, err := r.file.Read(b)
	if l.KBps > 0 {
		l.next = time.Now().Add(time.Duration(float64(n) / float64(l.KBps*1024) * float64(time.Second)))
	}
	return n, err
}

func hashReader(r io.Reader) (int64, string, error) {
	h := sha256.New()
	n, err := io.CopyBuffer(h, r, make([]byte, 32<<10))
	return n, hex.EncodeToString(h.Sum(nil)), err
}

// VerifyLocal rereads the entire pack, its tar members and the still-present
// sources. It never extracts members or changes any file. Call immediately
// before deletion; no durable writer exclusion is provided by this function.
func VerifyLocal(rawRoot, packRoot, venue, symbol, day, relative string, size int64, sum string, members []Member, quiet time.Duration, now time.Time, limiter *ReadLimiter) error {
	if err := validate(venue, symbol, day); err != nil {
		return err
	}
	if day >= now.UTC().Format("20060102") {
		return fmt.Errorf("%w: current/future UTC day", ErrBlocked)
	}
	if quiet <= 0 {
		quiet = 10 * time.Minute
	}
	// Refuse symlink aliases at every path component we rely on for deletion.
	dir := rawRoot
	for _, component := range []string{"", venue, symbol, day} {
		dir = filepath.Join(dir, component)
		st, err := os.Lstat(dir)
		if err != nil {
			return err
		}
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe source directory %s", dir)
		}
	}
	if relative != filepath.Join(venue, symbol, day+".tar") && relative != filepath.Join(venue, symbol, day+".tar.zst") {
		return errors.New("unexpected pack path")
	}
	want := make(map[string]Member, len(members))
	for _, m := range members {
		if !safe(m.Name) || m.Bytes < 0 || len(m.SHA256) != 64 {
			return errors.New("invalid manifest member")
		}
		if _, ok := want[m.Name]; ok {
			return errors.New("duplicate manifest member")
		}
		want[m.Name] = m
	}
	if len(want) == 0 || len(sum) != 64 {
		return errors.New("missing manifest checksums/members")
	}
	checkEntries := func() error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if _, ok := want[e.Name()]; !ok {
				return fmt.Errorf("%w: source-day leftover %s", ErrBlocked, e.Name())
			}
			st, err := e.Info()
			if err != nil {
				return err
			}
			if !st.Mode().IsRegular() {
				return fmt.Errorf("non-regular source %s", e.Name())
			}
			if !st.ModTime().Before(now.Add(-quiet)) {
				return fmt.Errorf("%w: quiet period %s", ErrBlocked, e.Name())
			}
		}
		return nil
	}
	if err := checkEntries(); err != nil {
		return err
	}
	f, err := os.Open(filepath.Join(packRoot, relative))
	if err != nil {
		return err
	}
	defer f.Close()
	defer dropFileCache(f)
	n, got, err := hashReader(pacedReader{f, limiter})
	if err != nil {
		return err
	}
	if n != size || got != sum {
		return errors.New("pack checksum or size mismatch")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var in io.Reader = pacedReader{f, limiter}
	if strings.HasSuffix(relative, ".tar.zst") {
		d, err := verifyDecoder(in)
		if err != nil {
			return err
		}
		defer d.Close()
		in = d
	}
	tr := tar.NewReader(in)
	seen := make(map[string]bool, len(want))
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		name := hdr.Name
		var r io.Reader = tr
		var dec *zstd.Decoder
		// A plain source in legacy zstd_then_tar gets an added .zst suffix.
		if _, ok := want[name]; !ok && strings.HasSuffix(name, ".jsonl.zst") {
			name = strings.TrimSuffix(name, ".zst")
			dec, err = verifyDecoder(tr)
			if err != nil {
				return err
			}
			r = dec
		}
		m, ok := want[name]
		if !ok || seen[name] || hdr.Typeflag != tar.TypeReg {
			if dec != nil {
				dec.Close()
			}
			return fmt.Errorf("unexpected tar member %s", hdr.Name)
		}
		n, got, err := hashReader(r)
		if dec != nil {
			dec.Close()
		}
		if err != nil {
			return err
		}
		if n != m.Bytes || got != m.SHA256 {
			return fmt.Errorf("tar member checksum or size mismatch: %s", name)
		}
		seen[name] = true
	}
	if len(seen) != len(want) {
		return errors.New("manifest member missing in tar")
	}
	for _, m := range members {
		filename := filepath.Join(dir, m.Name)
		before, err := os.Lstat(filename)
		if err != nil {
			return err
		}
		if !before.Mode().IsRegular() {
			return fmt.Errorf("non-regular source %s", m.Name)
		}
		if before.Size() != m.Bytes || m.MtimeNS != 0 && before.ModTime().UnixNano() != m.MtimeNS {
			return fmt.Errorf("source size/mtime changed: %s", m.Name)
		}
		src, err := os.Open(filename)
		if err != nil {
			return err
		}
		n, got, readErr := hashReader(pacedReader{src, limiter})
		dropFileCache(src)
		closeErr := src.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		after, err := os.Lstat(filename)
		if err != nil {
			return err
		}
		if n != m.Bytes || got != m.SHA256 || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
			return fmt.Errorf("source checksum/identity changed: %s", m.Name)
		}
	}
	return checkEntries()
}

func verifyDecoder(r io.Reader) (*zstd.Decoder, error) {
	return zstd.NewReader(r, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
}
