package rawlog

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// MaxLineBytes bounds one raw record, including its terminating newline.
const MaxLineBytes = 16 << 20

// ReadInfo describes stored input and successfully delivered complete records.
type ReadInfo struct {
	Compressed    bool
	TruncatedTail bool
	StoredBytes   int64
	Records       int64
	DecodedBytes  int64
}

// IsRawPath recognizes both supported raw-file suffixes.
func IsRawPath(path string) bool {
	return strings.HasSuffix(path, ".jsonl") || strings.HasSuffix(path, ".jsonl.zst")
}

// ReadLines streams newline-terminated records without their newline. The line
// is borrowed until the callback returns. Incomplete final records are never
// delivered. Unexpected EOF in the final zstd frame is recoverable; all other
// decoder errors (including checksum errors) fail the file.
func ReadLines(path string, visit func([]byte) error) (ReadInfo, error) {
	info := ReadInfo{Compressed: strings.HasSuffix(path, ".jsonl.zst")}
	if !IsRawPath(path) {
		return info, fmt.Errorf("unsupported raw path: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return info, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return info, err
	}
	info.StoredBytes = st.Size()
	b := bufio.NewReader(f)
	magic, _ := b.Peek(4)
	want := []byte{0x28, 0xb5, 0x2f, 0xfd}
	if len(magic) != 0 {
		if info.Compressed {
			if !bytes.Equal(magic, want[:len(magic)]) {
				return info, errors.New("raw zstd magic mismatch")
			}
		} else if bytes.Equal(magic, want) {
			return info, errors.New("zstd input requires .jsonl.zst suffix")
		}
	}
	var input io.Reader = b
	if info.Compressed {
		d, err := zstd.NewReader(b, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
		if err != nil {
			return info, err
		}
		defer d.Close()
		input = d
	}
	r := bufio.NewReaderSize(input, 64<<10)
	var line []byte
	for {
		part, err := r.ReadSlice('\n')
		if len(line)+len(part) > MaxLineBytes {
			return info, errors.New("raw record exceeds MaxLineBytes")
		}
		line = append(line, part...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err == nil {
			if e := visit(line[:len(line)-1]); e != nil {
				return info, e
			}
			info.Records++
			info.DecodedBytes += int64(len(line))
			line = line[:0]
			continue
		}
		if errors.Is(err, io.EOF) || info.Compressed && errors.Is(err, io.ErrUnexpectedEOF) {
			info.TruncatedTail = len(line) != 0 || errors.Is(err, io.ErrUnexpectedEOF)
			return info, nil
		}
		return info, err
	}
}
