package rawlog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func readFixture(t *testing.T, name string, b []byte) (ReadInfo, []string, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	var lines []string
	info, err := ReadLines(p, func(b []byte) error { lines = append(lines, string(b)); return nil })
	return info, lines, err
}

func TestReadLinesGolden(t *testing.T) {
	a := "{\"v\":2,\"wseq\":1,\"p\":{\"price\":\"1.2300\"}}"
	b := "{\"v\":2,\"wseq\":2,\"p\":{\"price\":\"2.3400\"}}"
	want := []string{a, b}
	e, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	plain := []byte(a + "\n" + b + "\n")
	compressed := e.EncodeAll(plain, nil)
	frames := append(e.EncodeAll([]byte(a+"\n"), nil), e.EncodeAll([]byte(b+"\n"), nil)...)
	for _, tc := range []struct {
		name       string
		data       []byte
		compressed bool
	}{
		{"plain.jsonl", plain, false}, {"one.jsonl.zst", compressed, true}, {"sessions.jsonl.zst", frames, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info, got, err := readFixture(t, tc.name, tc.data)
			if err != nil || !reflect.DeepEqual(got, want) || info.TruncatedTail || info.Compressed != tc.compressed || info.Records != 2 || info.StoredBytes != int64(len(tc.data)) {
				t.Fatalf("%+v %q %v", info, got, err)
			}
		})
	}
	for n := 0; n < len(compressed); n++ {
		info, got, err := readFixture(t, "cut.jsonl.zst", compressed[:n])
		if err != nil || len(got) > len(want) || !reflect.DeepEqual(got, want[:len(got)]) && len(got) > 0 || n > 0 && !info.TruncatedTail {
			t.Fatalf("cut %d: %+v %q %v", n, info, got, err)
		}
	}
	// Reopened files must preserve earlier frames even if the next header/body is cut.
	first := e.EncodeAll([]byte(a+"\n"), nil)
	second := e.EncodeAll([]byte(b+"\n"), nil)
	for n := 1; n < len(second); n++ {
		info, got, err := readFixture(t, "sessions.jsonl.zst", append(bytes.Clone(first), second[:n]...))
		if err != nil || !info.TruncatedTail || len(got) < 1 || !reflect.DeepEqual(got, want[:len(got)]) {
			t.Fatalf("session cut %d: %+v %q %v", n, info, got, err)
		}
	}
	bad := append(bytes.Clone(first), second...)
	bad[len(first)-1] ^= 0xff // first frame checksum, followed by a valid frame
	if _, _, err := readFixture(t, "bad.jsonl.zst", bad); err == nil {
		t.Fatal("corrupt middle accepted")
	}
	for _, name := range []string{"empty.jsonl", "empty.jsonl.zst"} {
		info, got, err := readFixture(t, name, nil)
		if err != nil || info.TruncatedTail || len(got) != 0 {
			t.Fatalf("empty: %+v %q %v", info, got, err)
		}
	}
	info, got, err := readFixture(t, "partial.jsonl", []byte(a+"\n"+b))
	if err != nil || !info.TruncatedTail || !reflect.DeepEqual(got, []string{a}) {
		t.Fatalf("partial: %+v %q %v", info, got, err)
	}
	if _, _, err := readFixture(t, "wrong.jsonl", compressed); err == nil {
		t.Fatal("magic mismatch accepted")
	}
	if _, _, err := readFixture(t, "wrong.jsonl.zst", plain); err == nil {
		t.Fatal("magic mismatch accepted")
	}
	// A flush is a decodable block boundary, not necessarily a closed frame.
	var live bytes.Buffer
	stream, err := zstd.NewWriter(&live, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte(a + "\n")); err != nil {
		t.Fatal(err)
	}
	if err := stream.Flush(); err != nil {
		t.Fatal(err)
	}
	flushed := bytes.Clone(live.Bytes())
	if _, err := stream.Write([]byte(b + "\n")); err != nil {
		t.Fatal(err)
	}
	if err := stream.Flush(); err != nil {
		t.Fatal(err)
	}
	for n := len(flushed); n <= live.Len(); n++ {
		info, got, err := readFixture(t, "live.jsonl.zst", live.Bytes()[:n])
		if err != nil || !info.TruncatedTail || len(got) < 1 || !reflect.DeepEqual(got, want[:len(got)]) {
			t.Fatalf("flush cut %d: %+v %q %v", n, info, got, err)
		}
	}
	stream.Close()
}

func TestReadLinesBoundAndCallback(t *testing.T) {
	if _, _, err := readFixture(t, "huge.jsonl", bytes.Repeat([]byte{'x'}, MaxLineBytes+1)); err == nil {
		t.Fatal("unbounded record accepted")
	}
	p := filepath.Join(t.TempDir(), "a.jsonl")
	if err := os.WriteFile(p, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	want := errors.New("stop")
	if _, err := ReadLines(p, func([]byte) error { return want }); !errors.Is(err, want) {
		t.Fatal(err)
	}
}
