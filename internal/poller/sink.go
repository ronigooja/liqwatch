package poller

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/ronigooja/liqwatch/internal/rawlog"
)

type Record struct {
	Venue, Symbol, Channel string
	RecvWall, RecvMono     int64
	Meta                   map[string]any
	Payload                []byte
}

type GapRecord = Record

type Sink interface{ Write(Record) }

type fileBuffer struct {
	f *os.File
	w *bufio.Writer
}

// FileSink buffers JSONL records. Err reports asynchronous write errors.
type FileSink struct {
	Dir     string
	mu      sync.Mutex
	files   map[string]*fileBuffer
	err     error
	stop    chan struct{}
	done    chan struct{}
	closed  bool
	encoder *zstd.Encoder
}

func NewFileSink(dir string, interval time.Duration) *FileSink {
	s, _ := NewFileSinkCompressed(dir, interval, false, 1)
	return s
}

// NewFileSinkCompressed retains the legacy path unless compression is enabled.
func NewFileSinkCompressed(dir string, interval time.Duration, compress bool, level int) (*FileSink, error) {
	if interval <= 0 {
		interval = time.Second
	}
	s := &FileSink{Dir: dir, files: make(map[string]*fileBuffer), stop: make(chan struct{}), done: make(chan struct{})}
	if compress {
		var err error
		s.encoder, err = rawlog.NewFrameEncoder(level)
		if err != nil {
			return nil, err
		}
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		defer close(s.done)
		for {
			select {
			case <-t.C:
				s.flush()
			case <-s.stop:
				return
			}
		}
	}()
	return s, nil
}

func safePart(s string) bool { return s != "" && s != "." && s != ".." && filepath.Base(s) == s }

func (s *FileSink) Write(r Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		if s.err == nil {
			s.err = fmt.Errorf("sink closed")
		}
		return
	}
	sym := r.Symbol
	if sym == "" {
		sym = "_all"
	}
	if r.Channel == "gap" {
		sym = "gap"
	}
	if !safePart(r.Venue) || !safePart(sym) || !safePart(r.Channel) {
		if s.err == nil {
			s.err = fmt.Errorf("invalid record path")
		}
		return
	}
	envelope := struct {
		Version int             `json:"v"`
		Wall    int64           `json:"rw"`
		Mono    int64           `json:"rm"`
		Meta    map[string]any  `json:"meta"`
		Payload json.RawMessage `json:"p"`
	}{2, r.RecvWall, r.RecvMono, r.Meta, r.Payload}
	b, err := json.Marshal(envelope)
	if err != nil {
		if s.err == nil {
			s.err = err
		}
		return
	}
	path := filepath.Join(s.Dir, r.Venue, sym, time.Unix(0, r.RecvWall).UTC().Format("20060102"), r.Channel+".p.jsonl")
	if s.encoder != nil {
		path += ".zst"
	}
	f := s.files[path]
	if f == nil {
		if err = os.MkdirAll(filepath.Dir(path), 0755); err == nil {
			var file *os.File
			file, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if err == nil {
				var dst io.Writer = file
				if s.encoder != nil {
					dst = rawlog.FrameWriter{Encoder: s.encoder, Dst: file}
				}
				f = &fileBuffer{file, bufio.NewWriter(dst)}
				s.files[path] = f
			}
		}
	}
	if err == nil {
		_, err = f.w.Write(append(b, '\n'))
	}
	if err != nil && s.err == nil {
		s.err = err
	}
}

func (s *FileSink) flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for path, f := range s.files {
		if err := f.w.Flush(); err != nil && s.err == nil {
			s.err = err
		}
		// Compression keeps no cross-flush stream state. Release idle poller
		// descriptors/buffers each tick rather than accumulating days of files.
		if s.encoder != nil {
			if err := f.f.Close(); err != nil && s.err == nil {
				s.err = err
			}
			delete(s.files, path)
		}
	}
}

func (s *FileSink) Err() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }

func (s *FileSink) Flush() error { s.flush(); return s.Err() }

func (s *FileSink) Close() error {
	s.mu.Lock()
	if s.closed {
		err := s.err
		s.mu.Unlock()
		return err
	}
	s.closed = true
	close(s.stop)
	s.mu.Unlock()
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.files {
		if err := f.w.Flush(); err != nil && s.err == nil {
			s.err = err
		}
		if err := f.f.Close(); err != nil && s.err == nil {
			s.err = err
		}
	}
	if s.encoder != nil {
		s.encoder.Close()
	}
	return s.err
}
