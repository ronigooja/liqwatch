// liqwatch-replay is an offline, non-networking compression experiment. It keeps
// source envelopes byte-identical and uses the production buffered frame writer.
package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/ronigooja/liqwatch/internal/rawlog"
)

type input struct {
	path string
	f    *os.File
	scan *bufio.Scanner
	line []byte
	rw   int64
}
type pending []*input

func (p pending) Len() int { return len(p) }
func (p pending) Less(i, j int) bool {
	if p[i].rw == p[j].rw {
		return p[i].path < p[j].path
	}
	return p[i].rw < p[j].rw
}
func (p pending) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p *pending) Push(x any)   { *p = append(*p, x.(*input)) }
func (p *pending) Pop() any     { a := *p; x := a[len(a)-1]; *p = a[:len(a)-1]; return x }
func next(in *input) bool {
	if !in.scan.Scan() {
		if err := in.scan.Err(); err != nil {
			panic(err)
		}
		return false
	}
	in.line = append(in.line[:0], in.scan.Bytes()...)
	in.line = append(in.line, '\n')
	var e struct {
		RW int64 `json:"rw"`
	}
	must(json.Unmarshal(in.line, &e))
	in.rw = e.RW
	return true
}

type output struct {
	f    *os.File
	b    *bufio.Writer
	elem *list.Element
}
type totals struct {
	Raw        int64 `json:"raw"`
	Compressed int64 `json:"compressed"`
	Files      int   `json:"files"`
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func digest(p string) (string, int64) {
	f, e := os.Open(p)
	must(e)
	defer f.Close()
	h := sha256.New()
	n, e := io.Copy(h, f)
	must(e)
	return hex.EncodeToString(h.Sum(nil)), n
}
func main() {
	src := flag.String("src", "", "read-only source root")
	dst := flag.String("dst", "", "fresh output root")
	max := flag.Int("max-open", 4096, "output LRU bound")
	hold := flag.Duration("hold", 0, "keep cgroup alive after reporting, for memory.peak sampling")
	flag.Parse()
	if *src == "" || *dst == "" || *max < 1 {
		panic("src, dst and max-open required")
	}
	if _, e := os.Stat(*dst); !os.IsNotExist(e) {
		panic("destination must not exist")
	}
	must(os.MkdirAll(*dst, 0750))
	var paths []string
	before := map[string]string{}
	allBytes := int64(0)
	must(filepath.WalkDir(*src, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.Type().IsRegular() {
			h, n := digest(p)
			before[p] = h
			allBytes += n
			if strings.HasSuffix(p, ".jsonl") {
				paths = append(paths, p)
			}
		}
		return nil
	}))
	sort.Strings(paths)
	var q pending
	for _, p := range paths {
		f, e := os.Open(p)
		must(e)
		s := bufio.NewScanner(f)
		s.Buffer(make([]byte, 4096), 16<<20)
		in := &input{path: p, f: f, scan: s}
		if next(in) {
			heap.Push(&q, in)
		} else {
			must(f.Close())
		}
	}
	enc, e := rawlog.NewFrameEncoder(1)
	must(e)
	files := map[string]*output{}
	lru := list.New()
	closeOut := func(p string) {
		o := files[p]
		must(o.b.Flush())
		must(o.f.Close())
		lru.Remove(o.elem)
		delete(files, p)
	}
	flush := func() {
		for _, o := range files {
			must(o.b.Flush())
		}
	}
	var usage0, usage1 syscall.Rusage
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &usage0))
	start := time.Now()
	var tick int64
	records := int64(0)
	for q.Len() > 0 {
		in := heap.Pop(&q).(*input)
		if tick == 0 {
			tick = in.rw + int64(5*time.Second)
		}
		if in.rw >= tick {
			flush()
			tick = in.rw + int64(5*time.Second)
		}
		rel, e := filepath.Rel(*src, in.path)
		must(e)
		p := filepath.Join(*dst, rel+".zst")
		o := files[p]
		if o == nil {
			if len(files) >= *max {
				closeOut(lru.Back().Value.(string))
			}
			must(os.MkdirAll(filepath.Dir(p), 0750))
			f, e := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
			must(e)
			o = &output{f: f, b: bufio.NewWriter(rawlog.FrameWriter{Encoder: enc, Dst: f})}
			o.elem = lru.PushFront(p)
			files[p] = o
		} else {
			lru.MoveToFront(o.elem)
		}
		_, e = o.b.Write(in.line)
		must(e)
		records++
		if next(in) {
			heap.Push(&q, in)
		} else {
			must(in.f.Close())
		}
	}
	for p := range files {
		closeOut(p)
	}
	must(enc.Close())
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &usage1))
	elapsed := time.Since(start).Seconds()
	sec := func(v syscall.Timeval) float64 { return float64(v.Sec) + float64(v.Usec)/1e6 }
	cpu := sec(usage1.Utime) + sec(usage1.Stime) - sec(usage0.Utime) - sec(usage0.Stime)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	byVenue := map[string]*totals{}
	var total totals
	for _, p := range paths {
		rel, e := filepath.Rel(*src, p)
		must(e)
		venue := strings.Split(rel, string(filepath.Separator))[0]
		if byVenue[venue] == nil {
			byVenue[venue] = &totals{}
		}
		_, n := digest(p)
		out := filepath.Join(*dst, rel+".zst")
		f, e := os.Open(out)
		must(e)
		d, e := zstd.NewReader(f, zstd.WithDecoderConcurrency(1))
		must(e)
		h := sha256.New()
		dn, e := io.Copy(h, d)
		must(e)
		d.Close()
		must(f.Close())
		if hex.EncodeToString(h.Sum(nil)) != before[p] || dn != n {
			panic("decoded hash mismatch: " + p)
		}
		st, e := os.Stat(out)
		must(e)
		byVenue[venue].Raw += n
		byVenue[venue].Compressed += st.Size()
		byVenue[venue].Files++
		total.Raw += n
		total.Compressed += st.Size()
		total.Files++
	}
	for p, h := range before {
		now, _ := digest(p)
		if now != h {
			panic("source changed: " + p)
		}
	}
	// Output contains no source payloads; only aggregate measurements and hashes.
	var fingerprint bytes.Buffer
	keys := make([]string, 0, len(before))
	for p := range before {
		keys = append(keys, p)
	}
	sort.Strings(keys)
	for _, p := range keys {
		fmt.Fprintf(&fingerprint, "%s %s\n", p, before[p])
	}
	sum := sha256.Sum256(fingerprint.Bytes())
	must(json.NewEncoder(os.Stdout).Encode(map[string]any{"source_files": len(before), "source_bytes": allBytes, "source_hash_list_sha256": hex.EncodeToString(sum[:]), "source_unchanged": true, "verified_files": len(paths), "records": records, "max_open": *max, "total": total, "venues": byVenue, "replay_cpu_seconds": cpu, "replay_wall_seconds": elapsed, "heap_sys": m.HeapSys, "heap_alloc": m.HeapAlloc, "rusage_peak_kib": usage1.Maxrss}))
	time.Sleep(*hold)
}
