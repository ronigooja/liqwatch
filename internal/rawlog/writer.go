package rawlog

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	ChMarkPrice = "markPrice_all"
	ChQuote     = "bookTicker"
	ChTrade     = "aggTrade"
)

var dropAt = map[string]float64{"mark": .50, "quote": .75, "trade": .90}

func defaultDropClass(channel string) (string, bool) {
	switch channel {
	case ChMarkPrice:
		return "mark", true
	case ChQuote:
		return "quote", true
	case ChTrade:
		return "trade", true
	default:
		return "", false
	}
}

type Closed struct {
	Path, Venue, Symbol, Channel string
	Day                          time.Time
	Bytes, MsgCount              int64
}
type Options struct {
	Dir          string
	QueueSize    int
	FlushEvery   time.Duration
	MaxDiskPct   float64
	OnClosed     func(Closed)
	Instance     string
	MaxOpenFiles int
	Fsync        bool
	DropClass    func(channel string) (class string, ok bool)
}
type Stats struct {
	Written              int64
	QueueDepth, QueueCap int
	Dropped              map[string]int64
	DiskStops            int64
}
type item struct {
	group string
	rec   Record
}
type outFile struct {
	f    *os.File
	buf  *bufio.Writer
	meta Closed
	elem *list.Element
}
type Writer struct {
	opts           Options
	q              chan item
	done           chan struct{}
	wseq           uint64
	written, stops atomic.Int64
	dropCnt        map[string]*atomic.Int64
	marked         map[string]int64
	venueMu        sync.Mutex
	venues         map[string]string
}

// openPaths prevents Sweep from reporting files held by a writer in this process.
var openPaths = struct {
	sync.Mutex
	paths map[string]int
}{paths: make(map[string]int)}

func absolute(p string) string {
	a, err := filepath.Abs(p)
	if err != nil {
		return filepath.Clean(p)
	}
	return a
}

func NewWriter(opts Options) (*Writer, error) {
	if opts.QueueSize <= 0 {
		opts.QueueSize = 200000
	}
	if opts.FlushEvery <= 0 {
		opts.FlushEvery = 5 * time.Second
	}
	if opts.MaxOpenFiles <= 0 {
		opts.MaxOpenFiles = 4096
	}
	if opts.DropClass == nil {
		opts.DropClass = defaultDropClass
	}
	if !validInstance(opts.Instance) {
		return nil, fmt.Errorf("rawlog: invalid instance %q", opts.Instance)
	}
	if err := os.MkdirAll(opts.Dir, 0750); err != nil {
		return nil, err
	}
	w := &Writer{opts: opts, q: make(chan item, opts.QueueSize), done: make(chan struct{}), dropCnt: map[string]*atomic.Int64{}, marked: map[string]int64{}, venues: map[string]string{}}
	return w, nil
}
func (w *Writer) Write(group string, r Record) {
	class, classified := w.opts.DropClass(r.Channel)
	if thr, ok := dropAt[class]; classified && ok {
		w.venueMu.Lock()
		w.venues[r.Channel] = venue(r.Venue)
		counter := w.dropCnt[r.Channel]
		if counter == nil {
			counter = new(atomic.Int64)
			w.dropCnt[r.Channel] = counter
		}
		w.venueMu.Unlock()
		if float64(len(w.q)) >= thr*float64(cap(w.q)) {
			counter.Add(1)
			return
		}
		select {
		case w.q <- item{group, r}:
		default:
			counter.Add(1)
		}
		return
	}
	select {
	case w.q <- item{group, r}:
	case <-w.done:
	}
}
func (w *Writer) Stats() Stats {
	s := Stats{Written: w.written.Load(), QueueDepth: len(w.q), QueueCap: cap(w.q), DiskStops: w.stops.Load(), Dropped: map[string]int64{}}
	w.venueMu.Lock()
	for ch, c := range w.dropCnt {
		s.Dropped[ch] = c.Load()
	}
	w.venueMu.Unlock()
	return s
}
func day(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
func venue(s string) string {
	if s == "" {
		return "binance"
	}
	return s
}

// component rejects unsafe components rather than allowing path traversal or collisions.
func component(s string) bool {
	if s == "" || s == "." || s == ".." {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func validInstance(s string) bool {
	if len(s) > 8 {
		return false
	}
	for _, r := range s {
		if r < 'a' || r > 'z' {
			return false
		}
	}
	return true
}
func (w *Writer) Run(ctx context.Context) {
	defer close(w.done)
	files := map[string]*outFile{}
	streams := map[string]string{}
	// counts survives LRU eviction; callbacks report all writes in this run.
	counts := map[string]int64{}
	lru := list.New()
	var buf bytes.Buffer
	ticker := time.NewTicker(w.opts.FlushEvery)
	defer ticker.Stop()
	closeFile := func(path string, rotated bool) {
		o := files[path]
		if o == nil {
			return
		}
		if err := o.buf.Flush(); err != nil {
			log.Printf("rawlog: flush %s: %v", path, err)
		}
		if w.opts.Fsync {
			if err := o.f.Sync(); err != nil {
				log.Printf("rawlog: sync %s: %v", path, err)
			}
		}
		st, err := o.f.Stat()
		if err != nil {
			log.Printf("rawlog: stat %s: %v", path, err)
		}
		if err := o.f.Close(); err != nil {
			log.Printf("rawlog: close %s: %v", path, err)
		}
		openPaths.Lock()
		p := absolute(filepath.Join(w.opts.Dir, path))
		openPaths.paths[p]--
		if openPaths.paths[p] == 0 {
			delete(openPaths.paths, p)
		}
		openPaths.Unlock()
		delete(files, path)
		stream := filepath.Join(o.meta.Venue, o.meta.Symbol, o.meta.Channel)
		if streams[stream] == path {
			delete(streams, stream)
		}
		lru.Remove(o.elem)
		if rotated {
			if st != nil {
				o.meta.Bytes = st.Size()
			}
			o.meta.MsgCount = counts[path]
			delete(counts, path)
			if w.opts.OnClosed != nil {
				w.opts.OnClosed(o.meta)
			}
		}
	}
	emit := func(group string, r Record) {
		v := venue(r.Venue)
		sym := r.Symbol
		ch := r.Channel
		if group == GroupGap || ch == "gap" {
			sym = "gap"
			ch = "gap"
		} else if sym == "" {
			sym = "_all"
		}
		if !component(v) || !component(sym) || !component(ch) {
			log.Printf("rawlog: invalid path components %q %q %q", v, sym, ch)
			return
		}
		d := day(time.Unix(0, r.RecvWal))
		name := ch
		if w.opts.Instance != "" {
			name += "." + w.opts.Instance
		}
		name += ".jsonl"
		path := filepath.Join(v, sym, d.Format("20060102"), name)
		// Rotate only the same stream, not unrelated symbols or channels.
		stream := filepath.Join(v, sym, ch)
		if p := streams[stream]; p != "" && p != path {
			closeFile(p, true)
		}
		o := files[path]
		if o == nil {
			if len(files) >= w.opts.MaxOpenFiles {
				closeFile(lru.Back().Value.(string), false)
			}
			full := filepath.Join(w.opts.Dir, path)
			if err := os.MkdirAll(filepath.Dir(full), 0750); err != nil {
				log.Printf("rawlog: mkdir %s: %v", path, err)
				return
			}
			// Register while opening so Sweep cannot observe an open file as closed.
			openPaths.Lock()
			f, err := os.OpenFile(full, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
			if err == nil {
				openPaths.paths[absolute(full)]++
			}
			openPaths.Unlock()
			if err != nil {
				log.Printf("rawlog: open %s: %v", path, err)
				return
			}
			o = &outFile{f: f, buf: bufio.NewWriter(f), meta: Closed{Path: path, Venue: v, Symbol: sym, Channel: ch, Day: d}}
			o.elem = lru.PushFront(path)
			files[path] = o
			streams[stream] = path
		} else {
			lru.MoveToFront(o.elem)
		}
		w.wseq++
		buf.Reset()
		buf.WriteString(`{"v":1,"wseq":`)
		buf.WriteString(strconv.FormatUint(w.wseq, 10))
		buf.WriteString(`,"conn":`)
		buf.WriteString(strconv.Quote(r.Conn))
		buf.WriteString(`,"cseq":`)
		buf.WriteString(strconv.FormatUint(r.CSeq, 10))
		buf.WriteString(`,"rw":`)
		buf.WriteString(strconv.FormatInt(r.RecvWal, 10))
		buf.WriteString(`,"rm":`)
		buf.WriteString(strconv.FormatInt(r.RecvMon, 10))
		buf.WriteString(`,"ch":`)
		buf.WriteString(strconv.Quote(r.Channel))
		buf.WriteString(`,"sym":`)
		buf.WriteString(strconv.Quote(r.Symbol))
		buf.WriteString(`,"venue":`)
		buf.WriteString(strconv.Quote(v))
		buf.WriteString(`,"p":`)
		buf.Write(r.Payload)
		buf.WriteString("}\n")
		n, err := o.buf.Write(buf.Bytes())
		if err != nil {
			log.Printf("rawlog: write %s: %v", path, err)
		}
		if err == nil {
			counts[path]++
		}
		w.written.Add(int64(n))
	}
	var diskStop bool
	var lastDisk time.Time
	lastMark := time.Now()
	handle := func(it item) {
		if time.Since(lastDisk) > 10*time.Second {
			lastDisk = time.Now()
			if w.opts.MaxDiskPct > 0 {
				pct := diskPct(w.opts.Dir)
				if (pct >= w.opts.MaxDiskPct) != diskStop {
					log.Printf("rawlog: disk at %.1f%%, stopped=%t", pct, pct >= w.opts.MaxDiskPct)
				}
				diskStop = pct >= w.opts.MaxDiskPct
			}
		}
		if diskStop {
			w.stops.Add(1)
			return
		}
		emit(it.group, it.rec)
	}
	markDrops := func() {
		now := time.Now()
		w.venueMu.Lock()
		for ch, c := range w.dropCnt {
			total := c.Load()
			if d := total - w.marked[ch]; d > 0 {
				w.marked[ch] = total
				v := w.venues[ch]
				p := fmt.Sprintf(`{"event":"drop","channel":%q,"count":%d,"since_ns":%d}`, ch, d, lastMark.UnixNano())
				emit(GroupGap, Record{Venue: v, Conn: "writer", RecvWal: now.UnixNano(), RecvMon: monoNow(), Channel: "gap", Payload: []byte(p)})
			}
		}
		w.venueMu.Unlock()
		lastMark = now
	}
	for {
		select {
		case it := <-w.q:
			handle(it)
		case <-ticker.C:
			markDrops()
			current := day(time.Now())
			for p, o := range files {
				if o.meta.Day.Before(current) {
					closeFile(p, true)
				} else {
					if err := o.buf.Flush(); err != nil {
						log.Printf("rawlog: flush %s: %v", p, err)
					}
					if w.opts.Fsync {
						if err := o.f.Sync(); err != nil {
							log.Printf("rawlog: sync %s: %v", p, err)
						}
					}
				}
			}
		case <-ctx.Done():
			for {
				select {
				case it := <-w.q:
					handle(it)
				default:
					markDrops()
					for p := range files {
						closeFile(p, false)
					}
					return
				}
			}
		}
	}
}

var processStart = time.Now()

func Mono() int64    { return int64(time.Since(processStart)) }
func monoNow() int64 { return Mono() }

// Sweep returns past-day files not held open by writers in this process.
func Sweep(dir string, now time.Time) []Closed {
	var out []Closed
	files, _ := filepath.Glob(filepath.Join(dir, "*", "*", "*", "*.jsonl"))
	current := day(now)
	for _, p := range files {
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			continue
		}
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) != 4 {
			continue
		}
		d, err := time.Parse("20060102", parts[2])
		if err != nil || !d.Before(current) {
			continue
		}
		name := strings.TrimSuffix(parts[3], ".jsonl")
		ch := name
		if i := strings.LastIndexByte(name, '.'); i >= 0 && validInstance(name[i+1:]) && name[i+1:] != "" {
			ch = name[:i]
		}
		if !component(parts[0]) || !component(parts[1]) || !component(ch) {
			continue
		}
		openPaths.Lock()
		held := openPaths.paths[absolute(p)] > 0
		var st os.FileInfo
		if !held {
			st, err = os.Stat(p)
		}
		openPaths.Unlock()
		if held || err != nil || !st.Mode().IsRegular() {
			continue
		}
		out = append(out, Closed{Path: rel, Venue: parts[0], Symbol: parts[1], Channel: ch, Day: d, Bytes: st.Size(), MsgCount: -1})
	}
	return out
}
func diskPct(path string) float64 {
	var s syscall.Statfs_t
	if syscall.Statfs(path, &s) != nil {
		return 0
	}
	total := float64(s.Blocks) * float64(s.Bsize)
	if total == 0 {
		return 0
	}
	return (total - float64(s.Bavail)*float64(s.Bsize)) / total * 100
}

var _ Sink = (*Writer)(nil)

// Done is closed when Run has drained its queue and closed its files.
func (w *Writer) Done() <-chan struct{} { return w.done }
