package rawlog

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Drop classes, lowest priority first. Channels not listed are never dropped.
const (
	ChMarkPrice = "markPrice_all"
	ChQuote     = "bookTicker"
	ChTrade     = "aggTrade"
)

// Queue fill ratios above which each class is dropped. Order follows docs/p0-core-design.md.
var dropAt = map[string]float64{ChMarkPrice: 0.50, ChQuote: 0.75, ChTrade: 0.90}

// Closed describes a finished hour file. Path is relative to Options.Dir.
type Closed struct {
	Path      string
	Group     string
	HourStart time.Time
	Bytes     int64
	MsgCount  int64 // -1 when unknown (recovered files)
}

type Options struct {
	Dir        string // raw root, e.g. /var/lib/liqwatch/raw
	QueueSize  int
	FlushEvery time.Duration // zstd frame close + fsync interval
	MaxDiskPct float64       // stop writing above this; 0 disables
	OnClosed   func(Closed)  // called when an hour file is finished by rotation
	// Instance names the writer slot ("a", "b"; empty for the legacy single process). It is part
	// of every file name, and crash recovery only touches .part files of the same instance.
	Instance string
}

type Stats struct {
	Written    int64
	QueueDepth int
	QueueCap   int
	Dropped    map[string]int64
	DiskStops  int64
}

type item struct {
	group string
	rec   Record
}

type outFile struct {
	f     *os.File
	enc   *zstd.Encoder
	key   string
	hour  time.Time
	final string
	count int64
}

type Writer struct {
	opts    Options
	q       chan item
	done    chan struct{}
	wseq    uint64
	written atomic.Int64
	stops   atomic.Int64
	dropCnt map[string]*atomic.Int64
	marked  map[string]int64
}

func NewWriter(opts Options) (*Writer, error) {
	if opts.QueueSize <= 0 {
		opts.QueueSize = 200000
	}
	if opts.FlushEvery <= 0 {
		opts.FlushEvery = 5 * time.Second
	}
	if !validInstance(opts.Instance) {
		return nil, fmt.Errorf("rawlog: invalid instance %q", opts.Instance)
	}
	if err := os.MkdirAll(opts.Dir, 0o750); err != nil {
		return nil, err
	}
	w := &Writer{
		opts: opts, q: make(chan item, opts.QueueSize), done: make(chan struct{}),
		dropCnt: map[string]*atomic.Int64{}, marked: map[string]int64{},
	}
	for ch := range dropAt {
		w.dropCnt[ch] = new(atomic.Int64)
	}
	if err := recoverParts(opts.Dir, opts.Instance); err != nil {
		return nil, err
	}
	return w, nil
}

// Write implements Sink. Droppable channels never block; all others wait for queue space.
func (w *Writer) Write(group string, r Record) {
	if thr, ok := dropAt[r.Channel]; ok {
		if float64(len(w.q)) >= thr*float64(cap(w.q)) {
			w.dropCnt[r.Channel].Add(1)
			return
		}
		select {
		case w.q <- item{group, r}:
		default:
			w.dropCnt[r.Channel].Add(1)
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
	for ch, c := range w.dropCnt {
		s.Dropped[ch] = c.Load()
	}
	return s
}

// Run drains the queue until ctx is cancelled, then flushes and closes all files.
// Stop producers before cancelling ctx so nothing is lost.
func (w *Writer) Run(ctx context.Context) {
	defer close(w.done)
	files := map[string]*outFile{}
	var buf bytes.Buffer
	flush := time.NewTicker(w.opts.FlushEvery)
	defer flush.Stop()
	var diskStop bool
	var lastDisk time.Time
	lastMark := time.Now()

	closeFile := func(g string, rotated bool) {
		o := files[g]
		if o == nil {
			return
		}
		delete(files, g)
		if err := o.enc.Close(); err != nil {
			log.Printf("rawlog: zstd close %s: %v", o.key, err)
		}
		_ = o.f.Sync()
		st, _ := o.f.Stat()
		_ = o.f.Close()
		if err := os.Rename(o.final+".part", o.final); err != nil {
			log.Printf("rawlog: rename %s: %v", o.final, err)
			return
		}
		if rotated && w.opts.OnClosed != nil {
			var size int64
			if st != nil {
				size = st.Size()
			}
			rel, _ := filepath.Rel(w.opts.Dir, o.final)
			w.opts.OnClosed(Closed{Path: rel, Group: g, HourStart: o.hour, Bytes: size, MsgCount: o.count})
		}
	}
	closeAll := func() {
		for g := range files {
			closeFile(g, false)
		}
	}
	syncAll := func() {
		for _, o := range files {
			_ = o.enc.Flush()
			_ = o.f.Sync()
		}
	}
	emit := func(group string, r Record) {
		hour := time.Unix(0, r.RecvWal).UTC().Truncate(time.Hour)
		o := files[group]
		if o != nil && !o.hour.Equal(hour) {
			closeFile(group, true)
			o = nil
		}
		if o == nil {
			var err error
			if o, err = w.open(group, hour); err != nil {
				log.Printf("rawlog: open %s: %v", group, err)
				return
			}
			files[group] = o
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
		buf.WriteString(`,"p":`)
		buf.Write(r.Payload)
		buf.WriteString("}\n")
		n, err := o.enc.Write(buf.Bytes())
		if err != nil {
			log.Printf("rawlog: write %s: %v", o.key, err)
		}
		o.count++
		w.written.Add(int64(n))
	}
	handle := func(it item) {
		if time.Since(lastDisk) > 10*time.Second {
			lastDisk = time.Now()
			if w.opts.MaxDiskPct > 0 {
				pct := diskPct(w.opts.Dir)
				if pct >= w.opts.MaxDiskPct && !diskStop {
					log.Printf("rawlog: disk at %.1f%%, writing stopped", pct)
				}
				if pct < w.opts.MaxDiskPct && diskStop {
					log.Printf("rawlog: disk at %.1f%%, writing resumed", pct)
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
		for ch, c := range w.dropCnt {
			total := c.Load()
			if d := total - w.marked[ch]; d > 0 {
				w.marked[ch] = total
				p := fmt.Sprintf(`{"event":"drop","channel":%q,"count":%d,"since_ns":%d}`, ch, d, lastMark.UnixNano())
				emit(GroupGap, Record{Conn: "writer", CSeq: 0, RecvWal: now.UnixNano(), RecvMon: monoNow(), Channel: "gap", Payload: []byte(p)})
			}
		}
		lastMark = now
	}

	for {
		select {
		case it := <-w.q:
			handle(it)
		case <-flush.C:
			markDrops()
			syncAll()
			// rotate idle groups whose hour has ended
			cur := time.Now().UTC().Truncate(time.Hour)
			for g, o := range files {
				if o.hour.Before(cur) {
					closeFile(g, true)
				}
			}
		case <-ctx.Done():
			for {
				select {
				case it := <-w.q:
					handle(it)
				default:
					markDrops()
					closeAll()
					return
				}
			}
		}
	}
}

var processStart = time.Now()

// Mono is the process-wide monotonic clock (ns) used for every record's rm field.
func Mono() int64 { return int64(time.Since(processStart)) }

func monoNow() int64 { return Mono() }

func (w *Writer) open(group string, hour time.Time) (*outFile, error) {
	key := group + "-" + hour.Format("2006010215")
	dir := filepath.Join(w.opts.Dir, hour.Format("20060102"))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	// A finished file is never reopened: it may already be uploaded. A restart inside the hour
	// writes a new segment (<key>.r1, .r2, ...).
	stem := filepath.Join(dir, key)
	if w.opts.Instance != "" {
		stem += "." + w.opts.Instance
	}
	final := uniqueFinal(stem)
	f, err := os.OpenFile(final+".part", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return nil, err
	}
	enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		f.Close()
		return nil, err
	}
	return &outFile{f: f, enc: enc, key: key, hour: hour, final: final}, nil
}

// validInstance accepts "" or 1-8 lowercase letters.
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

// partOwner returns the instance and the stem (path without .rN and .jsonl.zst.part) of a
// part file named <group>-<YYYYMMDDHH>[.<instance>][.rN].jsonl.zst.part.
func partOwner(p string) (instance, stem string) {
	stem = strings.TrimSuffix(p, ".jsonl.zst.part")
	dash := strings.LastIndexByte(stem, '-')
	if dash < 0 {
		return "", stem
	}
	segs := strings.Split(stem[dash+1:], ".")
	keep := segs[:1]
	for _, sg := range segs[1:] {
		if isSegment(sg) {
			continue
		}
		instance = sg
		keep = append(keep, sg)
	}
	return instance, stem[:dash+1] + strings.Join(keep, ".")
}

func isSegment(sg string) bool {
	if len(sg) < 2 || sg[0] != 'r' {
		return false
	}
	for _, r := range sg[1:] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// recoverParts renames leftover .part files (crash) of this instance to their final names. A
// torn last frame is possible; readers must tolerate a truncated tail. Parts of other instances
// belong to a process that may still be running and are left alone.
func recoverParts(dir, instance string) error {
	parts, err := filepath.Glob(filepath.Join(dir, "*", "*.jsonl.zst.part"))
	if err != nil {
		return err
	}
	for _, p := range parts {
		owner, stem := partOwner(p)
		if owner != instance {
			continue
		}
		dst := stem + ".jsonl.zst"
		for n := 1; ; n++ {
			if _, err := os.Stat(dst); err != nil {
				break
			}
			dst = fmt.Sprintf("%s.r%d.jsonl.zst", stem, n)
		}
		if err := os.Rename(p, dst); err != nil {
			return err
		}
		log.Printf("rawlog: recovered %s as %s", p, dst)
	}
	return nil
}

// uniqueFinal returns <stem>.jsonl.zst, or <stem>.rN.jsonl.zst for the first N where neither
// the final file nor its .part exists.
func uniqueFinal(stem string) string {
	for n := 0; ; n++ {
		name := stem + ".jsonl.zst"
		if n > 0 {
			name = fmt.Sprintf("%s.r%d.jsonl.zst", stem, n)
		}
		_, e1 := os.Stat(name)
		_, e2 := os.Stat(name + ".part")
		if e1 != nil && e2 != nil {
			return name
		}
	}
}

// Sweep lists finished hour files strictly older than the hour containing now.
// The collector registers any that are missing from the manifest (MsgCount is -1).
func Sweep(dir string, now time.Time) []Closed {
	var out []Closed
	files, _ := filepath.Glob(filepath.Join(dir, "*", "*.jsonl.zst"))
	cur := now.UTC().Truncate(time.Hour)
	for _, p := range files {
		base := strings.TrimSuffix(filepath.Base(p), ".jsonl.zst")
		i := strings.LastIndexByte(base, '-')
		if i < 0 {
			continue
		}
		hs := base[i+1:]
		if d := strings.IndexByte(hs, '.'); d >= 0 {
			hs = hs[:d]
		}
		hour, err := time.ParseInLocation("2006010215", hs, time.UTC)
		if err != nil || !hour.Before(cur) {
			continue
		}
		st, err := os.Stat(p)
		if err != nil {
			continue
		}
		rel, _ := filepath.Rel(dir, p)
		out = append(out, Closed{Path: rel, Group: base[:i], HourStart: hour, Bytes: st.Size(), MsgCount: -1})
	}
	return out
}

func diskPct(path string) float64 {
	var s syscall.Statfs_t
	if err := syscall.Statfs(path, &s); err != nil {
		return 0
	}
	total := float64(s.Blocks) * float64(s.Bsize)
	if total == 0 {
		return 0
	}
	return (total - float64(s.Bavail)*float64(s.Bsize)) / total * 100
}

var _ Sink = (*Writer)(nil)
