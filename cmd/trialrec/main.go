// Command trialrec is a minimal trial recorder used to measure load on the
// recorder host. It is not the production collector. See docs/trial-plan.md.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/klauspost/compress/zstd"
)

const (
	restBase   = "https://fapi.binance.com"
	wsPublic   = "wss://fstream.binance.com/public/stream?streams="
	wsMarket   = "wss://fstream.binance.com/market/stream?streams="
	maxStreams = 100
)

type config struct {
	out         string
	depthSyms   string
	depthSpeed  string
	bookTicker  string
	aggTrade    string
	markPrice   bool
	forceOrder  bool
	duration    time.Duration
	maxDiskPct  float64
	statsEvery  time.Duration
	snapshotGap time.Duration
}

type record struct {
	conn  string
	cseq  uint64
	rw    int64
	rm    int64
	ch    string
	sym   string
	bytes []byte
}

type chanStat struct {
	msgs     atomic.Int64
	bytes    atomic.Int64
	latMu    sync.Mutex
	lat      []int64
	peakSec  atomic.Int64
	curSec   atomic.Int64
	curCount atomic.Int64
}

type stats struct {
	mu       sync.Mutex
	ch       map[string]*chanStat
	drops    atomic.Int64
	reconns  atomic.Int64
	written  atomic.Int64
	compress atomic.Int64
	queueMax atomic.Int64
	restW    atomic.Int64
}

func (s *stats) get(name string) *chanStat {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.ch[name]
	if !ok {
		c = &chanStat{}
		s.ch[name] = c
	}
	return c
}

var (
	st     = &stats{ch: map[string]*chanStat{}}
	t0     = time.Now()
	seqCtr atomic.Uint64
)

func main() {
	var cfg config
	var dur string
	flag.StringVar(&cfg.out, "out", "/var/lib/wicker/trial", "output directory")
	flag.StringVar(&cfg.depthSyms, "depth", "", "comma-separated symbols for diff depth (e.g. BTCUSDT,ETHUSDT)")
	flag.StringVar(&cfg.depthSpeed, "depth-speed", "100ms", "depth update speed: 100ms, 250ms, 500ms")
	flag.StringVar(&cfg.bookTicker, "bookticker", "", "bookTicker symbols: comma list, 'all', or 'top:N'")
	flag.StringVar(&cfg.aggTrade, "aggtrade", "all", "aggTrade symbols: comma list, 'all', or 'top:N', or 'none'")
	flag.BoolVar(&cfg.markPrice, "markprice", true, "subscribe !markPrice@arr@1s")
	flag.BoolVar(&cfg.forceOrder, "forceorder", true, "subscribe !forceOrder@arr")
	flag.StringVar(&dur, "duration", "10m", "run duration")
	flag.Float64Var(&cfg.maxDiskPct, "max-disk-pct", 80, "stop writing at this disk use percent")
	flag.DurationVar(&cfg.statsEvery, "stats-every", 60*time.Second, "stats interval")
	flag.DurationVar(&cfg.snapshotGap, "snapshot-gap", 10*time.Minute, "min gap between depth snapshots per symbol")
	flag.Parse()

	d, err := time.ParseDuration(dur)
	if err != nil {
		log.Fatalf("bad duration: %v", err)
	}
	cfg.duration = d
	if err := os.MkdirAll(cfg.out, 0o750); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ctx, cancel2 := context.WithTimeout(ctx, cfg.duration)
	defer cancel2()

	info, err := fetchExchangeInfo(ctx, cfg.out)
	if err != nil {
		log.Fatalf("exchangeInfo: %v", err)
	}
	log.Printf("usdt perpetual trading symbols: %d", len(info.symbols))

	aggSyms := resolveSymbols(cfg.aggTrade, info)
	bookSyms := resolveSymbols(cfg.bookTicker, info)
	var depthSyms []string
	if cfg.depthSyms != "" {
		for _, s := range strings.Split(cfg.depthSyms, ",") {
			depthSyms = append(depthSyms, strings.ToUpper(strings.TrimSpace(s)))
		}
	}
	log.Printf("aggTrade=%d bookTicker=%d depth=%d markPrice=%v forceOrder=%v",
		len(aggSyms), len(bookSyms), len(depthSyms), cfg.markPrice, cfg.forceOrder)

	queue := make(chan record, 200000)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		writer(ctx, cfg, queue)
	}()

	var cw sync.WaitGroup
	// public endpoint: depth and bookTicker; market endpoint: aggTrade, markPrice, forceOrder.
	var pubStreams, mktStreams []string
	for _, s := range depthSyms {
		pubStreams = append(pubStreams, strings.ToLower(s)+"@depth@"+cfg.depthSpeed)
	}
	for _, s := range bookSyms {
		pubStreams = append(pubStreams, strings.ToLower(s)+"@bookTicker")
	}
	for _, s := range aggSyms {
		mktStreams = append(mktStreams, strings.ToLower(s)+"@aggTrade")
	}
	if cfg.markPrice {
		mktStreams = append(mktStreams, "!markPrice@arr@1s")
	}
	if cfg.forceOrder {
		mktStreams = append(mktStreams, "!forceOrder@arr")
	}

	startShards := func(prefix, base string, streams []string) {
		for i := 0; i*maxStreams < len(streams); i++ {
			lo := i * maxStreams
			hi := lo + maxStreams
			if hi > len(streams) {
				hi = len(streams)
			}
			name := fmt.Sprintf("%s%02d", prefix, i)
			url := base + strings.Join(streams[lo:hi], "/")
			cw.Add(1)
			go func() {
				defer cw.Done()
				collect(ctx, name, url, queue)
			}()
			time.Sleep(300 * time.Millisecond) // open connections gradually
		}
	}
	startShards("pub", wsPublic, pubStreams)
	startShards("mkt", wsMarket, mktStreams)

	if len(depthSyms) > 0 {
		cw.Add(1)
		go func() {
			defer cw.Done()
			snapshots(ctx, depthSyms, cfg.snapshotGap, queue)
		}()
	}

	go statsLoop(ctx, cfg)

	<-ctx.Done()
	cw.Wait()
	close(queue)
	wg.Wait()
	writeStats(cfg, true)
	log.Printf("done")
}

type exInfo struct {
	symbols []string
}

func fetchExchangeInfo(ctx context.Context, out string) (*exInfo, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", restBase+"/fapi/v1/exchangeInfo", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	_ = os.WriteFile(filepath.Join(out, "exchangeInfo.json"), body, 0o640)
	var v struct {
		RateLimits []map[string]any `json:"rateLimits"`
		Symbols    []struct {
			Symbol         string `json:"symbol"`
			Status         string `json:"status"`
			ContractType   string `json:"contractType"`
			QuoteAsset     string `json:"quoteAsset"`
			UnderlyingType string `json:"underlyingType"`
		} `json:"symbols"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return nil, err
	}
	log.Printf("rateLimits: %v", v.RateLimits)
	types := map[string]int{}
	under := map[string]int{}
	info := &exInfo{}
	for _, s := range v.Symbols {
		types[s.ContractType]++
		under[s.UnderlyingType]++
		if s.ContractType == "PERPETUAL" && s.Status == "TRADING" && s.QuoteAsset == "USDT" {
			info.symbols = append(info.symbols, s.Symbol)
		}
	}
	log.Printf("contractType counts: %v underlyingType counts: %v", types, under)
	sort.Strings(info.symbols)
	return info, nil
}

func resolveSymbols(spec string, info *exInfo) []string {
	switch {
	case spec == "" || spec == "none":
		return nil
	case spec == "all":
		return info.symbols
	case strings.HasPrefix(spec, "top:"):
		n, _ := strconv.Atoi(strings.TrimPrefix(spec, "top:"))
		return topByVolume(info, n)
	}
	var out []string
	for _, s := range strings.Split(spec, ",") {
		out = append(out, strings.ToUpper(strings.TrimSpace(s)))
	}
	return out
}

func topByVolume(info *exInfo, n int) []string {
	resp, err := http.Get(restBase + "/fapi/v1/ticker/24hr")
	if err != nil {
		log.Printf("ticker/24hr: %v; falling back to alphabetical", err)
		return head(info.symbols, n)
	}
	defer resp.Body.Close()
	var t []struct {
		Symbol      string `json:"symbol"`
		QuoteVolume string `json:"quoteVolume"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&t); err != nil {
		return head(info.symbols, n)
	}
	allowed := map[string]bool{}
	for _, s := range info.symbols {
		allowed[s] = true
	}
	type kv struct {
		s string
		v float64
	}
	var l []kv
	for _, x := range t {
		if allowed[x.Symbol] {
			f, _ := strconv.ParseFloat(x.QuoteVolume, 64)
			l = append(l, kv{x.Symbol, f})
		}
	}
	sort.Slice(l, func(i, j int) bool { return l[i].v > l[j].v })
	var out []string
	for i := 0; i < len(l) && i < n; i++ {
		out = append(out, l[i].s)
	}
	return out
}

func head(s []string, n int) []string {
	if n > len(s) {
		n = len(s)
	}
	return s[:n]
}

func collect(ctx context.Context, name, url string, queue chan<- record) {
	backoff := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := readConn(ctx, name, url, queue)
		if ctx.Err() != nil {
			return
		}
		st.reconns.Add(1)
		log.Printf("conn %s closed after %s: %v", name, time.Since(started).Round(time.Second), err)
		queue <- gapRecord(name, err)
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func gapRecord(conn string, err error) record {
	b, _ := json.Marshal(map[string]any{"event": "conn_closed", "err": fmt.Sprint(err)})
	return record{conn: conn, cseq: seqCtr.Add(1), rw: time.Now().UnixNano(), rm: int64(time.Since(t0)), ch: "gap", bytes: b}
}

func readConn(ctx context.Context, name, url string, queue chan<- record) error {
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return err
	}
	defer c.CloseNow()
	c.SetReadLimit(16 << 20)
	log.Printf("conn %s open", name)
	var cseq uint64
	for {
		_, data, err := c.Read(ctx)
		rw := time.Now().UnixNano()
		rm := int64(time.Since(t0))
		if err != nil {
			return err
		}
		cseq++
		ch, sym := classify(data)
		cs := st.get(ch)
		cs.msgs.Add(1)
		cs.bytes.Add(int64(len(data)))
		sec := rw / 1e9
		if cs.curSec.Load() != sec {
			if cs.curCount.Load() > cs.peakSec.Load() {
				cs.peakSec.Store(cs.curCount.Load())
			}
			cs.curSec.Store(sec)
			cs.curCount.Store(0)
		}
		cs.curCount.Add(1)
		if et := eventTime(data); et > 0 {
			cs.latMu.Lock()
			if len(cs.lat) < 200000 {
				cs.lat = append(cs.lat, rw/1e6-et)
			}
			cs.latMu.Unlock()
		}
		rec := record{conn: name, cseq: cseq, rw: rw, rm: rm, ch: ch, sym: sym, bytes: data}
		select {
		case queue <- rec:
			if l := int64(len(queue)); l > st.queueMax.Load() {
				st.queueMax.Store(l)
			}
		default:
			st.drops.Add(1)
		}
	}
}

// classify extracts channel and symbol from a combined-stream message without full parsing.
func classify(data []byte) (ch, sym string) {
	i := bytes.Index(data, []byte(`"stream":"`))
	if i < 0 {
		return "unknown", ""
	}
	rest := data[i+10:]
	j := bytes.IndexByte(rest, '"')
	if j < 0 {
		return "unknown", ""
	}
	name := string(rest[:j])
	if k := strings.IndexByte(name, '@'); k >= 0 {
		sym = strings.ToUpper(name[:k])
		ch = name[k+1:]
		if m := strings.IndexByte(ch, '@'); m >= 0 {
			ch = ch[:m]
		}
		if sym != "" && sym[0] == '!' {
			sym = ""
			ch = strings.TrimPrefix(name[:k], "!") + "_all"
		}
		return ch, sym
	}
	return name, ""
}

// eventTime reads the first "E": field (event time, ms) if present.
func eventTime(data []byte) int64 {
	i := bytes.Index(data, []byte(`"E":`))
	if i < 0 {
		return 0
	}
	rest := data[i+4:]
	n := 0
	for n < len(rest) && rest[n] >= '0' && rest[n] <= '9' {
		n++
	}
	v, _ := strconv.ParseInt(string(rest[:n]), 10, 64)
	return v
}

func snapshots(ctx context.Context, syms []string, gap time.Duration, queue chan<- record) {
	last := map[string]time.Time{}
	client := &http.Client{Timeout: 15 * time.Second}
	for ctx.Err() == nil {
		for _, s := range syms {
			if ctx.Err() != nil {
				return
			}
			if time.Since(last[s]) < gap {
				continue
			}
			req, _ := http.NewRequestWithContext(ctx, "GET", restBase+"/fapi/v1/depth?symbol="+s+"&limit=1000", nil)
			rw0 := time.Now().UnixNano()
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("snapshot %s: %v", s, err)
				time.Sleep(5 * time.Second)
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if w, e := strconv.Atoi(resp.Header.Get("X-MBX-USED-WEIGHT-1M")); e == nil {
				st.restW.Store(int64(w))
			}
			if resp.StatusCode == 429 || resp.StatusCode == 418 {
				log.Printf("snapshot %s: HTTP %d, backing off 60s", s, resp.StatusCode)
				select {
				case <-ctx.Done():
					return
				case <-time.After(60 * time.Second):
				}
				continue
			}
			if resp.StatusCode != 200 {
				log.Printf("snapshot %s: HTTP %d", s, resp.StatusCode)
				continue
			}
			rw1 := time.Now().UnixNano()
			wrapped, _ := json.Marshal(map[string]any{"req_start_ns": rw0, "resp": json.RawMessage(body)})
			select {
			case queue <- record{conn: "rest", cseq: seqCtr.Add(1), rw: rw1, rm: int64(time.Since(t0)), ch: "depthSnapshot", sym: s, bytes: wrapped}:
			default:
				st.drops.Add(1)
			}
			last[s] = time.Now()
			time.Sleep(700 * time.Millisecond) // stagger; weight 20 each
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// writer drains the queue into hourly zstd files named by channel group.
func writer(ctx context.Context, cfg config, queue <-chan record) {
	type out struct {
		f   *os.File
		enc *zstd.Encoder
		cw  *countWriter
		key string
	}
	files := map[string]*out{}
	closeAll := func() {
		for _, o := range files {
			o.enc.Close()
			o.f.Close()
		}
	}
	defer closeAll()
	lastFlush := time.Now()
	diskStop := false
	lastDisk := time.Time{}
	var buf bytes.Buffer
	for rec := range queue {
		if time.Since(lastDisk) > 10*time.Second {
			lastDisk = time.Now()
			if pct := diskPct(cfg.out); pct >= cfg.maxDiskPct && !diskStop {
				diskStop = true
				log.Printf("disk at %.1f%%, writing stopped", pct)
			}
		}
		if diskStop {
			st.drops.Add(1)
			continue
		}
		group := groupOf(rec.ch)
		hour := time.Unix(0, rec.rw).UTC().Format("2006010215")
		key := group + "-" + hour
		o := files[group]
		if o == nil || o.key != key {
			if o != nil {
				o.enc.Close()
				o.f.Close()
			}
			dir := filepath.Join(cfg.out, "raw", time.Unix(0, rec.rw).UTC().Format("20060102"))
			_ = os.MkdirAll(dir, 0o750)
			f, err := os.OpenFile(filepath.Join(dir, key+".jsonl.zst"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
			if err != nil {
				log.Printf("open: %v", err)
				continue
			}
			cw := &countWriter{w: f}
			enc, _ := zstd.NewWriter(cw, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
			o = &out{f: f, enc: enc, cw: cw, key: key}
			files[group] = o
		}
		buf.Reset()
		fmt.Fprintf(&buf, `{"v":1,"conn":%q,"cseq":%d,"rw":%d,"rm":%d,"ch":%q,"sym":%q,"p":`,
			rec.conn, rec.cseq, rec.rw, rec.rm, rec.ch, rec.sym)
		buf.Write(rec.bytes)
		buf.WriteString("}\n")
		n, _ := o.enc.Write(buf.Bytes())
		st.written.Add(int64(n))
		if time.Since(lastFlush) > 5*time.Second {
			for _, x := range files {
				x.enc.Flush()
			}
			lastFlush = time.Now()
		}
	}
	for _, x := range files {
		x.enc.Flush()
	}
}

type countWriter struct {
	w io.Writer
	n atomic.Int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n.Add(int64(n))
	st.compress.Add(int64(n))
	return n, err
}

func groupOf(ch string) string {
	switch {
	case ch == "depth" || ch == "depthSnapshot":
		return "depth"
	case ch == "bookTicker":
		return "quote"
	case ch == "gap":
		return "gap"
	case strings.HasPrefix(ch, "forceOrder"):
		return "liq"
	default:
		return "trade"
	}
}

func diskPct(path string) float64 {
	var s syscall.Statfs_t
	if err := syscall.Statfs(path, &s); err != nil {
		return 0
	}
	total := float64(s.Blocks) * float64(s.Bsize)
	free := float64(s.Bavail) * float64(s.Bsize)
	if total == 0 {
		return 0
	}
	return (total - free) / total * 100
}

func statsLoop(ctx context.Context, cfg config) {
	tk := time.NewTicker(cfg.statsEvery)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			writeStats(cfg, false)
		}
	}
}

func readProcStat() (utime, stime float64, rssKB int64, fds int) {
	b, err := os.ReadFile("/proc/self/stat")
	if err == nil {
		f := strings.Fields(string(b[strings.LastIndexByte(string(b), ')')+2:]))
		if len(f) > 13 {
			u, _ := strconv.ParseFloat(f[11], 64)
			s, _ := strconv.ParseFloat(f[12], 64)
			utime, stime = u/100, s/100
		}
	}
	if b, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(l, "VmRSS:") {
				fmt.Sscanf(strings.TrimSpace(strings.TrimPrefix(l, "VmRSS:")), "%d", &rssKB)
			}
		}
	}
	if d, err := os.ReadDir("/proc/self/fd"); err == nil {
		fds = len(d)
	}
	return
}

func readHost() (load string, memAvailMB int64) {
	if b, err := os.ReadFile("/proc/loadavg"); err == nil {
		load = strings.Join(strings.Fields(string(b))[:3], " ")
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(l, "MemAvailable:") {
				var kb int64
				fmt.Sscanf(strings.TrimSpace(strings.TrimPrefix(l, "MemAvailable:")), "%d", &kb)
				memAvailMB = kb / 1024
			}
		}
	}
	return
}

var lastCPU struct {
	u, s float64
	t    time.Time
}

func writeStats(cfg config, final bool) {
	u, s, rss, fds := readProcStat()
	now := time.Now()
	cpu := 0.0
	if !lastCPU.t.IsZero() {
		cpu = ((u - lastCPU.u) + (s - lastCPU.s)) / now.Sub(lastCPU.t).Seconds() * 100
	}
	lastCPU.u, lastCPU.s, lastCPU.t = u, s, now
	load, avail := readHost()
	chans := map[string]any{}
	st.mu.Lock()
	for name, c := range st.ch {
		c.latMu.Lock()
		lat := append([]int64(nil), c.lat...)
		c.lat = c.lat[:0]
		c.latMu.Unlock()
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		pct := func(p float64) int64 {
			if len(lat) == 0 {
				return 0
			}
			return lat[int(float64(len(lat)-1)*p)]
		}
		chans[name] = map[string]any{
			"msgs": c.msgs.Load(), "bytes": c.bytes.Load(), "peak_per_s": c.peakSec.Load(),
			"lat_ms_p50": pct(0.5), "lat_ms_p99": pct(0.99), "lat_ms_max": pct(1), "lat_n": len(lat),
		}
	}
	st.mu.Unlock()
	rec := map[string]any{
		"ts": now.UTC().Format(time.RFC3339), "uptime_s": int(time.Since(t0).Seconds()), "final": final,
		"cpu_pct_of_core": cpu, "rss_mb": rss / 1024, "fds": fds, "goroutines": goroutines(),
		"queue_max": st.queueMax.Load(), "drops": st.drops.Load(), "reconnects": st.reconns.Load(),
		"raw_bytes_written": st.written.Load(), "compressed_bytes": st.compress.Load(),
		"rest_weight_1m": st.restW.Load(), "load": load, "mem_avail_mb": avail,
		"disk_pct": diskPct(cfg.out), "channels": chans,
	}
	b, _ := json.Marshal(rec)
	f, err := os.OpenFile(filepath.Join(cfg.out, "stats.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err == nil {
		f.Write(append(b, '\n'))
		f.Close()
	}
	log.Printf("cpu=%.0f%% rss=%dMB fds=%d drops=%d reconn=%d queueMax=%d disk=%.0f%% avail=%dMB",
		cpu, rss/1024, fds, st.drops.Load(), st.reconns.Load(), st.queueMax.Load(), diskPct(cfg.out), avail)
}

func goroutines() int { return runtime.NumGoroutine() }
