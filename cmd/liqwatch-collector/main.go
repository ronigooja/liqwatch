// Command liqwatch-collector records trades, mark prices and liquidations.
// Design: docs/p0-core-design.md, scope: docs/p0-rollout.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ronigooja/liqwatch/internal/backfill"
	"github.com/ronigooja/liqwatch/internal/collector"
	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/binance"
	"github.com/ronigooja/liqwatch/internal/venuemap"
)

func main() {
	cfgPath := flag.String("config", "/etc/liqwatch/config.toml", "config file")
	instance := flag.String("instance", "", "writer slot name (a or b) for handoff; empty is the legacy single process")
	venue := flag.String("venue", "", "override collect.venue")
	metricsAddr := flag.String("metrics-addr", "", "override collect.metrics_addr")
	flag.Parse()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if *venue != "" {
		cfg.Collect.Venue = *venue
	}
	if *metricsAddr != "" {
		cfg.Collect.MetricsAddr = *metricsAddr
	}
	if err := run(cfg, *instance); err != nil {
		log.Fatalf("collector: %v", err)
	}
}

func run(cfg *config.Config, instance string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	name := cfg.Collect.Venue
	if name == "" {
		name = "binance"
	}
	adapter := venue.Get(name)
	if adapter == nil {
		return fmt.Errorf("unknown venue %q", name)
	}
	if a, ok := adapter.(*binance.Adapter); ok {
		a.StateDir = filepath.Join(cfg.DataDir, "state")
		a.MaxStreamsPerWS = cfg.Collect.MaxStreamsPerWS
	}
	rawDir := filepath.Join(cfg.DataDir, "raw")
	if instance != "" {
		unlock, err := lockInstance(cfg.DataDir, instance)
		if err != nil {
			return err
		}
		defer unlock()
	}
	w, err := rawlog.NewWriter(rawlog.Options{
		Dir: rawDir, QueueSize: cfg.Collect.QueueSize, MaxDiskPct: 92, Instance: instance, DropClass: adapter.Classes(),
		Compress: cfg.Record.Compress, Level: cfg.Record.Level,
	})
	if err != nil {
		return err
	}
	ready := &readiness{}
	syms, err := adapter.Symbols(ctx)
	if err != nil {
		return fmt.Errorf("symbols: %w", err)
	}
	if name != "binance" {
		m, err := venuemap.Load(cfg.Collect.MapPath)
		if err != nil {
			return fmt.Errorf("venue map %q: %w", cfg.Collect.MapPath, err)
		}
		if len(m.Symbols(name)) == 0 {
			return fmt.Errorf("venue map %q has no usable symbols for %s", cfg.Collect.MapPath, name)
		}
		syms = venue.Filter(syms, m, name)
		if len(syms) == 0 {
			return fmt.Errorf("venue map %q has no usable discovered symbols for %s", cfg.Collect.MapPath, name)
		}
	}
	shards, err := adapter.Shards(ctx, syms)
	if err != nil {
		return fmt.Errorf("shards: %w", err)
	}
	log.Printf("universe=%d", len(syms))
	var sink rawlog.Sink = w
	var bf *backfill.Backfiller
	var tap *backfill.Tap
	if a, ok := adapter.(*binance.Adapter); ok {
		log.Printf("weight_limit=%d", a.WeightPerMinLimit)
		tap = backfill.NewTap(w)
		sink = tap
		bf = backfill.New(tap, tap, &http.Client{Timeout: 20 * time.Second}, a.WeightPerMinLimit, 0.10)
	}
	col := collector.New(sink, adapter, shards)
	enabled := true
	overrides := map[string]time.Duration{}
	for ch, v := range cfg.Collect.Throttle {
		if ch == "enabled" {
			enabled = v.(bool)
		} else {
			overrides[ch], _ = time.ParseDuration(v.(string))
		}
	}
	col.ConfigureThrottle(enabled, overrides)
	col.OpenSpacing = time.Duration(cfg.Collect.OpenSpacingMS) * time.Millisecond
	if bf != nil {
		shardSyms := map[string][]string{}
		for _, sh := range shards {
			if !strings.HasPrefix(sh.Name, "trade") {
				continue
			}
			for _, st := range sh.Streams {
				shardSyms[sh.Name] = append(shardSyms[sh.Name], strings.ToUpper(strings.TrimSuffix(st, "@aggTrade")))
			}
		}
		col.OnDisconnect = func(sh collector.Shard) uint64 {
			if _, ok := shardSyms[sh.Name]; !ok {
				return 0
			}
			return tap.Mark()
		}
		col.OnReconnectToken = func(sh collector.Shard, marker uint64) {
			if syms, ok := shardSyms[sh.Name]; ok && marker != 0 {
				log.Printf("trade shard %s reconnected, backfilling %d symbols", sh.Name, len(syms))
				bf.RequestShardAfter(syms, marker)
			}
		}
	}
	col.OnOpen = ready.shardOpened
	ready.expectShards(len(shards))
	col.OnClose = ready.shardClosed

	var writerDone sync.WaitGroup
	writerCtx, writerStop := context.WithCancel(context.Background())
	writerDone.Add(1)
	go func() { defer writerDone.Done(); w.Run(writerCtx) }()

	var prod sync.WaitGroup
	prod.Add(1)
	go func() { defer prod.Done(); col.Run(ctx) }()
	if bf != nil {
		prod.Add(1)
		go func() { defer prod.Done(); bf.Run(ctx) }()
	}

	go serveMetrics(cfg.Collect.MetricsAddr, w, col, ready)
	go servePprof(cfg.Collect.PprofAddr)

	<-ctx.Done()
	log.Printf("shutting down")
	prod.Wait()
	writerStop()
	writerDone.Wait()
	return nil
}

func servePprof(addr string) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("pprof: %v", err)
	}
}

func serveMetrics(addr string, w *rawlog.Writer, col *collector.Collector, ready *readiness) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(rw http.ResponseWriter, _ *http.Request) {
		s := w.Stats()
		fmt.Fprintf(rw, "liqwatch_queue_depth %d\nliqwatch_queue_depth_ratio %f\nliqwatch_written_bytes_total %d\n",
			s.QueueDepth, float64(s.QueueDepth)/float64(max(s.QueueCap, 1)), s.Written)
		for ch, n := range s.Dropped {
			fmt.Fprintf(rw, "liqwatch_dropped_total{channel=%q} %d\n", ch, n)
		}
		for ch, n := range col.Thinned() {
			fmt.Fprintf(rw, "liqwatch_thinned_total{channel=%q} %d\n", ch, n)
		}
		fmt.Fprintf(rw, "liqwatch_disk_stop_total %d\nliqwatch_reconnects_total %d\nliqwatch_stalls_total %d\nliqwatch_messages_total %d\n",
			s.DiskStops, col.Reconnects.Load(), col.Stalls.Load(), col.Messages.Load())
		fmt.Fprintf(rw, "liqwatch_shards_open %d\nliqwatch_shards_expected %d\nliqwatch_start_unix %d\n", ready.open(), ready.expected(), startUnix)
	})
	mux.HandleFunc("/ready", func(rw http.ResponseWriter, _ *http.Request) {
		if ready.open() < ready.expected() || ready.expected() == 0 {
			http.Error(rw, "not ready", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(rw, "ready")
	})
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("metrics: %v", err)
	}
}

var startUnix = time.Now().Unix()

// readiness tracks how many shards have an open connection, for the handoff script.
type readiness struct {
	mu   sync.Mutex
	want int
	seen map[string]int
}

func (r *readiness) expectShards(n int) { r.mu.Lock(); r.want = n; r.mu.Unlock() }
func (r *readiness) shardOpened(sh collector.Shard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seen == nil {
		r.seen = map[string]int{}
	}
	r.seen[sh.Name]++
}
func (r *readiness) shardClosed(sh collector.Shard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seen[sh.Name] > 1 {
		r.seen[sh.Name]--
	} else {
		delete(r.seen, sh.Name)
	}
}
func (r *readiness) open() int     { r.mu.Lock(); defer r.mu.Unlock(); return len(r.seen) }
func (r *readiness) expected() int { r.mu.Lock(); defer r.mu.Unlock(); return r.want }

// lockInstance takes an exclusive flock on <data_dir>/state/instance-<name>.lock for the life of
// the process, so two processes can never run as the same slot.
func lockInstance(dataDir, name string) (func(), error) {
	dir := filepath.Join(dataDir, "state")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "instance-"+name+".lock"), os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("instance %q is already running: %w", name, err)
	}
	return func() { syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
}
