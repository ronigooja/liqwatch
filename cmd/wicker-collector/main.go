// Command wicker-collector is the production recorder: tier A streams, depth, and snapshots to the raw log.
// Design: docs/p0-core-design.md, scope: docs/p0-rollout.md.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/wicker/wicker/internal/backfill"
	"github.com/wicker/wicker/internal/collector"
	"github.com/wicker/wicker/internal/config"
	"github.com/wicker/wicker/internal/manifest"
	"github.com/wicker/wicker/internal/rawlog"
	"github.com/wicker/wicker/internal/snapshot"
	"github.com/wicker/wicker/internal/symbols"
)

func main() {
	cfgPath := flag.String("config", "/etc/wicker/config.toml", "config file")
	instance := flag.String("instance", "", "writer slot name (a or b) for handoff; empty is the legacy single process")
	metricsAddr := flag.String("metrics-addr", "", "override collect.metrics_addr")
	flag.Parse()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
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

	rawDir := filepath.Join(cfg.DataDir, "raw")
	if instance != "" {
		unlock, err := lockInstance(cfg.DataDir, instance)
		if err != nil {
			return err
		}
		defer unlock()
	}
	store, err := openManifest(cfg)
	if err != nil {
		return err
	}
	defer store.Close()

	var regMu sync.Mutex
	register := func(c rawlog.Closed) {
		regMu.Lock()
		defer regMu.Unlock()
		if ok, err := store.Known(filepath.ToSlash(c.Path)); err == nil && ok {
			return
		}
		sum, err := fileSHA(filepath.Join(rawDir, c.Path))
		if err != nil {
			log.Printf("manifest: hash %s: %v", c.Path, err)
			return
		}
		if err := store.RegisterClosed(manifest.File{
			Path: filepath.ToSlash(c.Path), Group: c.Group, HourStart: c.HourStart,
			MsgCount: c.MsgCount, Bytes: c.Bytes, SHA256: sum, ClosedAt: time.Now().UTC(), State: manifest.StateClosed,
		}); err != nil {
			log.Printf("manifest: register %s: %v", c.Path, err)
		}
	}

	w, err := rawlog.NewWriter(rawlog.Options{
		Dir: rawDir, QueueSize: cfg.Collect.QueueSize, MaxDiskPct: 92, Instance: instance,
		OnClosed: func(c rawlog.Closed) { go register(c) },
	})
	if err != nil {
		return err
	}
	// Files closed before this start (crash recovery, earlier runs) that the manifest has not seen.
	for _, c := range rawlog.Sweep(rawDir, time.Now()) {
		register(c)
	}

	ready := &readiness{}
	uni, err := symbols.Load(ctx, filepath.Join(cfg.DataDir, "state"))
	if err != nil {
		return fmt.Errorf("symbols: %w", err)
	}
	quotes, err := symbols.TopByVolume(ctx, uni.All, cfg.Collect.QuoteTopN)
	if err != nil {
		return fmt.Errorf("top by volume: %w", err)
	}
	depth := validDepth(cfg.Collect.DepthSymbols, uni.All)
	log.Printf("universe=%d quotes=%d depth=%v weight_limit=%d", len(uni.All), len(quotes), depth, uni.WeightPerMinLimit)

	shards := collector.BuildShards(uni.All, quotes, depth, cfg.Collect.DepthSpeed, cfg.Collect.MaxStreamsPerWS)
	tap := backfill.NewTap(w)
	col := collector.New(tap, shards)
	col.OpenSpacing = time.Duration(cfg.Collect.OpenSpacingMS) * time.Millisecond
	snap := snapshot.New(cfg.Snapshot, depth, w, uni.WeightPerMinLimit, &http.Client{Timeout: 15 * time.Second})
	bf := backfill.New(tap, tap, &http.Client{Timeout: 20 * time.Second}, uni.WeightPerMinLimit, 0.10)
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
	col.OnOpen = ready.shardOpened
	ready.expectShards(len(shards))
	col.OnReconnect = func(sh collector.Shard) {
		if sh.Name != "depth" {
			return
		}
		log.Printf("depth shard reconnected, requesting snapshots for %d symbols", len(depth))
		for _, s := range depth {
			snap.Trigger(s)
		}
	}

	var writerDone sync.WaitGroup
	writerCtx, writerStop := context.WithCancel(context.Background())
	writerDone.Add(1)
	go func() { defer writerDone.Done(); w.Run(writerCtx) }()

	var prod sync.WaitGroup
	prod.Add(3)
	go func() { defer prod.Done(); col.Run(ctx) }()
	go func() { defer prod.Done(); snap.Run(ctx) }()
	go func() { defer prod.Done(); bf.Run(ctx) }()

	go serveMetrics(cfg.Collect.MetricsAddr, w, col, snap, ready, len(depth))
	go sweepLoop(ctx, rawDir, register)

	<-ctx.Done()
	log.Printf("shutting down")
	prod.Wait()
	writerStop()
	writerDone.Wait()
	return nil
}

func openManifest(cfg *config.Config) (*manifest.SQLiteStore, error) {
	dir := filepath.Join(cfg.DataDir, "manifest")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	return manifest.Open(filepath.Join(dir, "manifest.db"))
}

// validDepth keeps configured depth symbols that exist in the universe and warns about the rest.
func validDepth(want, all []string) []string {
	ok := map[string]bool{}
	for _, s := range all {
		ok[s] = true
	}
	var out []string
	for _, s := range want {
		s = strings.ToUpper(strings.TrimSpace(s))
		if ok[s] {
			out = append(out, s)
		} else {
			log.Printf("depth symbol %q not in universe, skipped", s)
		}
	}
	return out
}

func fileSHA(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// sweepLoop registers hour files that closed without a rotation callback (idle groups, recovery).
func sweepLoop(ctx context.Context, rawDir string, register func(rawlog.Closed)) {
	t := time.NewTicker(2 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, c := range rawlog.Sweep(rawDir, time.Now()) {
				register(c)
			}
		}
	}
}

func serveMetrics(addr string, w *rawlog.Writer, col *collector.Collector, snap *snapshot.Scheduler, ready *readiness, depthSyms int) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(rw http.ResponseWriter, _ *http.Request) {
		s := w.Stats()
		ss := snap.Stats()
		fmt.Fprintf(rw, "wicker_queue_depth %d\nwicker_queue_depth_ratio %f\nwicker_written_bytes_total %d\n",
			s.QueueDepth, float64(s.QueueDepth)/float64(max(s.QueueCap, 1)), s.Written)
		for ch, n := range s.Dropped {
			fmt.Fprintf(rw, "wicker_dropped_total{channel=%q} %d\n", ch, n)
		}
		fmt.Fprintf(rw, "wicker_disk_stop_total %d\nwicker_reconnects_total %d\nwicker_stalls_total %d\nwicker_messages_total %d\n",
			s.DiskStops, col.Reconnects.Load(), col.Stalls.Load(), col.Messages.Load())
		fmt.Fprintf(rw, "wicker_snapshot_requests_total %d\nwicker_rest_weight_used %d\nwicker_rest_rate_limited_total %d\n",
			ss.Requests, ss.WeightUsed, ss.RateLimited)
		fmt.Fprintf(rw, "wicker_shards_open %d\nwicker_shards_expected %d\nwicker_snapshots_written %d\nwicker_snapshots_expected %d\nwicker_start_unix %d\n",
			ready.open(), ready.expected(), ss.Written, depthSyms, startUnix)
	})
	mux.HandleFunc("/ready", func(rw http.ResponseWriter, _ *http.Request) {
		if ready.open() < ready.expected() || ready.expected() == 0 || snap.Stats().Written < uint64(depthSyms) {
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
	seen map[string]bool
}

func (r *readiness) expectShards(n int) { r.mu.Lock(); r.want = n; r.mu.Unlock() }
func (r *readiness) shardOpened(sh collector.Shard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seen == nil {
		r.seen = map[string]bool{}
	}
	r.seen[sh.Name] = true
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
