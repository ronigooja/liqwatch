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

	"github.com/wicker/wicker/internal/collector"
	"github.com/wicker/wicker/internal/config"
	"github.com/wicker/wicker/internal/manifest"
	"github.com/wicker/wicker/internal/rawlog"
	"github.com/wicker/wicker/internal/snapshot"
	"github.com/wicker/wicker/internal/symbols"
)

func main() {
	cfgPath := flag.String("config", "/etc/wicker/config.toml", "config file")
	flag.Parse()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if err := run(cfg); err != nil {
		log.Fatalf("collector: %v", err)
	}
}

func run(cfg *config.Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rawDir := filepath.Join(cfg.DataDir, "raw")
	store, err := openManifest(cfg)
	if err != nil {
		return err
	}
	defer store.Close()

	var regMu sync.Mutex
	register := func(c rawlog.Closed) {
		regMu.Lock()
		defer regMu.Unlock()
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
		Dir: rawDir, QueueSize: cfg.Collect.QueueSize, MaxDiskPct: 92,
		OnClosed: func(c rawlog.Closed) { go register(c) },
	})
	if err != nil {
		return err
	}
	// Files closed before this start (crash recovery, earlier runs) that the manifest has not seen.
	for _, c := range rawlog.Sweep(rawDir, time.Now()) {
		register(c)
	}

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
	col := collector.New(w, shards)
	snap := snapshot.New(cfg.Snapshot, depth, w, uni.WeightPerMinLimit, &http.Client{Timeout: 15 * time.Second})

	var writerDone sync.WaitGroup
	writerCtx, writerStop := context.WithCancel(context.Background())
	writerDone.Add(1)
	go func() { defer writerDone.Done(); w.Run(writerCtx) }()

	var prod sync.WaitGroup
	prod.Add(2)
	go func() { defer prod.Done(); col.Run(ctx) }()
	go func() { defer prod.Done(); snap.Run(ctx) }()

	go serveMetrics(cfg.Collect.MetricsAddr, w, col, snap)
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

func serveMetrics(addr string, w *rawlog.Writer, col *collector.Collector, snap *snapshot.Scheduler) {
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
	})
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("metrics: %v", err)
	}
}
