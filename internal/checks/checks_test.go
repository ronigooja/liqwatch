package checks

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wicker/wicker/internal/config"
	"github.com/wicker/wicker/internal/manifest"
)

func TestUnverifiedHonorsAge(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "manifest"), 0o750); err != nil {
		t.Fatal(err)
	}
	s, err := manifest.Open(filepath.Join(dir, "manifest", "manifest.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now()
	for path, closed := range map[string]time.Time{"a": now.Add(-time.Minute), "b": now.Add(-4 * time.Hour)} {
		if err := s.RegisterClosed(manifest.File{Path: path, Group: "g", HourStart: closed, ClosedAt: closed, State: manifest.StateClosed}); err != nil {
			t.Fatal(err)
		}
	}
	n, ok, err := unverified(dir, 3*time.Hour)
	if err != nil || !ok || n != 1 {
		t.Fatalf("n=%d ok=%v err=%v, want only the 4h-old file", n, ok, err)
	}
	if n, _, _ := unverified(dir, 5*time.Hour); n != 0 {
		t.Fatalf("n=%d, want 0 when nothing is older than 5h", n)
	}
}

func TestAlertStateTransitionsOnce(t *testing.T) {
	dir := t.TempDir()
	s, err := NewAlertState(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.Now = func() time.Time { return time.Unix(1, 0) }
	if err := s.Set("disk.high", true, "warning", "disk.high", "high", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("disk.high", true, "warning", "disk.high", "high", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("disk.high", false, "info", "disk.high", "resolved", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("disk.high", false, "info", "disk.high", "resolved", nil); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, "state", "alerts.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var events []Event
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		var e Event
		if err := json.Unmarshal(scan.Bytes(), &e); err != nil {
			t.Fatal(err)
		}
		events = append(events, e)
	}
	if len(events) != 2 || events[0].State != "firing" || events[1].State != "resolved" {
		t.Fatalf("events = %+v", events)
	}
}

func TestEvaluateUsesAnyReachableMetricsEndpoint(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("wicker_queue_depth_ratio 0.1\nwicker_dropped_total{channel=\"aggTrade\"} 0\n"))
	}))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("wicker_queue_depth_ratio 0.9\nwicker_dropped_total{channel=\"aggTrade\"} 5\n"))
	}))
	defer bad.Close()
	addr := func(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "http://") }

	cfg := &config.Config{DataDir: t.TempDir()}
	cfg.Checks = config.Checks{DiskWarnPct: 100, StaleWriteSec: 120, UnverifiedHours: 3}

	cfg.Collect.MetricsAddr = "127.0.0.1:1" // old slot gone
	cfg.Collect.ExtraMetrics = []string{addr(good)}
	f, err := evaluate(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range f {
		if x.Type == "collector.metrics" {
			t.Fatalf("one reachable slot must be enough: %+v", x)
		}
	}

	cfg.Collect.MetricsAddr = addr(bad)
	cfg.Collect.ExtraMetrics = []string{addr(good)}
	f, _ = evaluate(cfg)
	var queue, drops int
	for _, x := range f {
		switch x.Type {
		case "data.queue":
			queue++
		case "data.drop":
			drops++
		}
	}
	if queue != 1 || drops != 1 {
		t.Fatalf("each reachable slot is judged on its own: queue=%d drops=%d", queue, drops)
	}

	cfg.Collect.MetricsAddr = "127.0.0.1:1"
	cfg.Collect.ExtraMetrics = []string{"127.0.0.1:2"}
	f, _ = evaluate(cfg)
	found := false
	for _, x := range f {
		found = found || x.Type == "collector.metrics"
	}
	if !found {
		t.Fatal("no reachable slot must raise collector.metrics")
	}
}
