package checks

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/config"
)

func metricsStartupFixture(t *testing.T, status int) (*config.Config, time.Time, map[string]time.Time) {
	t.Helper()
	t.Setenv("PATH", "") // Ignore the host's clock offset.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)
	c := &config.Config{DataDir: t.TempDir(), Checks: config.Checks{Venues: []string{"binance"}, DiskWarnPct: 101}}
	c.Collect.MetricsAddr = strings.TrimPrefix(server.URL, "http://")
	// Exercise all configured slots, including extra_metrics_addrs.
	c.Collect.ExtraMetrics = []string{c.Collect.MetricsAddr, c.Collect.MetricsAddr, c.Collect.MetricsAddr,
		c.Collect.MetricsAddr, c.Collect.MetricsAddr, c.Collect.MetricsAddr}
	start := time.Date(2026, 11, 22, 14, 46, 23, 0, time.UTC)
	return c, start, map[string]time.Time{"binance": start}
}

func assertMetricsStartupGolden(t *testing.T, c *config.Config, now time.Time, starts map[string]time.Time, active map[string]bool, golden string) {
	t.Helper()
	findings, err := evaluateAt(c, now, starts, active)
	if err != nil {
		t.Fatal(err)
	}
	var got strings.Builder
	for _, f := range findings {
		if !strings.HasPrefix(f.Key, "collector.") {
			continue
		}
		b, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		got.Write(b)
		got.WriteByte('\n')
	}
	want, err := os.ReadFile("testdata/metrics-" + golden + ".jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != string(want) {
		t.Fatalf("collector findings differ:\ngot:\n%swant:\n%s", got.String(), want)
	}
}

func TestMetricsStartupWithinGraceGolden(t *testing.T) {
	c, start, starts := metricsStartupFixture(t, http.StatusServiceUnavailable)
	assertMetricsStartupGolden(t, c, start, starts, nil, "none")
	assertMetricsStartupGolden(t, c, start.Add(5*time.Minute-time.Nanosecond), starts, nil, "none")
}

func TestMetricsStartupExpiredGolden(t *testing.T) {
	c, start, starts := metricsStartupFixture(t, http.StatusServiceUnavailable)
	assertMetricsStartupGolden(t, c, start.Add(5*time.Minute), starts, nil, "unreachable")
}

func TestMetricsStartupReachableGolden(t *testing.T) {
	c, start, starts := metricsStartupFixture(t, http.StatusOK)
	assertMetricsStartupGolden(t, c, start, starts, nil, "none")
	assertMetricsStartupGolden(t, c, start.Add(5*time.Minute), starts, map[string]bool{"collector.metrics": true}, "none")
}

func TestMetricsStartupActiveGolden(t *testing.T) {
	c, start, starts := metricsStartupFixture(t, http.StatusServiceUnavailable)
	assertMetricsStartupGolden(t, c, start, starts, map[string]bool{"collector.metrics": true}, "unreachable")
}

func TestMetricsStartupEvidenceGolden(t *testing.T) {
	c, start, starts := metricsStartupFixture(t, http.StatusServiceUnavailable)
	assertMetricsStartupGolden(t, c, start, nil, nil, "unreachable")
	assertMetricsStartupGolden(t, c, start.Add(-time.Second), starts, nil, "unreachable")
	assertMetricsStartupGolden(t, c, start, map[string]time.Time{"bybit": start}, nil, "unreachable")
}

func TestMetricsStartupFlushWindowGolden(t *testing.T) {
	c, start, starts := metricsStartupFixture(t, http.StatusServiceUnavailable)
	c.Record.Flush = "4m"
	assertMetricsStartupGolden(t, c, start.Add(5*time.Minute), starts, nil, "none")
	assertMetricsStartupGolden(t, c, start.Add(8*time.Minute), starts, nil, "unreachable")
}
