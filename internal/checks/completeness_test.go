package checks

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/config"
)

func writeChannelFixture(t *testing.T, cfg *config.Config, path, contents string, at time.Time) {
	t.Helper()
	p := filepath.Join(cfg.DataDir, "raw", path)
	if err := os.MkdirAll(filepath.Dir(p), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(contents), 0640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, at, at); err != nil {
		t.Fatal(err)
	}
}

func TestChannelCompletenessGolden(t *testing.T) {
	metrics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("liqwatch_queue_depth_ratio 0.1\n"))
	}))
	defer metrics.Close()
	now := time.Date(2026, 11, 22, 0, 1, 0, 0, time.UTC)
	cfg := &config.Config{DataDir: t.TempDir(), Checks: config.Checks{
		Venues: []string{"mexc", "bybit", "binance"}, DiskWarnPct: 101, UnverifiedHours: 3,
		StaleSec:      map[string]int{"forceOrder_all": 0},
		ChannelMaxAge: map[string]map[string]string{"mexc": {"funding": "2m"}},
	}}
	cfg.Collect.Venue = "binance"
	cfg.Collect.MetricsAddr = strings.TrimPrefix(metrics.URL, "http://")
	for _, path := range []string{
		"binance/BTCUSDT/20261122/aggTrade.a.jsonl.zst", "binance/ETHUSDT/20261122/markPrice_all.jsonl",
		"binance/BTCUSDT/20261122/brackets.p.jsonl.zst", "binance/_all/20261122/exchangeInfo.p.jsonl",
		"binance/BTCUSDT/20261122/openInterestHist.p.jsonl", "binance/BTCUSDT/20261122/fundingRate.p.jsonl",
		"binance/BTCUSDT/20261122/globalLongShortAccountRatio.p.jsonl", "binance/BTCUSDT/20261122/topLongShortAccountRatio.p.jsonl",
		"binance/BTCUSDT/20261122/topLongShortPositionRatio.p.jsonl", "binance/_all/20261122/forceOrder_all.jsonl",
		"bybit/BTCUSDT/20261122/trade.jsonl", "bybit/BTCUSDT/20261122/mark.jsonl",
		"bybit/BTCUSDT/20261122/accountRatio.p.jsonl", "bybit/_all/20261122/riskLimit.p.jsonl",
		"bybit/BTCUSDT/20261122/instruments.p.jsonl", "bybit/_all/20261122/instruments.a.jsonl",
		"mexc/BTC_USDT/20261122/trade.jsonl", "mexc/BTC_USDT/20261122/mark.jsonl",
		"mexc/BTC_USDT/20261122/index.jsonl", "mexc/BTC_USDT/20261122/ticker.jsonl",
		"mexc/BTC_USDT/20261122/contractDetail.p.jsonl",
	} {
		writeChannelFixture(t, cfg, path, "data\n", now)
	}
	writeChannelFixture(t, cfg, "binance/BTCUSDT/20261122/forceOrder_all.jsonl", "", now)
	writeChannelFixture(t, cfg, "bybit/ETHUSDT/20261121/liquidation.b.jsonl.zst", "data\n", now.Add(-5*time.Hour))
	writeChannelFixture(t, cfg, "mexc/BTC_USDT/20261121/funding.jsonl", "data\n", now.Add(-3*time.Minute))

	got, err := evaluateAt(cfg, now, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	for _, f := range got {
		if f.Type == "data.stale_write" {
			b, err := json.Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			out.Write(b)
			out.WriteByte('\n')
		}
	}
	want, err := os.ReadFile("testdata/channel-completeness.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != string(want) {
		t.Fatalf("channel findings differ:\ngot:\n%swant:\n%s", out.String(), want)
	}
}

func TestDailyChannelWriteSurvivesArchiveCleanup(t *testing.T) {
	now := time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)
	cfg := &config.Config{DataDir: t.TempDir(), Checks: config.Checks{Venues: []string{"binance"}},
		Archive: config.Archive{Mode: "local", DeleteSources: true}}
	writeChannelFixture(t, cfg, "binance/_all/20261121/exchangeInfo.p.jsonl", "data\n", now.Add(-time.Hour))
	daily := map[string]time.Time{}
	key := "data.stale_write:binance:exchangeInfo"
	missing := func(at time.Time) bool {
		t.Helper()
		var changed bool
		fs, err := channelFindings(cfg, at, daily, &changed)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range fs {
			if f.Key == key {
				return true
			}
		}
		return false
	}
	if missing(now) || !daily[key].Equal(now.Add(-time.Hour)) {
		t.Fatalf("previous-day raw not observed: %v", daily)
	}
	state, err := NewAlertState(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	state.Daily = daily
	if err := state.save(); err != nil {
		t.Fatal(err)
	}
	state, err = NewAlertState(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	daily = state.Daily
	if err := os.Remove(filepath.Join(cfg.DataDir, "raw/binance/_all/20261121/exchangeInfo.p.jsonl")); err != nil {
		t.Fatal(err)
	}
	if missing(now.Add(2*time.Hour)) || !missing(now.Add(36*time.Hour)) {
		t.Fatal("cached evidence must bridge deletion but expire after the configured age")
	}
}
