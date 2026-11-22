package checks

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
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

func TestFreshnessStartupGolden(t *testing.T) {
	start := time.Date(2026, 11, 22, 13, 34, 27, 0, time.UTC)
	starts := parseRecorderStarts(fmt.Sprintf("Id=liqwatch-recorder@binance.service\nExecMainStartTimestamp=@%d\n\nId=liqwatch-recorder@bitget.service\nExecMainStartTimestamp=@%d\n", start.Unix(), start.Unix()))
	cfg := &config.Config{DataDir: t.TempDir(), Checks: config.Checks{Venues: []string{"binance", "bitget"}}}
	for _, state := range []string{"poll-state.json", "poll-state-bitget.json"} {
		writeChannelFixture(t, cfg, state, fmt.Sprintf(`{"recording_start_ms":%d}`, start.UnixMilli()), start)
	}
	for _, path := range []string{"binance/BTCUSDT/brackets.p", "binance/_all/exchangeInfo.p", "bitget/BTCUSDT/positionTiers.p", "bitget/_all/contracts.p"} {
		parts := strings.Split(path, "/")
		writeChannelFixture(t, cfg, parts[0]+"/"+parts[1]+"/20261122/"+parts[2]+".jsonl", "data\n", start)
	}
	assertFreshnessGolden(t, cfg, start.Add(2*time.Minute), starts, nil, "")
	// An already-firing condition must not resolve merely because of a restart.
	assertFreshnessGolden(t, cfg, start.Add(2*time.Minute), starts,
		map[string]bool{"data.stale_write:binance:aggTrade": true}, "startup-active")
	if err := os.Remove(filepath.Join(cfg.DataDir, "raw/poll-state-bitget.json")); err != nil {
		t.Fatal(err)
	}
	assertFreshnessGolden(t, cfg, start.Add(2*time.Minute), starts, nil, "")
}

func writeBitgetWS(t *testing.T, cfg *config.Config, now time.Time) {
	t.Helper()
	for _, ch := range []string{"trade", "ticker", "funding", "liquidation", "tier"} {
		writeChannelFixture(t, cfg, "bitget/BTCUSDT/"+now.Format("20060102")+"/"+ch+".jsonl", "data\n", now)
	}
}

func TestFreshnessOverdueGolden(t *testing.T) {
	now := time.Date(2026, 11, 22, 15, 5, 0, 0, time.UTC)
	cfg := &config.Config{DataDir: t.TempDir(), Checks: config.Checks{Venues: []string{"bitget"}}}
	// Successful empty jobs have no raw write and must not slide the deadline.
	writeChannelFixture(t, cfg, "poll-state-bitget.json", fmt.Sprintf(`{"recording_start_ms":%d,"jobs":{"bitget/accountLongShort/BTCUSDT":{"last_success":%d}}}`,
		now.Add(-90*time.Minute).UnixMilli(), now.UnixNano()), now)
	writeBitgetWS(t, cfg, now)
	writeChannelFixture(t, cfg, "bitget/BTCUSDT/20261122/positionTiers.p.jsonl", "data\n", now)
	writeChannelFixture(t, cfg, "bitget/_all/20261122/contracts.p.jsonl", "data\n", now)
	writeChannelFixture(t, cfg, "bitget/ETHUSDT/20261122/positionLongShort.p.jsonl", "data\n", now)
	writeChannelFixture(t, cfg, "bitget/BTCUSDT/20261122/longShort.p.jsonl", "data\n", now.Add(-181*time.Minute))
	assertFreshnessGolden(t, cfg, now.Add(-time.Second), nil, nil, "overdue-before")
	assertFreshnessGolden(t, cfg, now, nil, nil, "overdue")
	// A new recorder process cannot move the persisted REST deadline.
	assertFreshnessGolden(t, cfg, now, map[string]time.Time{"bitget": now}, nil, "overdue")
}

func TestFreshnessVenueHealthyGolden(t *testing.T) {
	now := time.Date(2026, 11, 22, 15, 0, 0, 0, time.UTC)
	cfg := &config.Config{DataDir: t.TempDir(), Checks: config.Checks{Venues: []string{"bitget"}}}
	writeBitgetWS(t, cfg, now)
	writeChannelFixture(t, cfg, "bitget/BTCUSDT/20261122/positionTiers.p.jsonl", "data\n", now)
	writeChannelFixture(t, cfg, "bitget/_all/20261122/contracts.p.jsonl", "data\n", now)
	for _, ch := range []string{"accountLongShort", "longShort", "positionLongShort"} {
		writeChannelFixture(t, cfg, "bitget/ETHUSDT/20261122/"+ch+".p.jsonl.zst", "data\n", now.Add(-time.Hour))
	}
	writeChannelFixture(t, cfg, "poll-state-bitget.json", fmt.Sprintf(`{"recording_start_ms":%d,"jobs":{"bitget/longShort/BTCUSDT":{"no_data_at":%d},"bitget/longShort/ETHUSDT":{"row_timestamp":%d}}}`,
		now.Add(-48*time.Hour).UnixMilli(), now.UnixNano(), now.Add(-time.Hour).UnixMilli()), now)
	assertFreshnessGolden(t, cfg, now, nil, nil, "")
}

func TestFreshnessAfterArchiveGolden(t *testing.T) {
	now := time.Date(2026, 11, 22, 2, 0, 0, 0, time.UTC)
	cfg := &config.Config{DataDir: t.TempDir(), Checks: config.Checks{Venues: []string{"mexc"}},
		Archive: config.Archive{Mode: "local", DeleteSources: true}}
	for _, ch := range []string{"trade", "mark", "index", "ticker", "funding"} {
		writeChannelFixture(t, cfg, "mexc/BTC_USDT/20261122/"+ch+".jsonl", "data\n", now)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.ManifestPath()), 0750); err != nil {
		t.Fatal(err)
	}
	db, err := manifest.Open(cfg.ManifestPath())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.RegisterPack(manifest.Pack{Path: "pack", Venue: "mexc", Symbol: "BTC_USDT", Day: 20261121, State: manifest.StateLocalVerified}); err != nil {
		t.Fatal(err)
	}
	verified, err := db.ListByState(manifest.StateLocalVerified)
	if err != nil || len(verified) != 1 {
		t.Fatalf("verified packs: %v, %v", verified, err)
	}
	if err := db.BeginLocalDeletion(verified[0].ID, []manifest.Member{{Name: "contractDetail.p.jsonl.zst", Bytes: 10, MtimeNS: now.Add(-3 * time.Hour).UnixNano()}}); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(cfg.DataDir, "raw/mexc/BTC_USDT/20261121")
	writeChannelFixture(t, cfg, "mexc/BTC_USDT/20261121/contractDetail.p.jsonl.zst", "data\n", now.Add(-3*time.Hour))
	if err := os.Rename(dir, dir+".deleting"); err != nil {
		t.Fatal(err)
	}
	assertFreshnessGolden(t, cfg, now, nil, nil, "")
	if err := os.RemoveAll(dir + ".deleting"); err != nil {
		t.Fatal(err)
	}
	if err := db.SetState(verified[0].ID, manifest.StateLocalDeleted); err != nil {
		t.Fatal(err)
	}
	assertFreshnessGolden(t, cfg, now, nil, nil, "")
	cfg.Checks.ChannelMaxAge = map[string]map[string]string{"mexc": {"contractDetail": "2h30m"}}
	assertFreshnessGolden(t, cfg, now, nil, nil, "archive-expired")
}

func assertFreshnessGolden(t *testing.T, cfg *config.Config, now time.Time, starts map[string]time.Time, active map[string]bool, name string) {
	t.Helper()
	findings, err := channelFindings(cfg, now, starts, active)
	if err != nil {
		t.Fatal(err)
	}
	var got strings.Builder
	for _, f := range findings {
		b, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		got.Write(b)
		got.WriteByte('\n')
	}
	var want []byte
	if name != "" {
		want, err = os.ReadFile("testdata/freshness-" + name + ".jsonl")
		if err != nil {
			t.Fatal(err)
		}
	}
	if got.String() != string(want) {
		t.Fatalf("channel findings differ:\ngot:\n%swant:\n%s", got.String(), want)
	}
}
