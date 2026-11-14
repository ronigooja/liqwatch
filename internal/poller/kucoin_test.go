package poller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestKucoinSnapshots(t *testing.T) {
	risk, err := os.ReadFile("testdata/kucoin/risk.json")
	if err != nil {
		t.Fatal(err)
	}
	contracts, err := os.ReadFile("testdata/kucoin/contracts.json")
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.UserAgent() != DefaultUserAgent {
			t.Errorf("user agent: %q", r.UserAgent())
		}
		counts[r.URL.Path]++
		switch r.URL.Path {
		case "/api/v1/contracts/active":
			w.Write(contracts)
		case "/api/v1/contracts/risk-limit/XBTUSDTM":
			w.Write(risk)
		case "/api/v1/contracts/risk-limit/EMPTY":
			w.Write([]byte(`{"code":"200000","data":[]}`))
		default:
			w.Write([]byte(`{"code":"400100","msg":"bad symbol"}`))
		}
	}))
	defer server.Close()
	mapPath := filepath.Join(t.TempDir(), "map.toml")
	if err := os.WriteFile(mapPath, []byte("[[pair]]\nbinance='BTCUSDT'\nvenue='kucoin'\nsymbol='XBTUSDTM'\nstatus='auto'\n\n[[pair]]\nbinance='ETHUSDT'\nvenue='kucoin'\nsymbol='EMPTY'\nstatus='auto'\n\n[[pair]]\nbinance='SOLUSDT'\nvenue='kucoin'\nsymbol='OTHER'\nstatus='auto'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)}
	sink := &memorySink{}
	cfg := PollConfig{MapPath: mapPath, Venues: map[string]VenueConfig{"kucoin": {BaseURL: server.URL}}}
	env, channels, err := NewVenue("kucoin", cfg, Deps{Clock: clock, Sink: sink, Selected: []string{"XBTUSDTM", "EMPTY", "UNMAPPED"}})
	if err != nil {
		t.Fatal(err)
	}
	if env.RPS != 2 || env.WeightPerMinute != 4000 || channels[0].Weight != 5 {
		t.Fatal("incorrect defaults")
	}
	s, err := NewScheduler(channels, sink, clock, "", env.WeightPerMinute)
	if err != nil {
		t.Fatal(err)
	}
	env.Bind(s)
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(counts) != 3 || len(sink.records) != 3 {
		t.Fatalf("counts=%v records=%v", counts, sink.records)
	}
	got := map[string]Record{}
	for _, r := range sink.records {
		got[r.Channel] = r
	}
	if got["riskLimit"].Symbol != "XBTUSDTM" || got["contracts"].Symbol != "" || got["gap"].Symbol != "EMPTY" {
		t.Fatalf("records=%v", got)
	}
	var gap map[string]any
	if json.Unmarshal(got["gap"].Payload, &gap) != nil || gap["event"] != "no_data" {
		t.Fatalf("gap=%v", gap)
	}
	if !channels[0].Skip("EMPTY") {
		t.Fatal("empty not skipped")
	}
	clock.t = clock.t.Add(24 * time.Hour)
	if channels[0].Skip("EMPTY") {
		t.Fatal("empty skipped after 24h")
	}
	if _, err := channels[0].Fetch(context.Background(), "OTHER"); err == nil || !strings.Contains(err.Error(), "400100") {
		t.Fatalf("error wrapper: %v", err)
	}
}
