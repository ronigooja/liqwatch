package poller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func pair(binance, symbol, status string) string {
	return fmt.Sprintf("[[pair]]\nbinance = %q\nvenue = 'okx'\nsymbol = %q\nexp10 = 0\nstatus = %q\n\n", binance, symbol, status)
}

func writeMap(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "map.toml")
	body := pair("BTCUSDT", "BTC-USDT-SWAP", "auto") + pair("ETHUSDT", "ETH-USDT-SWAP", "reviewed") + pair("XRPUSDT", "XRP-USDT-SWAP", "rejected")
	if err := os.WriteFile(p, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

type uaRecorder struct {
	mu   sync.Mutex
	seen []string
}

func (u *uaRecorder) handler(w http.ResponseWriter, r *http.Request) {
	u.mu.Lock()
	u.seen = append(u.seen, r.Header.Get("User-Agent")+"|"+r.URL.String())
	u.mu.Unlock()
	fmt.Fprint(w, `{"ok":true}`)
}

func registerOKXLike(t *testing.T) {
	t.Helper()
	old := registry["okx"]
	t.Cleanup(func() {
		if old == nil {
			delete(registry, "okx")
		} else {
			registry["okx"] = old
		}
	})
	RegisterVenue("okx", func(e *Env) []Channel {
		return []Channel{
			e.Channel("ping", ChannelSpec{PerSymbol: true, Period: time.Minute, Fetch: func(ctx context.Context, sym string) ([]Record, error) {
				resp, err := e.Get(ctx, "/ping", url.Values{"s": {sym}})
				if err != nil {
					return nil, err
				}
				return []Record{{Venue: "okx", Symbol: sym, Channel: "ping", Payload: resp.Body}}, nil
			}}),
		}
	}, WithBaseURL("https://prod.invalid/"))
}

func TestVenueEnvUniverseUserAgentBaseURLAndLimits(t *testing.T) {
	registerOKXLike(t)
	rec := &uaRecorder{}
	server := httptest.NewServer(http.HandlerFunc(rec.handler))
	defer server.Close()
	cfg := PollConfig{MapPath: writeMap(t), Venues: map[string]VenueConfig{"okx": {BaseURL: server.URL + "/", RPS: 4, WeightPerMinute: 77, Periods: map[string]string{"ping": "5m"}, ChannelRPS: map[string]float64{"ping": 8}, Weights: map[string]float64{"ping": 3}, InitialLookback: "6h", Overlap: "30m"}}}
	sink := &memorySink{}
	clock := &fakeClock{time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)}
	env, channels, err := NewVenue("okx", cfg, Deps{Doer: server.Client(), Clock: clock, Sink: sink})
	if err != nil {
		t.Fatal(err)
	}
	if env.WeightPerMinute != 77 || env.Lookback != 6*time.Hour || env.Overlap != 30*time.Minute {
		t.Fatalf("%+v", env)
	}
	if c := channels[0]; c.Venue != "okx" || c.Period != 5*time.Minute || c.Daily || c.RPS != 8 || c.Weight != 3 {
		t.Fatalf("%+v", c)
	}
	if got := env.Universe(); len(got) != 2 || got[0] != "BTC-USDT-SWAP" || got[1] != "ETH-USDT-SWAP" {
		t.Fatalf("universe %v", got)
	}
	s, err := NewScheduler(channels, sink, clock, "", env.WeightPerMinute)
	if err != nil {
		t.Fatal(err)
	}
	env.Bind(s)
	for i := 0; i < 2; i++ {
		if _, err = s.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		clock.t = clock.t.Add(time.Second)
	}
	if len(rec.seen) != 2 || rec.seen[0] != "Mozilla/5.0|/ping?s=BTC-USDT-SWAP" || rec.seen[1] != "Mozilla/5.0|/ping?s=ETH-USDT-SWAP" {
		t.Fatalf("requests %v", rec.seen)
	}
	env, _, err = NewVenue("okx", cfg, Deps{Doer: server.Client(), Selected: []string{"ETH-USDT-SWAP", "NOPE"}})
	if err != nil || len(env.Universe()) != 1 || env.Universe()[0] != "ETH-USDT-SWAP" {
		t.Fatalf("selected %v %v", env.Universe(), err)
	}
	// default base URL when no override
	cfg.Venues = nil
	env, _, _ = NewVenue("okx", cfg, Deps{Doer: server.Client()})
	if env.BaseURL != "https://prod.invalid" || env.RPS != 1 {
		t.Fatalf("base %q rps %v", env.BaseURL, env.RPS)
	}
}

func TestUserAgentKeepsExplicitValue(t *testing.T) {
	rec := &uaRecorder{}
	server := httptest.NewServer(http.HandlerFunc(rec.handler))
	defer server.Close()
	d := uaDoer{server.Client()}
	req, _ := http.NewRequest("GET", server.URL+"/a", nil)
	d.Do(req)
	req, _ = http.NewRequest("GET", server.URL+"/b", nil)
	req.Header.Set("User-Agent", "custom")
	d.Do(req)
	if rec.seen[0] != "Mozilla/5.0|/a" || rec.seen[1] != "custom|/b" {
		t.Fatal(rec.seen)
	}
}

func TestBinanceRequestsCarryUserAgent(t *testing.T) {
	rec := &uaRecorder{}
	server := httptest.NewServer(http.HandlerFunc(rec.handler))
	defer server.Close()
	cfg := PollConfig{Venues: map[string]VenueConfig{"binance": {BaseURL: server.URL}}}
	_, channels, err := NewVenue("binance", cfg, Deps{Doer: server.Client(), Clock: &fakeClock{time.Now()}})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range channels {
		if c.Name == "constituents" {
			c.Fetch(context.Background(), "BTCUSDT")
		}
	}
	if len(rec.seen) != 1 || !strings.HasPrefix(rec.seen[0], "Mozilla/5.0|/fapi/v1/constituents") {
		t.Fatal(rec.seen)
	}
}

func TestBinanceKeepsLegacyConfigAndExchangeInfoUniverse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"symbols":[{"symbol":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING","quoteAsset":"USDT"}]}`)
	}))
	defer server.Close()
	cfg := PollConfig{BaseURL: server.URL, MapPath: "/does/not/exist.toml", Periods: map[string]string{"fundingRate": "1h"}, RPS: map[string]float64{"fundingRate": 5}}
	sink := &memorySink{}
	clock := &fakeClock{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	env, channels, err := NewVenue("binance", cfg, Deps{Doer: server.Client(), Clock: clock, Sink: sink})
	if err != nil {
		t.Fatalf("binance must not need the venue map: %v", err)
	}
	for _, c := range channels {
		if c.Name == "fundingRate" && (c.Period != time.Hour || c.RPS != 5 || c.Daily) {
			t.Fatalf("%+v", c)
		}
	}
	s, _ := NewScheduler(channels, sink, clock, "", env.WeightPerMinute)
	env.Bind(s)
	if _, err = s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := env.Universe(); len(got) != 1 || got[0] != "BTCUSDT" {
		t.Fatalf("universe %v", got)
	}
}

func TestUnknownVenueListsRegistered(t *testing.T) {
	_, _, err := NewVenue("nope", PollConfig{}, Deps{})
	if err == nil || !strings.Contains(err.Error(), `unknown venue "nope"`) || !strings.Contains(err.Error(), "binance") {
		t.Fatal(err)
	}
	if v := Venues(); len(v) == 0 || v[0] != "binance" && !strings.Contains(strings.Join(v, ","), "binance") {
		t.Fatal(v)
	}
}

func TestEnvRequestErrors(t *testing.T) {
	status := 200
	body := `{}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status == 429 {
			w.Header().Set("Retry-After", "7")
		}
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
	defer server.Close()
	e := &Env{Venue: "v", Doer: uaDoer{server.Client()}, Clock: &fakeClock{time.Now()}, BaseURL: server.URL}
	status = 429
	_, err := e.Get(context.Background(), "/x", nil)
	var re *RetryError
	if r, ok := err.(*RetryError); !ok || r.After != 7*time.Second {
		t.Fatalf("%v %v", err, re)
	}
	status, body = 500, "boom"
	if _, err = e.Get(context.Background(), "/x", nil); err == nil || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatal(err)
	}
	status, body = 200, "not json"
	if _, err = e.Get(context.Background(), "/x", nil); err == nil {
		t.Fatal("invalid JSON accepted")
	}
}

func TestHistoryChannelEndToEnd(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	f := newFakeExchange(now.Add(-3*time.Hour).UnixMilli(), now.UnixMilli(), true, 50)
	defer f.server.Close()
	clock := &fakeClock{now}
	sink := &memorySink{}
	e := &Env{Venue: "fake", Doer: uaDoer{f.server.Client()}, Clock: clock, Sink: sink, RPS: 1, Overlap: time.Hour, Lookback: 2 * time.Hour, Periods: map[string]time.Duration{}, ChannelRPS: map[string]float64{}, Weights: map[string]float64{}}
	ch := e.HistoryChannel("oi", HistorySpec{Step: step5, Grace: 2 * step5, Mode: PageBackward, PageSize: 50, Fetch: func(ctx context.Context, sym string, start, end int64) ([]Row, error) {
		return f.fetch(ctx, start, end)
	}})
	s, _ := NewScheduler([]Channel{ch}, sink, clock, "", 240)
	e.Universe = func() []string { return []string{"BTCUSDT"} }
	e.Bind(s)
	if _, err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.records) != 22 || s.RowTimestamp("fake", "oi", "BTCUSDT") != now.Add(-3*step5).UnixMilli() {
		t.Fatalf("records=%d cursor=%d", len(sink.records), s.RowTimestamp("fake", "oi", "BTCUSDT"))
	}
}
