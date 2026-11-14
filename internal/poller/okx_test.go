package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestOKXFixtures(t *testing.T) {
	for i := 0; i < 7; i++ {
		b, err := os.ReadFile(fmt.Sprintf("testdata/okx/response%d.json", i))
		if err != nil {
			t.Fatal(err)
		}
		var v okxEnvelope
		if err = json.Unmarshal(b, &v); err != nil || v.Code != "0" {
			t.Fatalf("fixture %d: %v", i, err)
		}
	}
}

func TestOKXHistoryAndSnapshots(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.UserAgent() != DefaultUserAgent {
			t.Errorf("user agent %q", r.UserAgent())
		}
		paths = append(paths, r.URL.Path)
		fixture := 0
		switch {
		case strings.Contains(r.URL.Path, "top-trader") && strings.Contains(r.URL.Path, "position"):
			fixture = 2
		case strings.Contains(r.URL.Path, "top-trader"):
			fixture = 1
		case strings.Contains(r.URL.Path, "taker-volume"):
			fixture = 3
		case strings.HasSuffix(r.URL.Path, "long-short-account-ratio"):
			fixture = 4
		case strings.HasSuffix(r.URL.Path, "position-tiers"):
			fixture = 5
			if r.URL.Query().Get("instFamily") != "BTC-USDT,ETH-USDT" {
				t.Errorf("families %s", r.URL.RawQuery)
			}
		case strings.HasSuffix(r.URL.Path, "instruments"):
			fixture = 6
		}
		b, err := os.ReadFile(fmt.Sprintf("testdata/okx/response%d.json", fixture))
		if err != nil {
			t.Error(err)
		}
		w.Write(b)
	}))
	defer server.Close()
	e := &Env{Venue: "okx", Doer: uaDoer{server.Client()}, Clock: RealClock{}, BaseURL: server.URL, Config: PollConfig{}, Universe: func() []string { return []string{"BTC-USDT-SWAP", "ETH-USDT-SWAP"} }, Periods: map[string]time.Duration{}, Weights: map[string]float64{}, ChannelRPS: map[string]float64{}}
	channels := okxFactory(e)
	for _, c := range channels {
		if c.Name == "instruments" || c.Name == "positionTiers" {
			r, err := c.Fetch(context.Background(), "")
			if err != nil || len(r) == 0 {
				t.Fatalf("%s %v", c.Name, err)
			}
		}
	}
	rows, err := okxRows(context.Background(), e, "/api/v5/rubik/stat/contracts/long-short-account-ratio-contract", "BTC-USDT-SWAP", 1, 2000000000000, false)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows %d: %v", len(rows), err)
	}
	if len(paths) != 4 {
		t.Fatalf("requests %d", len(paths))
	}
	if okxCurrency("1000PEPE-USDT-SWAP") != "1000PEPE" {
		t.Fatal("currency")
	}
}

func TestOKXBackwardPaging(t *testing.T) {
	const step = int64(300000)
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		start, _ := strconv.ParseInt(r.URL.Query().Get("begin"), 10, 64)
		end, _ := strconv.ParseInt(r.URL.Query().Get("end"), 10, 64)
		rows := [][]string{}
		for n := int64(250); n >= 1 && len(rows) < 100; n-- {
			ts := n * step
			if ts >= start && ts < end {
				rows = append(rows, []string{strconv.FormatInt(ts, 10), "1"})
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"code": "0", "data": rows})
	}))
	defer server.Close()
	e := &Env{Doer: uaDoer{server.Client()}, BaseURL: server.URL, Clock: RealClock{}}
	h := History{Venue: "okx", Channel: "test", Symbol: "BTC-USDT-SWAP", Clock: okxTestClock{time.UnixMilli(250 * step)}, Lookback: 250 * 5 * time.Minute, Step: 5 * time.Minute, Mode: PageBackward, PageSize: 100, Fetch: func(ctx context.Context, a, b int64) ([]Row, error) {
		return okxRows(ctx, e, "/test", "BTC-USDT-SWAP", a, b, false)
	}}
	records, err := h.Run(context.Background())
	if err != nil || len(records) != 248 || calls != 3 {
		t.Fatalf("records=%d calls=%d err=%v", len(records), calls, err)
	}
}

type okxTestClock struct{ time.Time }

func (c okxTestClock) Now() time.Time                                   { return c.Time }
func (c okxTestClock) Sleep(ctx context.Context, d time.Duration) error { return nil }

func TestOKXCurrencyRetentionError(t *testing.T) {
	for _, hasCursor := range []bool{false, true} {
		t.Run(fmt.Sprint(hasCursor), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				fmt.Fprint(w, `{"code":"50030","msg":"Data older than retention","data":[]}`)
			}))
			defer server.Close()
			clock := &fakeClock{t: time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)}
			sink := &memorySink{}
			e := &Env{Venue: "okx", Doer: uaDoer{server.Client()}, Clock: clock, BaseURL: server.URL, Lookback: time.Hour, Universe: func() []string { return []string{"BTC-USDT-SWAP"} }}
			channels := okxFactory(e)
			var ch Channel
			for _, v := range channels {
				if v.Name == "longShortAccountRatio" {
					ch = v
				}
			}
			s, _ := NewScheduler([]Channel{ch}, sink, clock, "", 240)
			e.Bind(s)
			if hasCursor {
				s.state.Jobs["okx/longShortAccountRatio/BTC-USDT-SWAP"] = jobState{RowTimestamp: clock.Now().Add(-72 * time.Hour).UnixMilli()}
			}
			records, err := ch.Fetch(context.Background(), "BTC-USDT-SWAP")
			if err != nil {
				t.Fatal(err)
			}
			found := false
			if hasCursor {
				for _, r := range sink.records {
					found = found || strings.Contains(string(r.Payload), `"retention_gap"`)
				}
			} else {
				for _, r := range records {
					found = found || strings.Contains(string(r.Payload), `"no_data"`)
				}
			}
			if !found || !ch.Skip("BTC-USDT-SWAP") || calls == 0 {
				t.Fatalf("missing gap or throttle, cursor=%v records=%v sink=%v calls=%d", hasCursor, records, sink.records, calls)
			}
		})
	}
}

func TestOKXNoDataAndSourceGap(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: now}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("instId") == "EMPTY-USDT-SWAP" {
			fmt.Fprint(w, `{"code":"0","data":[]}`)
			return
		}
		fmt.Fprintf(w, `{"code":"0","data":[["%d","2"],["%d","1"]]}`, now.Add(-10*time.Minute).UnixMilli(), now.Add(-20*time.Minute).UnixMilli())
	}))
	defer server.Close()
	e := &Env{Venue: "okx", Doer: uaDoer{server.Client()}, Clock: clock, BaseURL: server.URL, Lookback: time.Hour, Universe: func() []string { return []string{"EMPTY-USDT-SWAP", "BTC-USDT-SWAP"} }}
	ch := okxFactory(e)[0]
	sink := &memorySink{}
	s, _ := NewScheduler([]Channel{ch}, sink, clock, "", 240)
	e.Bind(s)
	empty, err := ch.Fetch(context.Background(), "EMPTY-USDT-SWAP")
	if err != nil || len(empty) != 1 || !strings.Contains(string(empty[0].Payload), `"no_data"`) || !ch.Skip("EMPTY-USDT-SWAP") {
		t.Fatalf("no_data %v %v", empty, err)
	}
	r, err := ch.Fetch(context.Background(), "BTC-USDT-SWAP")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, v := range r {
		if v.Channel == "gap" && strings.Contains(string(v.Payload), `"source_gap"`) {
			found = true
		}
	}
	if !found {
		t.Fatal("missing source gap")
	}
	// One request for the empty instrument, then the data page plus one confirming request: the page's
	// earliest row is more than two steps after the lookback start, so coverage paging asks once more.
	if calls != 3 {
		t.Fatalf("calls %d", calls)
	}
}

func TestOKXOverlapDedup(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: now}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"code":"0","data":[["%d","2"],["%d","1"]]}`, now.Add(-10*time.Minute).UnixMilli(), now.Add(-15*time.Minute).UnixMilli())
	}))
	defer server.Close()
	e := &Env{Venue: "okx", Doer: uaDoer{server.Client()}, Clock: clock, BaseURL: server.URL, Lookback: time.Hour, Universe: func() []string { return []string{"BTC-USDT-SWAP"} }}
	ch := okxFactory(e)[0]
	sink := &memorySink{}
	s, _ := NewScheduler([]Channel{ch}, sink, clock, "", 240)
	e.Bind(s)
	for i := 0; i < 2; i++ {
		records, err := ch.Fetch(context.Background(), "BTC-USDT-SWAP")
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range records {
			if !s.dedup(&r) {
				sink.Write(r)
			}
		}
		s.state.Jobs["okx/longShortAccountRatioContract/BTC-USDT-SWAP"] = jobState{RowTimestamp: now.Add(-10 * time.Minute).UnixMilli()}
	}
	if len(sink.records) != 2 || s.Duplicates.Load() != 2 {
		t.Fatalf("records %d duplicates %d", len(sink.records), s.Duplicates.Load())
	}
}

func TestOKXUniverseFromMap(t *testing.T) {
	p := t.TempDir() + "/map.toml"
	b := `[[pair]]
venue="okx"
binance="BTCUSDT"
symbol="BTC-USDT-SWAP"
status="auto"
[[pair]]
venue="okx"
binance="ETHUSDT"
symbol="ETH-USDT-SWAP"
status="reviewed"
[[pair]]
venue="okx"
binance="SOLUSDT"
symbol="SOL-USDT-SWAP"
status="review"
[[pair]]
venue="bybit"
binance="DOGEUSDT"
symbol="DOGEUSDT"
status="auto"
`
	if err := os.WriteFile(p, []byte(b), 0600); err != nil {
		t.Fatal(err)
	}
	e, _, err := NewVenue("okx", PollConfig{MapPath: p}, Deps{Selected: []string{"ETH-USDT-SWAP", "SOL-USDT-SWAP"}})
	if err != nil {
		t.Fatal(err)
	}
	got := e.Universe()
	if len(got) != 1 || got[0] != "ETH-USDT-SWAP" {
		t.Fatalf("universe %v", got)
	}
	if okxCurrency("PEPE-USDT-SWAP") != "PEPE" || okxCurrency("1000PEPE-USDT-SWAP") != "1000PEPE" {
		t.Fatal("currency derivation")
	}
}
