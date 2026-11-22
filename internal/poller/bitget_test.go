package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestBitgetChannels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") != DefaultUserAgent {
			t.Error("missing user agent")
		}
		if strings.HasSuffix(r.URL.Path, "contracts") || strings.HasSuffix(r.URL.Path, "query-position-lever") {
			if r.URL.Query().Get("productType") != "USDT-FUTURES" {
				t.Error("wrong productType")
			}
			fmt.Fprint(w, `{"code":"00000","data":[]}`)
			return
		}
		if r.URL.Query().Get("period") != "5m" || r.URL.Query().Get("symbol") == "" {
			t.Error("wrong history query")
		}
		fmt.Fprint(w, `{"code":"00000","data":[{"ts":"1700000100000"},{"ts":"1700000400000"}]}`)
	}))
	defer server.Close()
	e := &Env{Venue: "bitget", BaseURL: server.URL, Doer: uaDoer{server.Client()}, Clock: RealClock{}, Lookback: 40000 * time.Hour, RPS: 1, Periods: map[string]time.Duration{}, ChannelRPS: map[string]float64{}, Weights: map[string]float64{}}
	channels := bitgetChannels(e)
	if len(channels) != 6 {
		t.Fatalf("got %d channels", len(channels))
	}
	for i, c := range channels {
		records, err := c.Fetch(context.Background(), "BTCUSDT")
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		want := 1
		if i < 4 {
			want = 2
			if c.Period != time.Hour {
				t.Errorf("%s period %v", c.Name, c.Period)
			}
		}
		if len(records) != want {
			t.Errorf("%s got %d records", c.Name, len(records))
		}
	}
}

func TestBitgetHourlyWindowAndOutage(t *testing.T) {
	clock := &fakeClock{time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)}
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("User-Agent") != DefaultUserAgent {
			t.Error("missing User-Agent")
		}
		rows := make([]map[string]string, 30)
		for i := range rows {
			rows[i] = map[string]string{"ts": fmt.Sprint(clock.Now().Add(time.Duration(i-29) * 5 * time.Minute).UnixMilli())}
		}
		json.NewEncoder(w).Encode(map[string]any{"code": "00000", "data": rows})
	}))
	defer server.Close()
	e := &Env{Venue: "bitget", BaseURL: server.URL, Doer: uaDoer{server.Client()}, Clock: clock, Lookback: 150 * time.Minute, RPS: 1}
	c := bitgetChannels(e)[0]
	sink := &memorySink{}
	s, _ := historicalScheduler([]Channel{c}, sink, clock, "", 240)
	e.Universe = func() []string { return []string{"BTCUSDT"} }
	e.Bind(s)
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.t = clock.t.Add(time.Hour)
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.records) != 40 {
		t.Fatalf("hourly records=%d, want 40", len(sink.records))
	}
	if calls != 2 {
		t.Fatalf("hourly requests=%d", calls)
	}
	clock.t = clock.t.Add(4 * time.Hour)
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	var gaps []map[string]any
	for _, r := range sink.records {
		if r.Channel == "gap" {
			var gap map[string]any
			if err := json.Unmarshal(r.Payload, &gap); err != nil {
				t.Fatal(err)
			}
			gaps = append(gaps, gap)
		}
	}
	if len(gaps) != 1 || gaps[0]["event"] != "source_gap" {
		t.Fatalf("gaps=%v", gaps)
	}
	wantStart := clock.Now().Add(-4*time.Hour - 5*time.Minute).UnixMilli()
	wantEnd := clock.Now().Add(-29 * 5 * time.Minute).UnixMilli()
	if int64(gaps[0]["start_ms"].(float64)) != wantStart || int64(gaps[0]["end_ms"].(float64)) != wantEnd {
		t.Fatalf("gap bounds=%v", gaps[0])
	}
}

func TestBitgetNoDataAndUniverse(t *testing.T) {
	clock := &fakeClock{time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("User-Agent") != DefaultUserAgent {
			t.Error("missing User-Agent")
		}
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"code":"40054","msg":"empty","data":null}`)
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "map.toml")
	data := "[[pair]]\nbinance='SANDUSDT'\nvenue='bitget'\nsymbol='SANDUSDT'\nstatus='auto'\n[[pair]]\nbinance='OTHERUSDT'\nvenue='bitget'\nsymbol='OTHERUSDT'\nstatus='rejected'\n[[pair]]\nbinance='BTCUSDT'\nvenue='bitget'\nsymbol='BTCUSDT'\nstatus='reviewed'\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := PollConfig{MapPath: path, Venues: map[string]VenueConfig{"bitget": {BaseURL: server.URL}}}
	e, channels, err := NewVenue("bitget", cfg, Deps{Doer: server.Client(), Clock: clock, Selected: []string{"SANDUSDT", "OTHERUSDT"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Universe(); !reflect.DeepEqual(got, []string{"SANDUSDT"}) {
		t.Fatalf("universe=%v", got)
	}
	sink := &memorySink{}
	s, _ := historicalScheduler(channels[:1], sink, clock, "", 240)
	e.Bind(s)
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.t = clock.t.Add(time.Hour)
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(sink.records) != 1 || !strings.Contains(string(sink.records[0].Payload), `"event":"no_data"`) {
		t.Fatalf("calls=%d records=%v", calls, sink.records)
	}
	clock.t = clock.t.Add(24 * time.Hour)
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(sink.records) != 1 {
		t.Fatalf("daily retry calls=%d records=%v", calls, sink.records)
	}
}

func TestBitgetLiveUnavailableGolden(t *testing.T) {
	for _, tc := range []struct{ name, endpoint string }{
		{"longShort", "long-short"},
		{"accountLongShort", "account-long-short"},
		{"positionLongShort", "position-long-short"},
		{"takerBuySell", "taker-buy-sell"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fakeClock{time.Date(2026, 11, 22, 13, 45, 0, 0, time.UTC)}
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.RequestURI() != "/api/v2/mix/market/"+tc.endpoint+"?period=5m&symbol=AAVEUSDT" {
					t.Errorf("unexpected request: %s", r.URL.RequestURI())
				}
				if calls == 1 {
					fmt.Fprint(w, `{"code":"00000","msg":"success","data":[]}`)
					return
				}
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(w, `{"code":"40054","msg":"The data fetched by AAVEUSDT is empty","requestTime":1795355100000,"data":null}`)
			}))
			defer server.Close()
			e := &Env{Venue: "bitget", BaseURL: server.URL, Doer: server.Client(), Clock: clock}
			channels := bitgetChannels(e)
			for _, c := range channels {
				if c.Name != tc.name {
					continue
				}
				s, err := NewScheduler([]Channel{c}, &memorySink{}, clock, "", 240)
				if err != nil {
					t.Fatal(err)
				}
				e.Bind(s)
				ctx := context.Background()
				if got, err := c.Fetch(ctx, "AAVEUSDT"); err != nil || len(got) != 0 || c.Skip("AAVEUSDT") {
					t.Fatalf("ordinary empty page: records=%v err=%v skip=%v", got, err, c.Skip("AAVEUSDT"))
				}
				got, err := c.Fetch(ctx, "AAVEUSDT")
				if err != nil || len(got) != 1 {
					t.Fatalf("unavailable: records=%v err=%v, want one no_data record", got, err)
				}
				got[0].RecvMono = 0 // Nondeterministic process clock.
				want := Record{Venue: "bitget", Channel: "gap", Symbol: "AAVEUSDT", RecvWall: 1795355100000000000,
					Payload: []byte(`{"channel":"` + tc.name + `","event":"no_data","symbol":"AAVEUSDT"}`)}
				if !reflect.DeepEqual(got[0], want) {
					t.Fatalf("record = %+v (%s), want %+v (%s)", got[0], got[0].Payload, want, want.Payload)
				}
				clock.t = clock.t.Add(time.Hour)
				if !c.Skip("AAVEUSDT") || c.Skip("BTCUSDT") {
					t.Fatal("cooldown must be scoped to the unavailable symbol")
				}
				if got, err := c.Fetch(ctx, "AAVEUSDT"); err != nil || len(got) != 0 || calls != 2 {
					t.Fatalf("hourly cooldown: records=%v err=%v calls=%d", got, err, calls)
				}
				clock.t = clock.t.Add(23 * time.Hour)
				if c.Skip("AAVEUSDT") {
					t.Fatal("cooldown did not expire after 24h")
				}
				if got, err := c.Fetch(ctx, "AAVEUSDT"); err != nil || len(got) != 0 || calls != 3 || !c.Skip("AAVEUSDT") {
					t.Fatalf("daily retry: records=%v err=%v calls=%d skip=%v", got, err, calls, c.Skip("AAVEUSDT"))
				}
				return
			}
			t.Fatalf("channel %s not found", tc.name)
		})
	}
}
