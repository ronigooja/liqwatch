package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestGateHistoryAndSnapshots(t *testing.T) {
	now := time.Unix(1800000000, 0).UTC()
	now = now.Truncate(time.Minute)
	requests := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.UserAgent() == "" {
			t.Error("missing User-Agent")
		}
		requests[r.URL.Path]++
		switch r.URL.Path {
		case "/futures/usdt/contract_stats":
			if r.URL.Query().Get("contract") != "龙虾_USDT" {
				t.Error("contract encoding")
			}
			if r.URL.Query().Get("interval") != "1m" || r.URL.Query().Get("limit") != "2000" {
				t.Error("history parameters")
			}
			end, _ := strconv.ParseInt(r.URL.Query().Get("to"), 10, 64)
			rows := []map[string]int64{}
			for i := 4000; i >= 0; i-- {
				ts := now.Unix() - int64(i)*60
				if ts <= end && i != 2100 {
					rows = append(rows, map[string]int64{"time": ts})
				}
			}
			if len(rows) > 1997 {
				rows = rows[len(rows)-1997:]
			}
			json.NewEncoder(w).Encode(rows)
		case "/futures/usdt/risk_limit_tiers":
			offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
			rows := []map[string]any{}
			if offset == 0 {
				for i := 0; i < 100; i++ {
					rows = append(rows, map[string]any{"contract": fmt.Sprintf("C%d", i), "tier": 1})
				}
			} else if offset == 100 {
				rows = append(rows, map[string]any{"contract": "龙虾_USDT", "tier": 1})
			}
			json.NewEncoder(w).Encode(rows)
		case "/futures/usdt/contracts":
			fmt.Fprint(w, `[{"name":"龙虾_USDT"}]`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	e := &Env{Venue: "gate", BaseURL: srv.URL, Doer: uaDoer{srv.Client()}, Clock: gateClock{now}, Lookback: 3000 * time.Minute, Overlap: time.Hour, Periods: map[string]time.Duration{}, ChannelRPS: map[string]float64{}, Weights: map[string]float64{}, Config: PollConfig{}}
	channels := gateChannels(e)
	// Explicit scope for the pagination fixture: all response symbols are selected.
	e.Universe = func() []string {
		symbols := []string{"龙虾_USDT"}
		for i := 0; i < 100; i++ {
			symbols = append(symbols, fmt.Sprintf("C%d", i))
		}
		return symbols
	}
	recs, err := channels[0].Fetch(context.Background(), "龙虾_USDT")
	if err != nil {
		t.Fatal(err)
	}
	count, gaps := 0, 0
	for _, r := range recs {
		if r.Channel == "gap" {
			gaps++
		} else {
			count++
		}
	}
	if count != 2998 || gaps != 1 {
		t.Fatalf("history rows=%d gaps=%d", count, gaps)
	}
	if requests["/futures/usdt/contract_stats"] != 2 {
		t.Fatal("expected two history pages")
	}
	tiers, err := channels[1].Fetch(context.Background(), "")
	if err != nil || len(tiers) != 101 {
		t.Fatalf("tiers=%d err=%v", len(tiers), err)
	}
	if tiers[100].Symbol != "龙虾_USDT" {
		t.Fatal("missing Chinese tier")
	}
	contracts, err := channels[2].Fetch(context.Background(), "")
	if err != nil || len(contracts) != 1 {
		t.Fatalf("contracts=%d err=%v", len(contracts), err)
	}
}

func TestGateNoDataAndOverlap(t *testing.T) {
	clock := &fakeClock{time.Unix(1800000000, 0).UTC().Truncate(time.Minute)}
	requests := 0
	missing := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.UserAgent() != DefaultUserAgent {
			t.Errorf("user agent %q", r.UserAgent())
		}
		if missing {
			w.WriteHeader(400)
			fmt.Fprint(w, `{"label":"CONTRACT_NOT_FOUND"}`)
			return
		}
		end, _ := strconv.ParseInt(r.URL.Query().Get("to"), 10, 64)
		rows := []map[string]int64{}
		for i := 3; i >= 0; i-- {
			ts := clock.t.Unix() - int64(i)*60
			if ts <= end {
				rows = append(rows, map[string]int64{"time": ts})
			}
		}
		json.NewEncoder(w).Encode(rows)
	}))
	defer srv.Close()
	sink := &memorySink{}
	e := &Env{Venue: "gate", BaseURL: srv.URL, Doer: uaDoer{srv.Client()}, Clock: clock, Lookback: 2 * time.Minute, Overlap: time.Hour, Periods: map[string]time.Duration{}, ChannelRPS: map[string]float64{}, Weights: map[string]float64{}}
	c := gateChannels(e)[0]
	c.Period = time.Minute
	s, err := historicalScheduler([]Channel{c}, sink, clock, "", 240)
	if err != nil {
		t.Fatal(err)
	}
	e.Bind(s)
	s.Universe = func() []string { return []string{"MISSING_USDT"} }
	if _, err = s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.records) != 1 || !strings.Contains(string(sink.records[0].Payload), `"no_data"`) || requests != 1 {
		t.Fatalf("no_data records=%d requests=%d", len(sink.records), requests)
	}
	clock.t = clock.t.Add(time.Hour)
	if _, err = s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("premature retry: %d", requests)
	}
	clock.t = clock.t.Add(24 * time.Hour)
	if _, err = s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests != 2 || len(sink.records) != 1 {
		t.Fatalf("daily retry requests=%d records=%d", requests, len(sink.records))
	}
	missing = false
	clock.t = clock.t.Add(24 * time.Hour)
	if _, err = s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := len(sink.records)
	clock.t = clock.t.Add(time.Minute)
	if _, err = s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.Duplicates.Load() == 0 || len(sink.records) <= first {
		t.Fatalf("dedup=%d first=%d total=%d", s.Duplicates.Load(), first, len(sink.records))
	}
}

func TestGateUniverse(t *testing.T) {
	p := filepath.Join(t.TempDir(), "map.toml")
	body := ""
	for _, x := range []struct{ bin, sym, status string }{{"BTCUSDT", "BTC_USDT", "auto"}, {"LOBSTERUSDT", "龙虾_USDT", "reviewed"}, {"BADUSDT", "BAD_USDT", "rejected"}} {
		body += fmt.Sprintf("[[pair]]\nbinance=%q\nvenue='gate'\nsymbol=%q\nexp10=0\nstatus=%q\n", x.bin, x.sym, x.status)
	}
	if err := os.WriteFile(p, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	e, _, err := NewVenue("gate", PollConfig{MapPath: p}, Deps{Selected: []string{"龙虾_USDT", "BAD_USDT"}})
	if err != nil {
		t.Fatal(err)
	}
	if u := e.Universe(); len(u) != 1 || u[0] != "龙虾_USDT" {
		t.Fatalf("universe %v", u)
	}
}

func TestGateRetriesIgnoredEnd(t *testing.T) {
	now := time.Unix(1800000000, 0).UTC().Truncate(time.Minute)
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		end, _ := strconv.ParseInt(r.URL.Query().Get("to"), 10, 64)
		if calls == 1 {
			fmt.Fprintf(w, `[{"time":%d}]`, end+60)
			return
		}
		fmt.Fprintf(w, `[{"time":%d}]`, end-end%60)
	}))
	defer srv.Close()
	e := &Env{Venue: "gate", BaseURL: srv.URL, Doer: uaDoer{srv.Client()}, Clock: gateClock{now}, Lookback: time.Minute, Periods: map[string]time.Duration{}, ChannelRPS: map[string]float64{}, Weights: map[string]float64{}}
	_, err := gateChannels(e)[0].Fetch(context.Background(), "BTC_USDT")
	if err != nil || calls != 2 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

type gateClock struct{ now time.Time }

func (c gateClock) Now() time.Time                             { return c.now }
func (c gateClock) Sleep(context.Context, time.Duration) error { return nil }
