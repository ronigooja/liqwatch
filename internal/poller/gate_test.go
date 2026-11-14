package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
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
			if len(rows) > 2000 {
				rows = rows[len(rows)-2000:]
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
	if count != 3000 || gaps != 1 {
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

type gateClock struct{ now time.Time }

func (c gateClock) Now() time.Time                             { return c.now }
func (c gateClock) Sleep(context.Context, time.Duration) error { return nil }
