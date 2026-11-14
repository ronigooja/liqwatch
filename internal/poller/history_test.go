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

func TestHistoryWindowWalkAndDedup(t *testing.T) {
	now := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	clock := &fakeClock{now}
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		start, _ := strconv.ParseInt(r.URL.Query().Get("startTime"), 10, 64)
		end, _ := strconv.ParseInt(r.URL.Query().Get("endTime"), 10, 64)
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		var rows []map[string]int64
		for ts := now.Add(-3 * 24 * time.Hour).UnixMilli(); ts <= now.UnixMilli(); ts += 300000 {
			if ts >= start && ts <= end {
				rows = append(rows, map[string]int64{"timestamp": ts})
			}
		}
		if len(rows) > limit {
			rows = rows[len(rows)-limit:]
		}
		json.NewEncoder(w).Encode(rows)
	}))
	defer server.Close()
	b := &Binance{Clock: clock, Doer: server.Client(), Config: PollConfig{BaseURL: server.URL, InitialLookback: "72h"}}
	channels := BinanceChannels(b)
	sink := &memorySink{}
	s, _ := NewScheduler(channels, sink, clock, "", 240)
	b.Scheduler = s
	rows, err := b.request(context.Background(), "openInterestHist", "BTCUSDT")
	if err != nil || len(rows) != 865 {
		t.Fatalf("rows=%d calls=%d err=%v", len(rows), calls, err)
	}
	for _, r := range rows {
		if s.dedup(&r) {
			t.Fatal("unexpected duplicate")
		}
	}
	for _, r := range rows {
		if !s.dedup(&r) {
			t.Fatal("duplicate missed")
		}
	}
	revised := rows[len(rows)-1]
	revised.Payload = []byte(fmt.Sprintf(`{"timestamp":%d,"value":1}`, now.UnixMilli()))
	if s.dedup(&revised) || revised.Meta["revised"] != true {
		t.Fatal("revision missed")
	}
}

func TestHistorySourceGap(t *testing.T) {
	now := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `[{"timestamp":%d},{"timestamp":%d}]`, now.Add(-15*time.Minute).UnixMilli(), now.Add(-5*time.Minute).UnixMilli())
	}))
	defer server.Close()
	b := &Binance{Clock: &fakeClock{now}, Doer: server.Client(), Config: PollConfig{BaseURL: server.URL, InitialLookback: "20m"}}
	rows, err := b.request(context.Background(), "takerlongshortRatio", "BTCUSDT")
	if err != nil || len(rows) != 3 || rows[2].Channel != "gap" {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	var gap struct {
		Kind, Channel, Symbol string
		Start                 int64 `json:"start_ms"`
		End                   int64 `json:"end_ms"`
	}
	if err := json.Unmarshal(rows[2].Payload, &gap); err != nil || gap.Kind != "source_gap" || gap.Channel != "takerlongshortRatio" || gap.Symbol != "BTCUSDT" || gap.Start != now.Add(-10*time.Minute).UnixMilli() || gap.End != now.Add(-5*time.Minute).UnixMilli() {
		t.Fatalf("gap=%+v err=%v", gap, err)
	}
}
