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
