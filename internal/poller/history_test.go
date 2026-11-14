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

func TestInitialLookbackConfig(t *testing.T) {
	now := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, lookback string
		state          string
		want           time.Duration
	}{{"missing", "", "missing", 2 * time.Hour}, {"corrupt", "", "bad", 2 * time.Hour}, {"override", "72h", "missing", 72 * time.Hour}, {"clamped", "1000h", "missing", 29 * 24 * time.Hour}} {
		t.Run(tc.name, func(t *testing.T) {
			var first int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if first == 0 {
					first, _ = strconv.ParseInt(r.URL.Query().Get("startTime"), 10, 64)
				}
				fmt.Fprint(w, `[]`)
			}))
			defer server.Close()
			p := filepath.Join(t.TempDir(), "state.json")
			if tc.state == "bad" {
				if err := os.WriteFile(p, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			clock := &fakeClock{now}
			b := &Binance{Clock: clock, Doer: server.Client(), Config: PollConfig{BaseURL: server.URL, InitialLookback: tc.lookback}}
			s, err := NewScheduler(BinanceChannels(b), &memorySink{}, clock, p, 240)
			if err != nil {
				t.Fatal(err)
			}
			b.Scheduler = s
			if _, err = b.request(context.Background(), "openInterestHist", "BTCUSDT"); err != nil {
				t.Fatal(err)
			}
			if first != now.Add(-tc.want).UnixMilli() {
				t.Fatalf("start=%d want=%d", first, now.Add(-tc.want).UnixMilli())
			}
		})
	}
}

func TestRestartDedupAcrossDaysAndTruncatedTail(t *testing.T) {
	now := time.Date(2026, 1, 11, 0, 2, 0, 0, time.UTC)
	clock := &fakeClock{now}
	dir := t.TempDir()
	sink := NewFileSink(dir, time.Hour)
	old := Record{Venue: "binance", Symbol: "BTCUSDT", Channel: "openInterestHist", RecvWall: now.Add(-10 * time.Minute).UnixNano(), Meta: map[string]any{"last_row_timestamp": now.Add(-10 * time.Minute).UnixMilli()}, Payload: []byte(fmt.Sprintf(`{"timestamp":%d}`, now.Add(-10*time.Minute).UnixMilli()))}
	recent := old
	recent.RecvWall = now.UnixNano()
	recent.Meta = map[string]any{"last_row_timestamp": now.UnixMilli()}
	recent.Payload = []byte(fmt.Sprintf(`{"timestamp":%d}`, now.UnixMilli()))
	sink.Write(old)
	sink.Write(recent)
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "binance", "BTCUSDT", now.UTC().Format("20060102"), "openInterestHist.p.jsonl")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(f, `{"p":`)
	f.Close()
	sink = NewFileSink(dir, time.Hour)
	defer sink.Close()
	s, _ := NewScheduler(nil, sink, clock, "", 240)
	for _, r := range []Record{old, recent} {
		if !s.dedup(&r) {
			t.Fatalf("missed duplicate %s", r.Payload)
		}
	}
	changed := recent
	changed.Payload = []byte(fmt.Sprintf(`{"timestamp":%d,"v":2}`, now.UnixMilli()))
	changed.Meta = map[string]any{"last_row_timestamp": now.UnixMilli()}
	if s.dedup(&changed) || changed.Meta["revised"] != true {
		t.Fatal("revision not marked")
	}
	if !s.dedup(&changed) {
		t.Fatal("revision duplicate not dropped")
	}
	missing := recent
	missing.Symbol = "ETHUSDT"
	if s.dedup(&missing) {
		t.Fatal("missing file generated duplicate")
	}
}

func TestHistoryOutagesAndState(t *testing.T) {
	now := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)
	clock := &fakeClock{now}
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		start, _ := strconv.ParseInt(r.URL.Query().Get("startTime"), 10, 64)
		end, _ := strconv.ParseInt(r.URL.Query().Get("endTime"), 10, 64)
		var rows []map[string]int64
		for ts := (start + 299999) / 300000 * 300000; ts <= end; ts += 300000 {
			rows = append(rows, map[string]int64{"timestamp": ts})
		}
		if len(rows) > 500 {
			rows = rows[len(rows)-500:]
		}
		json.NewEncoder(w).Encode(rows)
	}))
	defer server.Close()
	for _, days := range []int{10, 35} {
		t.Run(fmt.Sprint(days), func(t *testing.T) {
			clock.t = now
			requests = 0
			sink := &memorySink{}
			b := &Binance{Clock: clock, Doer: server.Client(), Config: PollConfig{BaseURL: server.URL}}
			s, _ := NewScheduler(BinanceChannels(b), sink, clock, "", 240)
			b.Scheduler = s
			cursor := now.Add(-time.Duration(days) * 24 * time.Hour).UnixMilli()
			s.state.Jobs["binance/openInterestHist/BTCUSDT"] = jobState{RowTimestamp: cursor}
			rows, err := b.request(context.Background(), "openInterestHist", "BTCUSDT")
			if err != nil {
				t.Fatal(err)
			}
			expected := days*288 + 25
			if days == 35 {
				expected = 29*288 + 1
			}
			if len(rows) != expected {
				t.Fatalf("rows %d expected %d", len(rows), expected)
			}
			seen := map[int64]bool{}
			for _, r := range rows {
				ts := r.Meta["last_row_timestamp"].(int64)
				if seen[ts] {
					t.Fatal("duplicate")
				}
				seen[ts] = true
			}
			if days == 35 {
				if len(sink.records) != 1 {
					t.Fatalf("gaps=%d", len(sink.records))
				}
				var g struct {
					Event string
					Start int64 `json:"start_ms"`
					End   int64 `json:"end_ms"`
				}
				json.Unmarshal(sink.records[0].Payload, &g)
				if g.Event != "retention_gap" || g.Start != cursor || g.End != now.Add(-29*24*time.Hour).UnixMilli() {
					t.Fatalf("gap=%+v", g)
				}
			}
			if requests < 7 {
				t.Fatalf("too few windows %d", requests)
			}
		})
	}
	for _, contents := range []string{"missing", "{broken"} {
		p := filepath.Join(t.TempDir(), "state.json")
		if contents != "missing" {
			os.WriteFile(p, []byte(contents), 0600)
		}
		s, err := NewScheduler(nil, &memorySink{}, clock, p, 240)
		if err != nil || s.RowTimestamp("binance", "openInterestHist", "BTCUSDT") != 0 {
			t.Fatal(contents, err)
		}
	}
}

func TestFundingPagination(t *testing.T) {
	now := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	clock := &fakeClock{now}
	base := now.Add(-1500 * time.Hour).UnixMilli()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		start, _ := strconv.ParseInt(r.URL.Query().Get("startTime"), 10, 64)
		end, _ := strconv.ParseInt(r.URL.Query().Get("endTime"), 10, 64)
		rows := []map[string]int64{}
		for i := int64(0); i < 1501; i++ {
			ts := base + i*int64(time.Hour/time.Millisecond)
			if ts >= start && ts <= end {
				rows = append(rows, map[string]int64{"fundingTime": ts})
			}
		}
		if len(rows) > 1000 {
			rows = rows[:1000]
		}
		json.NewEncoder(w).Encode(rows)
	}))
	defer server.Close()
	b := &Binance{Clock: clock, Doer: server.Client(), Config: PollConfig{BaseURL: server.URL}}
	s, _ := NewScheduler(BinanceChannels(b), &memorySink{}, clock, "", 240)
	b.Scheduler = s
	s.state.Jobs["binance/fundingRate/BTCUSDT"] = jobState{RowTimestamp: base}
	rows, err := b.request(context.Background(), "fundingRate", "BTCUSDT")
	if err != nil || len(rows) != 1501 || calls != 2 {
		t.Fatalf("rows=%d calls=%d err=%v", len(rows), calls, err)
	}
}

func TestRestartForcesDueOverlap(t *testing.T) {
	now := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	clock := &fakeClock{now}
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		fmt.Fprintf(w, `[{"timestamp":%d}]`, now.Add(-5*time.Minute).UnixMilli())
	}))
	defer server.Close()
	for i := 0; i < 2; i++ {
		sink := NewFileSink(dir, time.Hour)
		b := &Binance{Clock: clock, Doer: server.Client(), Config: PollConfig{BaseURL: server.URL, InitialLookback: "1h", Periods: map[string]string{"openInterestHist": "1m"}}}
		var channels []Channel
		for _, c := range BinanceChannels(b) {
			if c.Name == "openInterestHist" {
				channels = append(channels, c)
			}
		}
		s, err := NewScheduler(channels, sink, clock, statePath, 240)
		if err != nil {
			t.Fatal(err)
		}
		s.SetSymbols([]string{"BTCUSDT"})
		b.Scheduler = s
		if err = s.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if i == 1 && s.Duplicates.Load() != 1 {
			t.Fatalf("duplicates %d", s.Duplicates.Load())
		}
		if err = sink.Close(); err != nil {
			t.Fatal(err)
		}
		clock.t = clock.t.Add(time.Minute)
	}
	if calls != 2 {
		t.Fatalf("requests %d", calls)
	}
}
