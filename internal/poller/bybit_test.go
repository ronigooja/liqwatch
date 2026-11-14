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

func TestBybitPagingAndSnapshots(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	var ends []int64
	fixture := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join("testdata/bybit", name+"-fixture.json"))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.UserAgent() != DefaultUserAgent {
			t.Errorf("missing user agent: %q", r.UserAgent())
		}
		q := r.URL.Query()
		if q.Get("category") != "linear" {
			t.Error(q)
		}
		switch r.URL.Path {
		case "/v5/market/account-ratio":
			if q.Get("period") != "5min" || q.Get("limit") != "500" {
				t.Error(q)
			}
			if q.Get("symbol") == "BAD" {
				fmt.Fprint(w, `{"retCode":10001,"retMsg":"params error: symbol not support"}`)
				return
			}
			if q.Get("symbol") == "EMPTY" {
				fmt.Fprint(w, `{"retCode":0,"result":{"list":[]}}`)
				return
			}
			end, _ := strconv.ParseInt(q.Get("endTime"), 10, 64)
			start, _ := strconv.ParseInt(q.Get("startTime"), 10, 64)
			ends = append(ends, end)
			rows := []json.RawMessage{}
			for i := 0; i < 1201; i++ {
				ts := now.UnixMilli() - int64(i)*300000
				if ts >= start && ts <= end && len(rows) < 500 {
					rows = append(rows, json.RawMessage(fmt.Sprintf(`{"symbol":"BTCUSDT","timestamp":"%d"}`, ts)))
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"retCode": 0, "result": map[string]any{"list": rows}})
		case "/v5/market/risk-limit", "/v5/market/instruments-info":
			name := "risk"
			if strings.Contains(r.URL.Path, "instruments") {
				name = "instruments"
				if q.Get("limit") != "1000" {
					t.Error(q)
				}
			}
			var v map[string]any
			json.Unmarshal(fixture(name), &v)
			result := v["result"].(map[string]any)
			if q.Get("cursor") == "" {
				result["nextPageCursor"] = "next%3D1"
			} else {
				if q.Get("cursor") != "next%3D1" {
					t.Error(q)
				}
				result["nextPageCursor"] = ""
			}
			json.NewEncoder(w).Encode(v)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	mapPath := filepath.Join(t.TempDir(), "map.toml")
	os.WriteFile(mapPath, []byte("[[pair]]\nbinance='BTCUSDT'\nvenue='bybit'\nsymbol='BTCUSDT'\nexp10=0\nstatus='auto'\n\n[[pair]]\nbinance='ETHUSDT'\nvenue='bybit'\nsymbol='ETHUSDT'\nexp10=0\nstatus='auto'\n"), 0600)
	env, cs, err := NewVenue("bybit", PollConfig{MapPath: mapPath, InitialLookback: "101h", Venues: map[string]VenueConfig{"bybit": {BaseURL: server.URL}}}, Deps{Clock: &fakeClock{now}, Selected: []string{"BTCUSDT"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(env.Universe()) != 1 || env.Universe()[0] != "BTCUSDT" || env.RPS != 5 || env.WeightPerMinute != 600 {
		t.Fatal(env.Universe(), env.RPS, env.WeightPerMinute)
	}
	records, err := cs[0].Fetch(context.Background(), "BTCUSDT")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1201 || len(ends) != 4 {
		t.Fatalf("rows %d ends %v", len(records), ends)
	}
	if ends[1] != now.UnixMilli()-499*300000-1 || ends[2] != now.UnixMilli()-999*300000-1 {
		t.Fatal(ends)
	}
	for _, sym := range []string{"BAD", "EMPTY"} {
		rows, err := cs[0].Fetch(context.Background(), sym)
		if err != nil || len(rows) != 0 {
			t.Fatal(sym, rows, err)
		}
	}
	for _, c := range cs[1:] {
		rows, err := c.Fetch(context.Background(), "")
		if err != nil || len(rows) != 2 {
			t.Fatal(c.Name, len(rows), err)
		}
	}
}

func TestBybitDailyOverlapAndShortPage(t *testing.T) {
	for _, short := range []bool{false, true} {
		t.Run(fmt.Sprintf("short=%v", short), func(t *testing.T) {
			now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
			clock := &fakeClock{now}
			calls := 0
			firstPage := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				q := r.URL.Query()
				start, _ := strconv.ParseInt(q.Get("startTime"), 10, 64)
				end, _ := strconv.ParseInt(q.Get("endTime"), 10, 64)
				rows := []json.RawMessage{}
				// The server caps time slots, so holes produce a short first page.
				for ts, slots := end-end%300000, 0; ts >= start && slots < 500; ts, slots = ts-300000, slots+1 {
					if short && ts <= now.UnixMilli()-300000 && ts >= now.UnixMilli()-900000 {
						continue
					}
					rows = append(rows, json.RawMessage(fmt.Sprintf(`{"symbol":"BTCUSDT","timestamp":"%d"}`, ts)))
				}
				if calls == 1 {
					firstPage = len(rows)
				}
				json.NewEncoder(w).Encode(map[string]any{"retCode": 0, "result": map[string]any{"list": rows}})
			}))
			defer server.Close()
			path := filepath.Join(t.TempDir(), "map.toml")
			if err := os.WriteFile(path, []byte("[[pair]]\nbinance='BTCUSDT'\nvenue='bybit'\nsymbol='BTCUSDT'\nexp10=0\nstatus='auto'\n"), 0600); err != nil {
				t.Fatal(err)
			}
			sink := &memorySink{}
			env, channels, err := NewVenue("bybit", PollConfig{MapPath: path, InitialLookback: "72h", Venues: map[string]VenueConfig{"bybit": {BaseURL: server.URL}}}, Deps{Clock: clock, Sink: sink})
			if err != nil {
				t.Fatal(err)
			}
			s, err := NewScheduler(channels[:1], sink, clock, "", env.WeightPerMinute)
			if err != nil {
				t.Fatal(err)
			}
			env.Bind(s)
			if _, err := s.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			wantFirst := 500
			if short {
				wantFirst = 497
			}
			if firstPage != wantFirst || calls != 2 {
				t.Fatalf("first page=%d calls=%d", firstPage, calls)
			}
			seen := map[int64]bool{}
			gaps := 0
			for _, r := range sink.records {
				if r.Channel == "gap" {
					gaps++
					continue
				}
				seen[r.Meta["last_row_timestamp"].(int64)] = true
			}
			for ts := now.Add(-72 * time.Hour).UnixMilli(); ts <= now.UnixMilli(); ts += 300000 {
				hole := short && ts <= now.UnixMilli()-300000 && ts >= now.UnixMilli()-900000
				if seen[ts] == hole {
					t.Fatalf("timestamp %d present=%v hole=%v", ts, seen[ts], hole)
				}
			}
			if short && gaps != 1 || !short && gaps != 0 {
				t.Fatalf("gaps=%d", gaps)
			}
			if short {
				return
			}
			clock.t = now.Add(24 * time.Hour)
			if _, err := s.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			if calls != 3 || s.Duplicates.Load() != 25 {
				t.Fatalf("total calls=%d duplicates=%d", calls, s.Duplicates.Load())
			}
			counts := map[int64]int{}
			for _, r := range sink.records {
				if r.Channel == "accountRatio" {
					counts[r.Meta["last_row_timestamp"].(int64)]++
				}
			}
			if len(counts) != 865+288 {
				t.Fatalf("unique rows=%d", len(counts))
			}
			for ts, n := range counts {
				if n != 1 {
					t.Fatalf("duplicate %d: %d", ts, n)
				}
			}
		})
	}
}

func TestBybitNoDataRetryDaily(t *testing.T) {
	for _, body := range []string{`{"retCode":10001,"retMsg":"params error: symbol not support"}`, `{"retCode":0,"result":{"list":[]}}`} {
		t.Run(body, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; fmt.Fprint(w, body) }))
			defer server.Close()
			path := filepath.Join(t.TempDir(), "map.toml")
			if err := os.WriteFile(path, []byte("[[pair]]\nbinance='BTCUSDT'\nvenue='bybit'\nsymbol='BTCUSDT'\nexp10=0\nstatus='auto'\n"), 0600); err != nil {
				t.Fatal(err)
			}
			clock := &fakeClock{time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)}
			sink := &memorySink{}
			env, channels, err := NewVenue("bybit", PollConfig{MapPath: path, Venues: map[string]VenueConfig{"bybit": {BaseURL: server.URL, Periods: map[string]string{"accountRatio": "5m"}}}}, Deps{Clock: clock, Sink: sink})
			if err != nil {
				t.Fatal(err)
			}
			s, err := NewScheduler(channels[:1], sink, clock, "", env.WeightPerMinute)
			if err != nil {
				t.Fatal(err)
			}
			env.Bind(s)
			step := func() {
				t.Helper()
				if _, err := s.Step(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			step()
			if calls != 1 || len(sink.records) != 1 || !strings.Contains(string(sink.records[0].Payload), `"no_data"`) {
				t.Fatalf("calls=%d records=%v", calls, sink.records)
			}
			clock.t = clock.t.Add(23 * time.Hour)
			step()
			if calls != 1 {
				t.Fatalf("early retry calls=%d", calls)
			}
			clock.t = clock.t.Add(time.Hour)
			step()
			if calls != 2 || len(sink.records) != 1 {
				t.Fatalf("calls=%d records=%d", calls, len(sink.records))
			}
		})
	}
}
