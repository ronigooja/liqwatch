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
	if len(records) != 1201 || len(ends) != 3 {
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
