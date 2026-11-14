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
	e := &Env{Venue: "bitget", BaseURL: server.URL, Doer: uaDoer{server.Client()}, Clock: RealClock{}, RPS: 1, Periods: map[string]time.Duration{}, ChannelRPS: map[string]float64{}, Weights: map[string]float64{}}
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
	e := &Env{Venue: "bitget", BaseURL: server.URL, Doer: uaDoer{server.Client()}, Clock: clock, RPS: 1}
	c := bitgetChannels(e)[0]
	sink := &memorySink{}
	s, _ := NewScheduler([]Channel{c}, sink, clock, "", 240)
	e.Universe = func() []string { return []string{"BTCUSDT"} }
	e.Bind(s)
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.t = clock.t.Add(time.Hour)
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.records) != 42 {
		t.Fatalf("hourly records=%d, want 42", len(sink.records))
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
	wantStart := clock.Now().Add(-4*time.Hour + 5*time.Minute).UnixMilli()
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
	s, _ := NewScheduler(channels[:1], sink, clock, "", 240)
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
}
