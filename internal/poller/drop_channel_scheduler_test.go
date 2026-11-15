package poller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Expectations are independent of the policy table: removing a drop entry must fail.
var disabledRESTChannels = map[string][]string{
	"binance": {"constituents", "takerlongshortRatio"},
	"bitget":  {"takerBuySell"},
	"okx":     {"takerVolumeContract"},
}

func TestRegisteredRESTDropChannels(t *testing.T) {
	got := map[string][]string{}
	for _, venue := range Venues() {
		e := &Env{Venue: venue, Clock: &fakeClock{time.Now()}, RPS: 1}
		for _, c := range registry[venue].factory(e) {
			if channelDropped(c.Venue, c.Name) {
				got[venue] = append(got[venue], c.Name)
			}
		}
		sort.Strings(got[venue])
	}
	// Do not add empty venue entries to the expected disabled set.
	for venue, channels := range got {
		if len(channels) == 0 {
			delete(got, venue)
		}
	}
	if !reflect.DeepEqual(got, disabledRESTChannels) {
		t.Fatalf("disabled registered channels = %v, want %v", got, disabledRESTChannels)
	}
}

func TestDroppedRESTChannelsNeverRequestThroughScheduler(t *testing.T) {
	for _, venue := range []string{"binance", "bitget", "okx"} {
		for _, once := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/once=%t", venue, once), func(t *testing.T) {
				var mu sync.Mutex
				requests := map[string]int{}
				supported, path, body := "exchangeInfo", "/fapi/v1/exchangeInfo", `{"symbols":[{"symbol":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING","quoteAsset":"USDT"}]}`
				if venue == "bitget" {
					supported, path, body = "contracts", "/api/v2/mix/market/contracts", `{"code":"00000","data":[]}`
				} else if venue == "okx" {
					supported, path, body = "instruments", "/api/v5/public/instruments", `{"code":"0","data":[]}`
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					requests[r.URL.Path]++
					mu.Unlock()
					if r.URL.Path == path {
						fmt.Fprint(w, body)
					} else if r.URL.Path == "/unknown" {
						fmt.Fprint(w, `{"unclassified":true}`)
					} else {
						w.WriteHeader(http.StatusTooManyRequests)
						fmt.Fprint(w, `{}`)
					}
				}))
				defer server.Close()
				clock := &fakeClock{time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
				sink := &memorySink{}
				cfg := PollConfig{MapPath: writeMap(t), Venues: map[string]VenueConfig{venue: {BaseURL: server.URL}}}
				env, registered, err := NewVenue(venue, cfg, Deps{Doer: server.Client(), Clock: clock, Sink: sink})
				if err != nil {
					t.Fatal(err)
				}
				var channels []Channel
				for _, c := range registered {
					if hasString(disabledRESTChannels[venue], c.Name) {
						channels = append(channels, c)
					}
				}
				if len(channels) != len(disabledRESTChannels[venue]) {
					t.Fatalf("missing disabled registrations: %v", channels)
				}
				for _, c := range registered {
					if c.Name == supported {
						channels = append(channels, c)
					}
				}
				// An unregistered policy must still request, write unchanged and advance its cursor.
				channels = append(channels, env.Channel("unknown-rest-policy", ChannelSpec{Period: time.Hour, Fetch: func(ctx context.Context, sym string) ([]Record, error) {
					resp, err := env.Get(ctx, "/unknown", nil)
					if err != nil {
						return nil, err
					}
					return []Record{{Venue: venue, Channel: "unknown-rest-policy", Payload: resp.Body, Meta: map[string]any{"last_row_timestamp": int64(123)}}}, nil
				}}))
				statePath := filepath.Join(t.TempDir(), "state.json")
				s, err := NewScheduler(channels, sink, clock, statePath, 240)
				if err != nil {
					t.Fatal(err)
				}
				env.Bind(s)
				s.Universe = func() []string { return []string{"BTCUSDT", "ETHUSDT"} }
				// Existing cursors and retry queues must not revive disabled work.
				preserved := map[string]jobState{}
				for _, c := range channels[:len(disabledRESTChannels[venue])] {
					key := jobKey(c, "ETHUSDT")
					st := jobState{LastSuccess: clock.t.Add(-48 * time.Hour).UnixNano(), RowTimestamp: 456, NoDataAt: 789}
					s.state.Jobs[key], preserved[key] = st, st
					s.failures[key] = &failure{attempts: 3, since: clock.t.Add(-time.Hour), next: clock.t.Add(-time.Second)}
				}
				run := func() {
					t.Helper()
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					if once {
						if err := s.RunOnce(ctx); err != nil {
							t.Fatal(err)
						}
					} else {
						for i := 0; i < 6; i++ {
							if _, err := s.Step(ctx); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
				run()
				// Restart from the persisted checkpoint: no startup burst or cursor mutation.
				s, err = NewScheduler(channels, sink, clock, statePath, 240)
				if err != nil {
					t.Fatal(err)
				}
				env.Bind(s)
				s.Universe = func() []string { return []string{"BTCUSDT", "ETHUSDT"} }
				run()
				mu.Lock()
				defer mu.Unlock()
				if len(requests) != 2 || requests[path] != 1 || requests["/unknown"] != 1 {
					t.Fatalf("HTTP requests = %v; disabled requests must be zero", requests)
				}
				if len(sink.records) != 2 || sink.records[0].Channel != supported || string(sink.records[1].Payload) != `{"unclassified":true}` {
					t.Fatalf("output = %+v", sink.records)
				}
				if s.RowTimestamp(venue, "unknown-rest-policy", "") != 123 {
					t.Fatal("supported cursor did not survive restart")
				}
				for key, st := range preserved {
					if s.state.Jobs[key] != st {
						t.Fatalf("disabled checkpoint changed: %s", key)
					}
				}
				for key := range s.state.Jobs {
					if strings.HasSuffix(key, "/BTCUSDT") {
						t.Fatalf("initial disabled job created checkpoint: %s", key)
					}
				}
			})
		}
	}
}

func TestDropChannelSchedulingHonorsPolicyTestBypass(t *testing.T) {
	withoutPolicies(t)
	for venue, channels := range disabledRESTChannels {
		for _, channel := range channels {
			if channelDropped(venue, channel) {
				t.Fatalf("policy bypass still disables %s/%s", venue, channel)
			}
		}
	}
}
