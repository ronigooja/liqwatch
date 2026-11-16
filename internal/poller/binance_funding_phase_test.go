package poller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestBinanceRegisteredHistoryClassification(t *testing.T) {
	_, channels, err := NewVenue("binance", PollConfig{}, Deps{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"exchangeInfo": false, "brackets": false, "constituents": false,
		"topLongShortPositionRatio": true, "topLongShortAccountRatio": true,
		"globalLongShortAccountRatio": true, "takerlongshortRatio": true,
		"openInterestHist": true, "fundingRate": true,
	}
	for _, c := range channels {
		history, ok := want[c.Name]
		if !ok || c.History != history {
			t.Errorf("%s History=%t, want %t (known=%t)", c.Name, c.History, history, ok)
		}
		delete(want, c.Name)
		if c.Name == "fundingRate" && (!c.Daily || c.Period != 24*time.Hour || c.RPS != 1 || c.Weight != 1) {
			t.Errorf("funding cadence/rates changed: %+v", c)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing channels: %v", want)
	}
}

func TestBinanceFundingStartupAndPersistedDailyPhases(t *testing.T) {
	for _, count := range []int{2, 520} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			ctx := context.Background()
			floorTime := time.Date(2026, 1, 1, 0, 6, 0, 0, time.UTC)
			floor := floorTime.UnixMilli()
			clock := &fakeClock{floorTime}
			sink := &memorySink{}
			fundingCalls := map[string]int{}
			snapshotCalls := map[string]int{}
			symbols := make([]string, count)
			for i := range symbols {
				symbols[i] = fmt.Sprintf("S%03dUSDT", i)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/fapi/v1/exchangeInfo":
					snapshotCalls["exchangeInfo"]++
					fmt.Fprint(w, `{"symbols":[`)
					for i, sym := range symbols {
						if i > 0 {
							fmt.Fprint(w, ",")
						}
						fmt.Fprintf(w, `{"symbol":%q,"contractType":"PERPETUAL","status":"TRADING","quoteAsset":"USDT"}`, sym)
					}
					fmt.Fprint(w, `]}`)
				case "/brackets":
					snapshotCalls["brackets"]++
					fmt.Fprint(w, `{"code":"0","message":"","messageDetail":"","success":true,"data":{"brackets":[{"symbol":"S000USDT","updateTime":null,"notionalLimit":1,"riskBrackets":[{"bracketSeq":1,"bracketNotionalFloor":0,"bracketNotionalCap":1,"bracketMaintenanceMarginRate":1,"cumFastMaintenanceAmount":1,"minOpenPosLeverage":1,"maxOpenPosLeverage":2}]}]}}`)
				case "/fapi/v1/fundingRate":
					sym := r.URL.Query().Get("symbol")
					fundingCalls[sym]++
					if r.URL.Query().Get("startTime") != strconv.FormatInt(floor, 10) || r.URL.Query().Get("limit") != "1000" || r.URL.Query().Get("endTime") != strconv.FormatInt(clock.Now().UnixMilli(), 10) {
						t.Errorf("unexpected funding range: %s", r.URL.RawQuery)
					}
					// Include an out-of-range row to exercise the real history wrapper's floor filter.
					fmt.Fprintf(w, `[{"symbol":%q,"fundingTime":%d,"fundingRate":"0.1"},{"symbol":%q,"fundingTime":%d,"fundingRate":"0.2"},{"symbol":%q,"fundingTime":%d,"fundingRate":"0.3"}]`, sym, floor-1, sym, floor, sym, floor+1)
				default:
					fmt.Fprint(w, `[]`)
				}
			}))
			defer server.Close()
			cfg := PollConfig{BaseURL: server.URL, BracketsURL: server.URL + "/brackets"}
			path := filepath.Join(t.TempDir(), "state.json")
			build := func() *Scheduler {
				// Recreate through the registered factory on restart, not synthetic channels.
				env, channels, err := NewVenue("binance", cfg, Deps{Clock: clock, Doer: server.Client(), Sink: sink})
				if err != nil {
					t.Fatal(err)
				}
				s, err := NewScheduler(channels, sink, clock, path, env.WeightPerMinute)
				if err != nil {
					t.Fatal(err)
				}
				env.Bind(s)
				return s
			}
			s := build()
			if err := s.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if len(fundingCalls) != 0 {
				t.Fatalf("startup fetched funding for %d/%d symbols", len(fundingCalls), count)
			}
			if snapshotCalls["exchangeInfo"] != 1 || snapshotCalls["brackets"] != 1 || len(s.Symbols()) != count {
				t.Fatalf("snapshot bootstrap changed: calls=%v symbols=%d", snapshotCalls, len(s.Symbols()))
			}
			if count != 2 {
				return
			}
			// Full daily group: brackets, exchangeInfo, funding/S000, funding/S001.
			// Dropped constituents are excluded; other history channels are non-daily.
			// Thus the funding deadlines are literal 12:05 and 18:05 UTC.
			for i, hour := range []int{12, 18} {
				phase := time.Date(2026, 1, 1, hour, 5, 0, 0, time.UTC)
				clock.t = phase.Add(-time.Nanosecond)
				s = build()
				if got := s.RecordingFloor("binance", "fundingRate", symbols[i]); got != floor {
					t.Fatalf("restart floor=%d, want %d", got, floor)
				}
				if err := s.RunOnce(ctx); err != nil {
					t.Fatal(err)
				}
				if len(fundingCalls) != i {
					t.Fatalf("funding ran before %s: %v", phase, fundingCalls)
				}
				clock.t = phase
				if err := s.RunOnce(ctx); err != nil {
					t.Fatal(err)
				}
				if len(fundingCalls) != i+1 || fundingCalls[symbols[i]] != 1 {
					t.Fatalf("funding missing at %s: %v", phase, fundingCalls)
				}
			}
			rows := map[string][]int64{}
			for _, r := range sink.records {
				if r.Channel == "fundingRate" {
					rows[r.Symbol] = append(rows[r.Symbol], r.Meta["last_row_timestamp"].(int64))
				}
			}
			for _, sym := range symbols {
				got := rows[sym]
				if len(got) != 2 || got[0] != floor || got[1] != floor+1 {
					t.Fatalf("%s funding rows=%v, want inclusive floor and post-floor only", sym, got)
				}
			}
			// Successful jobs also retain their next daily slot across a restart.
			clock.t = time.Date(2026, 1, 2, 12, 5, 0, 0, time.UTC).Add(-time.Nanosecond)
			s = build()
			if err := s.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if fundingCalls[symbols[0]] != 1 || fundingCalls[symbols[1]] != 1 {
				t.Fatalf("restart advanced daily phases: %v", fundingCalls)
			}
			clock.t = time.Date(2026, 1, 2, 12, 5, 0, 0, time.UTC)
			if err := s.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if fundingCalls[symbols[0]] != 2 || fundingCalls[symbols[1]] != 1 {
				t.Fatalf("restart lost next daily phase: %v", fundingCalls)
			}
		})
	}
}
