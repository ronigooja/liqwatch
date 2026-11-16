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

// Exercise registered producers through the scheduler and real file sink, not a
// second implementation of their scope predicate.
func TestRegisteredSplitSnapshotScope(t *testing.T) {
	for _, venue := range []string{"binance", "gate"} {
		for _, selection := range []string{"all", "subset", "empty"} {
			t.Run(venue+"/"+selection, func(t *testing.T) {
				clock := &fakeClock{time.Date(2026, 11, 16, 1, 0, 0, 0, time.UTC)}
				calls := map[string]int{}
				brackets := []string{}
				for _, sym := range []string{"BTCUSDT", "ETHUSDT", "DELISTEDUSDT", "TRADIFIUSDT"} {
					brackets = append(brackets, fmt.Sprintf(`{"symbol":%q,"updateTime":123,"notionalLimit":1.2300,"riskBrackets":[{"bracketSeq":1,"bracketNotionalFloor":0,"bracketNotionalCap":2,"bracketMaintenanceMarginRate":3,"cumFastMaintenanceAmount":4,"minOpenPosLeverage":5,"maxOpenPosLeverage":6}]}`, sym))
				}
				bulk := `[{"name":"BTC_USDT"},{"name":"TRADIFI_USDT"}]`
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls[r.URL.Path]++
					if r.URL.Query().Get("symbol") != "" || r.URL.Query().Get("contract") != "" {
						t.Error("bulk request acquired symbol parameter")
					}
					switch r.URL.Path {
					case "/fapi/v1/exchangeInfo":
						fmt.Fprint(w, `{"symbols":[{"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","quoteAsset":"USDT"},{"symbol":"ETHUSDT","status":"TRADING","contractType":"PERPETUAL","quoteAsset":"USDT"},{"symbol":"DELISTEDUSDT","status":"SETTLING","contractType":"PERPETUAL","quoteAsset":"USDT"}]}`)
					case "/brackets":
						fmt.Fprintf(w, `{"code":"0","message":"","messageDetail":"","success":true,"data":{"brackets":[%s]}}`, strings.Join(brackets, ","))
					case "/futures/usdt/risk_limit_tiers":
						if r.URL.Query().Get("limit") != "1000" || r.URL.Query().Get("offset") != "0" {
							t.Error("changed Gate pagination")
						}
						fmt.Fprint(w, `[{"contract":"BTC_USDT","tier":1,"risk_limit":1.2300},{"contract":"ETH_USDT","tier":1,"risk_limit":2},{"contract":"DELISTED_USDT","tier":1},{"contract":"TRADIFI_USDT","tier":1}]`)
					case "/futures/usdt/contracts":
						fmt.Fprint(w, bulk)
					default:
						t.Errorf("unexpected request %s", r.URL)
					}
				}))
				defer srv.Close()
				mapPath := filepath.Join(t.TempDir(), "map.toml")
				if err := os.WriteFile(mapPath, []byte("[[pair]]\nbinance='BTCUSDT'\nvenue='gate'\nsymbol='BTC_USDT'\nexp10=0\nstatus='auto'\n[[pair]]\nbinance='ETHUSDT'\nvenue='gate'\nsymbol='ETH_USDT'\nexp10=0\nstatus='reviewed'\n[[pair]]\nbinance='DELISTEDUSDT'\nvenue='gate'\nsymbol='DELISTED_USDT'\nexp10=0\nstatus='rejected'\n"), 0600); err != nil {
					t.Fatal(err)
				}
				selected := []string(nil)
				want := []string{"BTCUSDT", "ETHUSDT"}
				channel := "brackets"
				if venue == "gate" {
					want = []string{"BTC_USDT", "ETH_USDT"}
					channel = "riskLimitTiers"
				}
				if selection == "subset" {
					selected = []string{want[0], "DELISTEDUSDT", "DELISTED_USDT", "TRADIFIUSDT", "TRADIFI_USDT"}
					want = want[:1]
				}
				if selection == "empty" {
					selected = []string{"ABSENT"}
					want = nil
				}
				dir := t.TempDir()
				fileSink := NewFileSink(dir, time.Hour)
				defer fileSink.Close()
				capture := &scopeCapture{Sink: fileSink}
				e, channels, err := NewVenue(venue, PollConfig{MapPath: mapPath, BaseURL: srv.URL, BracketsURL: srv.URL + "/brackets", Venues: map[string]VenueConfig{venue: {BaseURL: srv.URL}}}, Deps{Doer: srv.Client(), Clock: clock, Sink: capture, Selected: selected})
				if err != nil {
					t.Fatal(err)
				}
				keep := []Channel{}
				for _, c := range channels {
					if c.Name == channel || c.Name == "exchangeInfo" || c.Name == "contracts" {
						keep = append(keep, c)
					}
				}
				s, err := NewScheduler(keep, capture, clock, "", 240)
				if err != nil {
					t.Fatal(err)
				}
				e.Bind(s)
				for i := 0; i < len(keep); i++ {
					if _, err := s.Step(context.Background()); err != nil {
						t.Fatal(err)
					}
					clock.t = clock.t.Add(time.Second)
				}
				if err := fileSink.Flush(); err != nil {
					t.Fatal(err)
				}
				var got []string
				for _, r := range capture.records {
					if r.Channel != channel {
						continue
					}
					got = append(got, r.Symbol)
					if r.Meta["snapshot"] != true && venue == "gate" {
						t.Fatal("lost snapshot metadata")
					}
					if venue == "binance" && (r.Meta["response_sha256"] == nil || r.Meta["status"] != 200) {
						t.Fatal("lost Binance metadata")
					}
					var expected string
					if venue == "binance" {
						for _, b := range brackets {
							if strings.Contains(b, `"symbol":"`+r.Symbol+`"`) {
								expected = b
							}
						}
					} else if r.Symbol == "BTC_USDT" {
						expected = `[{"contract":"BTC_USDT","tier":1,"risk_limit":1.2300}]`
					} else {
						expected = `[{"contract":"ETH_USDT","tier":1,"risk_limit":2}]`
					}
					filtered, _, err := ApplyPolicy(venue, channel, []byte(expected))
					if err != nil || string(r.Payload) != string(filtered) {
						t.Fatalf("payload changed: %s want %s (%v)", r.Payload, filtered, err)
					}
					paths, _ := filepath.Glob(filepath.Join(dir, venue, r.Symbol, "*", channel+".p.jsonl"))
					if len(paths) != 1 {
						t.Fatalf("missing selected file %s", r.Symbol)
					}
					b, err := os.ReadFile(paths[0])
					if err != nil {
						t.Fatal(err)
					}
					var envelope struct {
						P json.RawMessage `json:"p"`
					}
					if err := json.Unmarshal(b, &envelope); err != nil {
						t.Fatal(err)
					}
					if string(envelope.P) != string(filtered) {
						t.Fatalf("file payload changed: %s", envelope.P)
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("split symbols %v want %v", got, want)
				}
				files, _ := filepath.Glob(filepath.Join(dir, venue, "*", "*", channel+".p.jsonl"))
				if len(files) != len(want) {
					t.Fatalf("out-of-scope files: %v", files)
				}
				if venue == "binance" {
					if calls["/brackets"] != 1 || calls["/fapi/v1/exchangeInfo"] != 1 || len(calls) != 2 {
						t.Fatalf("requests %v", calls)
					}
					if len(s.Symbols()) != 2 {
						t.Fatal("discovery was narrowed by selection")
					}
				} else {
					if calls["/futures/usdt/risk_limit_tiers"] != 1 || calls["/futures/usdt/contracts"] != 1 || len(calls) != 2 {
						t.Fatalf("requests %v", calls)
					}
					for _, r := range capture.records {
						if r.Channel == "contracts" && (r.Symbol != "" || !strings.Contains(string(r.Payload), "TRADIFI_USDT")) {
							t.Fatal("whole snapshot was scoped")
						}
					}
				}
			})
		}
	}
}

type scopeCapture struct {
	Sink
	records []Record
}

func TestSplitScopeDoesNotSuppressBulkDiagnostic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "fixture failure", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	clock := &fakeClock{time.Date(2026, 11, 16, 1, 0, 0, 0, time.UTC)}
	sink := NewFileSink(t.TempDir(), time.Hour)
	defer sink.Close()
	e, channels, err := NewVenue("gate", PollConfig{MapPath: writeMap(t), Venues: map[string]VenueConfig{"gate": {BaseURL: srv.URL}}}, Deps{Doer: srv.Client(), Clock: clock, Sink: sink, Selected: []string{"ABSENT"}})
	if err != nil {
		t.Fatal(err)
	}
	var tiers Channel
	for _, c := range channels {
		if c.Name == "riskLimitTiers" {
			tiers = c
		}
	}
	s, err := NewScheduler([]Channel{tiers}, sink, clock, "", 240)
	if err != nil {
		t.Fatal(err)
	}
	e.Bind(s)
	if _, err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := sink.Flush(); err != nil {
		t.Fatal(err)
	}
	paths, _ := filepath.Glob(filepath.Join(sink.Dir, "gate", "gap", "*", "gap.p.jsonl"))
	if len(paths) != 1 {
		t.Fatalf("missing bulk diagnostic: %v", paths)
	}
	b, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"event":"poll_failed"`) || !strings.Contains(string(b), `"symbol":""`) {
		t.Fatalf("diagnostic changed: %s", b)
	}
}

func TestSnapshotSplitScopeResolution(t *testing.T) {
	e := &Env{Venue: "fake", Clock: &fakeClock{time.Now()}}
	c := e.SnapshotChannel("tiers", SnapshotSpec{Mode: SnapshotSplit,
		Fetch: func(context.Context, string) ([]byte, error) { return []byte(`{}`), nil },
		Split: func([]byte) ([]SnapshotRow, error) {
			return []SnapshotRow{{Symbol: "A", Raw: []byte(`{"x":1.2300}`)}, {Symbol: "B", Raw: []byte(`{"x":2}`)}}, nil
		}})
	for _, tc := range []struct {
		name     string
		universe func() []string
		want     int
	}{
		{"unset", nil, 2},
		{"configured empty", func() []string { return nil }, 0},
		{"selected", func() []string { return []string{"A"} }, 1},
		{"expanded", func() []string { return []string{"A", "B"} }, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e.Universe = tc.universe
			recs, err := c.Fetch(context.Background(), "")
			if err != nil || len(recs) != tc.want {
				t.Fatalf("records %v err %v", recs, err)
			}
			if len(recs) > 0 && string(recs[0].Payload) != `{"x":1.2300}` {
				t.Fatal("split payload bytes changed")
			}
		})
	}
}

func (s *scopeCapture) Write(r Record) { s.records = append(s.records, r); s.Sink.Write(r) }
