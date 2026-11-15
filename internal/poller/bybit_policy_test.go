package poller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestBybitPolicyRecords(t *testing.T) {
	tests := []struct{ channel, file, want string }{
		{"accountRatio", "ratio-fixture.json", `{"buyRatio":"0.7197","sellRatio":"0.2803","timestamp":"1794666000000"}`},
		{"riskLimit", "risk-fixture.json", `{"result":{"category":"linear","list":[{"id":1,"symbol":"0GUSDT","riskLimitValue":"10000","maintenanceMargin":"0.015","initialMargin":"0.02","isLowestRisk":1,"maxLeverage":"50.00","mmDeduction":""},{"id":2,"symbol":"0GUSDT","riskLimitValue":"25000","maintenanceMargin":"0.02","initialMargin":"0.04","isLowestRisk":0,"maxLeverage":"25.00","mmDeduction":"50"}]},"time":1794666297098}`},
		{"instruments", "instruments-fixture.json", `{"result":{"category":"linear","list":[{"symbol":"0GUSDT","contractType":"LinearPerpetual","baseCoin":"0G","quoteCoin":"USDT","leverageFilter":{"minLeverage":"1","maxLeverage":"50.00","leverageStep":"0.01"},"fundingInterval":240,"settleCoin":"USDT"},{"symbol":"1000000BABYDOGEUSDT","contractType":"LinearPerpetual","baseCoin":"1000000BABYDOGE","quoteCoin":"USDT","leverageFilter":{"minLeverage":"1","maxLeverage":"25.00","leverageStep":"0.01"},"fundingInterval":240,"settleCoin":"USDT"}]},"time":1794666298281}`},
	}
	for _, tt := range tests {
		t.Run(tt.channel, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/bybit/" + tt.file)
			if err != nil {
				t.Fatal(err)
			}
			if tt.channel == "accountRatio" {
				var v struct {
					Result struct {
						List []json.RawMessage `json:"list"`
					} `json:"result"`
				}
				if err := json.Unmarshal(raw, &v); err != nil {
					t.Fatal(err)
				}
				raw = v.Result.List[0]
			}
			before := UnknownFields("bybit", tt.channel)
			got, record, err := ApplyPolicy("bybit", tt.channel, raw)
			if err != nil || !record || string(got) != tt.want {
				t.Fatalf("got %s record=%v err=%v", got, record, err)
			}
			if delta := UnknownFields("bybit", tt.channel) - before; delta != 0 {
				t.Fatalf("fixture unknown=%d", delta)
			}
			raw = append([]byte(`{"unrecognized":123,`), bytes.TrimSpace(raw)[1:]...)
			got, record, err = ApplyPolicy("bybit", tt.channel, raw)
			if err != nil || !record || !bytes.Contains(got, []byte(`"unrecognized":123`)) || UnknownFields("bybit", tt.channel)-before != 1 {
				t.Fatalf("unknown: %s %v %v", got, record, err)
			}
		})
	}
}

func TestBybitPolicyGolden(t *testing.T) { assertVenueGolden(t, "bybit") }
func TestBybitPolicyTimestamp(t *testing.T) {
	for _, tc := range []struct{ channel, key string }{{"accountRatio", "timestamp"}, {"riskLimit", ""}, {"instruments", ""}} {
		policies.RLock()
		p, ok := policies.m[policyKey{"bybit", tc.channel}]
		policies.RUnlock()
		if !ok || p.TimestampKey != tc.key || p.TimestampIndex != nil || p.TimestampPath != "" {
			t.Fatalf("%s: %+v registered=%v", tc.channel, p, ok)
		}
	}
}
func TestBybitPolicyDedup(t *testing.T) {
	row := func(s string) []Record {
		return []Record{{Venue: "bybit", Channel: "accountRatio", Symbol: "BTCUSDT", Meta: map[string]any{"last_row_timestamp": int64(42)}, Payload: []byte(s)}}
	}
	assertVenueDedup(t, "bybit", "accountRatio", venueDedupFixture{Original: row(`{"timestamp":"42","buyRatio":"0.5","sellRatio":"0.5","symbol":"BTCUSDT"}`), Dropped: row(`{"timestamp":"42","buyRatio":"0.5","sellRatio":"0.5","symbol":"OTHER"}`), Kept: row(`{"timestamp":"42","buyRatio":"0.6","sellRatio":"0.5","symbol":"BTCUSDT"}`)})
}
func TestBybitPolicyPaging(t *testing.T) {
	for _, name := range []string{"riskLimit", "instruments"} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				cursor := r.URL.Query().Get("cursor")
				if calls == 1 && cursor == "" {
					fmt.Fprint(w, `{"retCode":0,"result":{"category":"linear","list":[],"nextPageCursor":"next"},"time":1}`)
				} else if calls == 2 && cursor == "next" {
					fmt.Fprint(w, `{"retCode":0,"result":{"category":"linear","list":[],"nextPageCursor":""},"time":2}`)
				} else {
					t.Errorf("unexpected cursor %q", cursor)
					http.Error(w, "bad", 500)
				}
			}))
			defer server.Close()
			e := &Env{Venue: "bybit", BaseURL: server.URL, Doer: uaDoer{server.Client()}, Clock: RealClock{}}
			rows, err := bybitSnapshot(e, name, "/", name == "instruments").Fetch(context.Background(), "")
			if err != nil || calls != 2 || len(rows) != 2 {
				t.Fatalf("calls=%d rows=%d err=%v", calls, len(rows), err)
			}
			for _, row := range rows {
				got, record, err := ApplyPolicy("bybit", name, row.Payload)
				if err != nil || !record || strings.Contains(string(got), "nextPageCursor") {
					t.Fatalf("filtered=%s record=%v err=%v", got, record, err)
				}
			}
		})
	}
}
