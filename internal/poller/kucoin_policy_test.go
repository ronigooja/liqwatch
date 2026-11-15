package poller

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestKucoinPolicyRecords(t *testing.T) {
	cases := []struct{ channel, keep string }{
		{"contracts", "symbol baseCurrency quoteCurrency settleCurrency multiplier isInverse isQuanto type fundingFeeRate nextFundingRateDateTime nextFundingRateTime fundingRateGranularity currentFundingRateGranularity effectiveFundingRateCycleStartTime openInterest maxLeverage initialMargin maintainMargin maxRiskLimit minRiskLimit riskStep supportCross"},
		{"riskLimit", "level maxRiskLimit minRiskLimit maxLeverage initialMargin maintainMargin"},
	}
	for _, tc := range cases {
		t.Run(tc.channel, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/kucoin/" + tc.channel + ".sample.jsonl")
			if err != nil {
				t.Fatal(err)
			}
			var source struct {
				Data []map[string]json.RawMessage `json:"data"`
			}
			if err = json.Unmarshal(raw, &source); err != nil {
				t.Fatal(err)
			}
			before := UnknownFields("kucoin", tc.channel)
			got, record, err := ApplyPolicy("kucoin", tc.channel, raw)
			if err != nil || !record {
				t.Fatalf("record=%v err=%v", record, err)
			}
			var out map[string]json.RawMessage
			if err = json.Unmarshal(got, &out); err != nil {
				t.Fatal(err)
			}
			if len(out) != 1 {
				t.Fatalf("unexpected wrapper fields")
			}
			var rows []map[string]json.RawMessage
			if err = json.Unmarshal(out["data"], &rows); err != nil {
				t.Fatal(err)
			}
			if len(rows) != len(source.Data) {
				t.Fatal("row count")
			}
			keep := map[string]bool{}
			for _, k := range strings.Fields(tc.keep) {
				keep[k] = true
			}
			for i, row := range source.Data {
				for k, v := range row {
					if keep[k] {
						if !bytes.Equal(rows[i][k], v) {
							t.Fatalf("row %d retained %s changed", i, k)
						}
					} else if _, ok := rows[i][k]; ok {
						t.Fatalf("row %d unexpected field %s", i, k)
					}
				}
			}
			if n := UnknownFields("kucoin", tc.channel) - before; n != 0 {
				t.Fatalf("unexpected unknown count %d", n)
			}
			before = UnknownFields("kucoin", tc.channel)
			got, _, err = ApplyPolicy("kucoin", tc.channel, []byte(`{"code":"200000","data":[{"futureField":1}]}`))
			if err != nil || !bytes.Contains(got, []byte(`"futureField":1`)) || UnknownFields("kucoin", tc.channel)-before != 1 {
				t.Fatal("unknown field not retained and counted")
			}
		})
	}
}
func TestKucoinPolicyGolden(t *testing.T) { assertVenueGolden(t, "kucoin") }
func TestKucoinPolicyRegistrationAndDedup(t *testing.T) {
	for _, ch := range []string{"contracts", "riskLimit"} {
		policies.RLock()
		p, ok := policies.m[policyKey{"kucoin", ch}]
		policies.RUnlock()
		if !ok || p.TimestampKey != "" || p.TimestampIndex != nil || p.TimestampPath != "" {
			t.Fatalf("%s registration or snapshot timestamp", ch)
		}
	}
	row := func(s string) []Record {
		return []Record{{Venue: "kucoin", Channel: "contracts", Meta: map[string]any{"last_row_timestamp": int64(42)}, Payload: []byte(s)}}
	}
	assertVenueDedup(t, "kucoin", "contracts", venueDedupFixture{Original: row(`{"code":"200000","data":[{"symbol":"X","status":"Open"}]}`), Dropped: row(`{"code":"200000","data":[{"symbol":"X","status":"Closed"}]}`), Kept: row(`{"code":"200000","data":[{"symbol":"Y","status":"Open"}]}`)})
}
