package poller

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// These expectations are deliberately independent of the registered tables.
var gateExpected = map[string]string{
	"contractStats":  "time lsr_taker lsr_account top_lsr_account top_lsr_size open_interest open_interest_usd mark_price long_liq_size short_liq_size long_liq_usd short_liq_usd",
	"riskLimitTiers": "tier risk_limit initial_rate maintenance_rate leverage_max deduction",
	"contracts":      "name type quanto_multiplier enable_decimal funding_rate funding_next_apply funding_interval leverage_min leverage_max mark_price index_price position_size config_change_time",
}

func gateSample(t *testing.T, ch string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/gate/" + ch + ".sample.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSpace(b)
}

func TestGatePolicyRecords(t *testing.T) {
	for ch, fields := range gateExpected {
		t.Run(ch, func(t *testing.T) {
			raw := gateSample(t, ch)
			if ch == "contractStats" {
				raw = append(append([]byte{'['}, raw...), ']')
			}
			var source []map[string]json.RawMessage
			if err := json.Unmarshal(raw, &source); err != nil {
				t.Fatal(err)
			}
			before := UnknownFields("gate", ch)
			got, record, err := ApplyPolicy("gate", ch, raw)
			if err != nil || !record {
				t.Fatalf("filter: %v", err)
			}
			var rows []map[string]json.RawMessage
			if err := json.Unmarshal(got, &rows); err != nil || len(rows) != len(source) {
				t.Fatalf("rows: %v", err)
			}
			keep := map[string]bool{}
			for _, k := range strings.Fields(fields) {
				keep[k] = true
			}
			for i, row := range source {
				for k, v := range row {
					if keep[k] {
						if !bytes.Equal(rows[i][k], v) {
							t.Fatalf("row %d kept %s", i, k)
						}
					} else if _, ok := rows[i][k]; ok {
						t.Fatalf("row %d dropped %s", i, k)
					}
				}
			}
			if n := UnknownFields("gate", ch) - before; n != 0 {
				t.Fatalf("unknown %d", n)
			}
			before = UnknownFields("gate", ch)
			got, _, err = ApplyPolicy("gate", ch, []byte(`[{"new_field":1}]`))
			if err != nil || string(got) != `[{"new_field":1}]` || UnknownFields("gate", ch)-before != 1 {
				t.Fatal("unknown field")
			}
		})
	}
}

func TestGatePolicyTimestampAndDedup(t *testing.T) {
	policies.RLock()
	p, ok := policies.m[policyKey{"gate", "contractStats"}]
	policies.RUnlock()
	if !ok || p.TimestampPath != "[]" || p.TimestampKey != "time" {
		t.Fatal("timestamp registration")
	}
	for _, tc := range []struct{ ch, original, dropped, kept string }{
		{"contractStats", `{"time":42,"long_users":1,"mark_price":3}`, `{"time":42,"long_users":2,"mark_price":3}`, `{"time":42,"long_users":1,"mark_price":4}`},
		{"contracts", `[{"name":"X","last_price":1}]`, `[{"name":"X","last_price":2}]`, `[{"name":"Y","last_price":1}]`},
	} {
		t.Run(tc.ch, func(t *testing.T) {
			row := func(s string) []Record {
				return []Record{{Venue: "gate", Channel: tc.ch, Meta: map[string]any{"last_row_timestamp": int64(42)}, Payload: []byte(s)}}
			}
			assertVenueDedup(t, "gate", tc.ch, venueDedupFixture{Original: row(tc.original), Dropped: row(tc.dropped), Kept: row(tc.kept)})
		})
	}
}

func TestGatePolicyGolden(t *testing.T) {
	var got []byte
	for _, ch := range []string{"contractStats", "riskLimitTiers", "contracts"} {
		raw := gateSample(t, ch)
		clock := &fakeClock{time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)}
		sink := &memorySink{}
		c := Channel{Venue: "gate", Name: ch, Fetch: func(context.Context, string) ([]Record, error) {
			return []Record{{Venue: "gate", Channel: ch, Symbol: "BTC_USDT", RecvWall: clock.Now().UnixNano(), Meta: map[string]any{"sample": true}, Payload: raw}}, nil
		}}
		s, err := NewScheduler([]Channel{c}, sink, clock, "", 240)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, r := range sink.records {
			b, err := json.Marshal(fixtureGoldenRecord{"testdata/gate/" + ch + ".sample.jsonl", r.Venue, r.Channel, r.Symbol, r.RecvWall, r.Meta, r.Payload})
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, b...)
			got = append(got, '\n')
		}
	}
	want, err := os.ReadFile("testdata/gate/golden-filtered.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("Gate golden differs")
	}
}
