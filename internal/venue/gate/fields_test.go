package gate

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/venuetest"
)

func TestRecordFieldPolicy(t *testing.T) {
	cases := []struct {
		channel, wire string
		drop, keep    []string
	}{
		{"trades", "futures.trades", []string{"contract", "create_time"}, []string{"id", "size", "price", "create_time_ms", "is_internal"}},
		{"tickers", "futures.tickers", []string{"contract", "funding_rate_indicative", "event_type", "funding_interval", "funding_offset", "total_size"}, []string{"mark_price", "index_price", "funding_rate", "funding_next_apply", "t"}},
		{"liquidations", "futures.public_liquidates", []string{"contract"}, []string{"price", "size", "time"}},
		{"contract_stats", "futures.contract_stats", []string{"contract"}, []string{"time", "lsr_account", "lsr_taker", "top_lsr_account", "top_lsr_size", "open_interest", "open_interest_usd", "long_liq_amount", "long_liq_size", "long_liq_usd", "short_liq_amount", "short_liq_size", "short_liq_usd", "mark_price"}},
		{"contract_info", "futures.contract_info", []string{"contract", "order_price_round", "mark_price_round"}, []string{"quanto_multiplier", "risk_limit_tiers"}},
	}
	fixtures := venuetest.Fixture(t, "testdata/frames.jsonl")
	for _, tc := range cases {
		t.Run(tc.channel, func(t *testing.T) {
			// Test all listed fields even where a live capture did not contain them.
			item := map[string]any{"contract": "BTC_USDT", "unknown_item": 1}
			for _, k := range tc.drop {
				if k != "contract" {
					item[k] = "drop-value"
				}
			}
			for _, k := range tc.keep {
				item[k] = "keep-value"
			}
			if tc.channel == "contract_info" {
				item["risk_limit_tiers"] = []any{map[string]any{"tier": 1, "unknown_nested": "preserved"}}
			}
			for _, shape := range []string{"array", "object"} {
				t.Run(shape, func(t *testing.T) {
					var result any = item
					if shape == "array" {
						result = []any{item}
					}
					env := map[string]any{"channel": tc.wire, "event": "update", "time": 123, "time_ms": 123456, "result": result, "unknown_envelope": 2}
					if tc.channel == "contract_info" {
						env["event"] = "all"
					}
					msg, _ := json.Marshal(env)
					checkRecordFields(t, tc.channel, msg, tc.drop, tc.keep, true)
				})
			}
			found := false
			for _, msg := range fixtures {
				var env struct{ Channel, Event string }
				if json.Unmarshal(msg, &env) == nil && env.Channel == tc.wire && env.Event == "update" {
					checkRecordFields(t, tc.channel, msg, tc.drop, tc.keep, false)
					found = true
					break
				}
			}
			if !found && tc.channel != "contract_info" && tc.channel != "contract_stats" {
				t.Fatal("missing live fixture")
			}
		})
	}
}

func checkRecordFields(t *testing.T, ch string, msg []byte, drops, keeps []string, synthetic bool) {
	t.Helper()
	var input map[string]json.RawMessage
	if err := json.Unmarshal(msg, &input); err != nil {
		t.Fatal(err)
	}
	var inItems []map[string]json.RawMessage
	array := len(input["result"]) > 0 && input["result"][0] == '['
	if array {
		if err := json.Unmarshal(input["result"], &inItems); err != nil {
			t.Fatal(err)
		}
	} else {
		var v map[string]json.RawMessage
		if err := json.Unmarshal(input["result"], &v); err != nil {
			t.Fatal(err)
		}
		inItems = append(inItems, v)
	}
	var sym string
	json.Unmarshal(inItems[0]["contract"], &sym)
	a := &Adapter{}
	shards, _ := a.Shards(context.Background(), []string{sym})
	// Distinct shard names isolate the global hourly sampler.
	sh := venue.Shard{Name: fmt.Sprintf("fields-%s-%s-%t", t.Name(), ch, synthetic)}
	_ = shards
	records := a.Split(sh, msg)
	if len(records) != len(inItems)+1 {
		t.Fatalf("records=%d", len(records))
	}
	if string(records[len(records)-1].Payload) != `{"sample":`+string(msg)+`}` {
		t.Fatal("sample changed")
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(records[0].Payload, &out); err != nil {
		t.Fatal(err)
	}
	if records[0].Symbol != sym || records[0].Channel != ch {
		t.Fatalf("identity: %+v", records[0])
	}
	if _, ok := out["channel"]; ok {
		t.Error("channel retained")
	}
	if ch == "contract_info" {
		if string(out["event"]) != string(input["event"]) {
			t.Error("event changed")
		}
	} else if _, ok := out["event"]; ok {
		t.Error("event retained")
	}
	for _, k := range []string{"time", "time_ms"} {
		if string(out[k]) != string(input[k]) {
			t.Errorf("envelope %s changed", k)
		}
	}
	var items []map[string]json.RawMessage
	if array {
		json.Unmarshal(out["result"], &items)
	} else {
		var v map[string]json.RawMessage
		json.Unmarshal(out["result"], &v)
		items = append(items, v)
	}
	if len(items) != 1 {
		t.Fatalf("result shape: %s", out["result"])
	}
	for _, k := range drops {
		if _, ok := items[0][k]; ok {
			t.Errorf("drop %s retained", k)
		}
	}
	for _, k := range keeps {
		if v, ok := inItems[0][k]; ok && string(items[0][k]) != string(v) {
			t.Errorf("keep %s changed", k)
		}
	}
	if synthetic && (string(items[0]["unknown_item"]) != "1" || string(out["unknown_envelope"]) != "2" || a.UnknownFields() != 2) {
		t.Errorf("unknown fields: %s count=%d", records[0].Payload, a.UnknownFields())
	}
}
