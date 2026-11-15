package gate

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/ronigooja/liqwatch/internal/venue"
)

// These expectations are independent of the adapter's classification tables.
func TestAllClassifiedFields(t *testing.T) {
	cases := []struct{ channel, wire, keep, drop string }{
		{"trades", "futures.trades", "id size price create_time_ms time time_ms t quanto_multiplier is_internal", "contract create_time"},
		{"liquidations", "futures.public_liquidates", "size price time time_ms t create_time create_time_ms id quanto_multiplier", "contract"},
		{"tickers", "futures.tickers", "mark_price index_price funding_rate funding_next_apply time time_ms t quanto_multiplier", "contract funding_rate_indicative funding_interval funding_offset total_size event_type last change_percentage volume_24h volume_24h_base volume_24h_quote volume_24h_settle quanto_base_rate low_24h high_24h price_type change_from change_price highest_bid highest_size lowest_ask lowest_size bid ask bid_size ask_size"},
		{"contract_stats", "futures.contract_stats", "time time_ms t open_interest open_interest_usd lsr_account lsr_taker top_lsr_account top_lsr_size mark_price long_liq_amount long_liq_size long_liq_usd short_liq_amount short_liq_size short_liq_usd", "contract"},
		{"contract_info", "futures.contract_info", "time time_ms t quanto_multiplier risk_limit_tiers mark_price index_price funding_rate funding_next_apply total_size leverage_min leverage_max leverage_tiers", "contract order_price_round mark_price_round last change_percentage volume_24h volume_24h_base volume_24h_quote volume_24h_settle quanto_base_rate low_24h high_24h highest_bid highest_size lowest_ask lowest_size bid ask bid_size ask_size"},
	}
	for _, c := range cases {
		t.Run(c.channel, func(t *testing.T) {
			item := map[string]any{"contract": "BTC_USDT"}
			wantItem := map[string]any{}
			for _, k := range strings.Fields(c.keep) {
				item[k] = "value"
				wantItem[k] = "value"
			}
			for _, k := range strings.Fields(c.drop) {
				if k != "contract" {
					item[k] = "discard"
				}
			}
			input := map[string]any{"channel": c.wire, "event": "update", "time": 1, "time_ms": 2, "conn_id": "discard", "trace_id": "discard", "result": item}
			want := map[string]any{"time": float64(1), "time_ms": float64(2), "result": wantItem}
			if c.channel == "contract_info" {
				want["event"] = "update"
			}
			b, _ := json.Marshal(input)
			a := &Adapter{}
			a.Shards(context.Background(), []string{"BTC_USDT"})
			r := a.Split(venue.Shard{Name: t.Name()}, b)
			if len(r) != 2 {
				t.Fatalf("records: %+v", r)
			}
			var got any
			if json.Unmarshal(r[0].Payload, &got) != nil || !reflect.DeepEqual(got, want) || a.UnknownFields() != 0 {
				t.Fatalf("got %s unknown=%d want=%v", r[0].Payload, a.UnknownFields(), want)
			}
		})
	}
}
