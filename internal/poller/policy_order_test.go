package poller

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestPolicyDedupUsesFilteredRows(t *testing.T) {
	var bybit bybitResponse
	b, err := os.ReadFile("testdata/bybit/ratio-fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &bybit); err != nil {
		t.Fatal(err)
	}
	var okx okxEnvelope
	b, err = os.ReadFile("testdata/okx/response0.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &okx); err != nil {
		t.Fatal(err)
	}
	var okxRows []json.RawMessage
	if err = json.Unmarshal(okx.Data, &okxRows); err != nil {
		t.Fatal(err)
	}
	zero := 0
	cases := []struct {
		name    string
		raw     []byte
		ts      int64
		policy  Policy
		changed []byte
	}{
		{"binance", []byte(`{"timestamp":1794666000000,"symbol":"BTCUSDT","value":"1"}`), 1794666000000, Policy{TimestampKey: "timestamp", Keep: []string{"timestamp", "symbol"}, Drop: []string{"value"}}, []byte(`{"timestamp":1794666000000,"symbol":"BTCUSDT","value":"2"}`)},
		{"okx", okxRows[0], 1794666000000, Policy{TimestampIndex: &zero, KeepIndexes: []int{0}, DropIndexes: []int{1}}, []byte(`["1794666000000","changed"]`)},
		{"bybit", bybit.Result.List[0], 1794666000000, Policy{TimestampKey: "timestamp", Keep: []string{"timestamp", "symbol", "sellRatio"}, Drop: []string{"buyRatio"}}, []byte(`{"symbol":"BTCUSDT","buyRatio":"changed","sellRatio":"0.2803","timestamp":"1794666000000"}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			venue := "order-" + tc.name
			if err := RegisterPolicy(venue, "filtered", tc.policy); err != nil {
				t.Fatal(err)
			}
			run := func(channel string) ([]Record, uint64, uint64, int64) {
				clock := &fakeClock{time.UnixMilli(tc.ts + 600000)}
				sink := &memorySink{}
				n := 0
				c := Channel{Venue: venue, Name: channel, Period: time.Hour, Fetch: func(context.Context, string) ([]Record, error) {
					n++
					raw := tc.raw
					if n == 2 {
						raw = tc.changed
					}
					return []Record{{Venue: venue, Channel: channel, Symbol: "BTC", Meta: map[string]any{"last_row_timestamp": tc.ts}, Payload: raw}}, nil
				}}
				s, err := NewScheduler([]Channel{c}, sink, clock, "", 240)
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 2; i++ {
					if _, err := s.Step(context.Background()); err != nil {
						t.Fatal(err)
					}
					clock.t = clock.t.Add(25 * time.Hour)
				}
				return sink.records, s.Duplicates.Load(), s.Revisions.Load(), s.RowTimestamp(venue, channel, "")
			}
			before, bd, br, bc := run("unfiltered")
			after, ad, ar, ac := run("filtered")
			if len(before) != 2 || bd != 0 || br != 1 || len(after) != 1 || ad != 1 || ar != 0 || bc != ac || bc != tc.ts {
				t.Fatalf("before %d/%d/%d/%d after %d/%d/%d/%d", len(before), bd, br, bc, len(after), ad, ar, ac)
			}
			if _, ok := after[0].Meta["unfiltered_payload_sha256"]; ok {
				t.Fatal("unexpected unfiltered hash metadata")
			}
		})
	}
}
