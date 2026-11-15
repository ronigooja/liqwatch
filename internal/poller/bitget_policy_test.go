package poller

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestBitgetPolicyRecords(t *testing.T) {
	for _, tc := range []struct{ channel, fixture, expected string }{
		{"contracts", "contracts.json", "contracts-expected.txt"},
		{"positionTiers", "query-position-lever.json", "positionTiers-expected.txt"},
	} {
		t.Run(tc.channel, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/bitget/" + tc.fixture)
			if err != nil {
				t.Fatal(err)
			}
			want, err := os.ReadFile("testdata/bitget/" + tc.expected)
			if err != nil {
				t.Fatal(err)
			}
			before := UnknownFields("bitget", tc.channel)
			got, record, err := ApplyPolicy("bitget", tc.channel, raw)
			if err != nil || !record || !bytes.Equal(got, bytes.TrimSpace(want)) {
				t.Fatalf("record=%v err=%v got=%s want=%s", record, err, got, want)
			}
			if delta := UnknownFields("bitget", tc.channel) - before; delta != 0 {
				t.Fatalf("unexpected fixture fields: %d", delta)
			}
			raw = bytes.Replace(raw, []byte(`"data": [`), []byte(`"novelWrapper":1,"data": [`), 1)
			raw = bytes.Replace(raw, []byte(`"symbol":`), []byte(`"novelItem":1,"symbol":`), 1)
			before = UnknownFields("bitget", tc.channel)
			got, record, err = ApplyPolicy("bitget", tc.channel, raw)
			if err != nil || !record || !bytes.Contains(got, []byte(`"novelWrapper":1`)) || !bytes.Contains(got, []byte(`"novelItem":1`)) || UnknownFields("bitget", tc.channel)-before != 2 {
				t.Fatalf("unknown handling: %s %v %v", got, record, err)
			}
		})
	}
}

func TestBitgetUnfilteredRatiosAndDroppedChannel(t *testing.T) {
	for _, tc := range []struct{ channel, fixture string }{{"longShort", "long-short.json"}, {"accountLongShort", "account-long-short.json"}, {"positionLongShort", "position-long-short.json"}} {
		var envelope struct {
			Data []json.RawMessage `json:"data"`
		}
		raw, err := os.ReadFile("testdata/bitget/" + tc.fixture)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &envelope); err != nil || len(envelope.Data) == 0 {
			t.Fatal(err)
		}
		policies.RLock()
		_, exists := policies.m[policyKey{"bitget", tc.channel}]
		policies.RUnlock()
		if exists {
			t.Fatalf("%s must not have a no-op policy", tc.channel)
		}
		got, record, err := ApplyPolicy("bitget", tc.channel, envelope.Data[0])
		if err != nil || !record || !bytes.Equal(got, envelope.Data[0]) {
			t.Fatalf("%s changed", tc.channel)
		}
	}
	got, record, err := ApplyPolicy("bitget", "takerBuySell", []byte(`{"ts":"42"}`))
	if err != nil || record || len(got) != 0 {
		t.Fatalf("drop: %s %v %v", got, record, err)
	}
}

func TestBitgetPolicyGolden(t *testing.T) { assertVenueGolden(t, "bitget") }

func TestBitgetPolicyDedup(t *testing.T) {
	row := func(s string) []Record {
		return []Record{{Venue: "bitget", Channel: "positionTiers", Symbol: "BTCUSDT", Meta: map[string]any{"last_row_timestamp": int64(42)}, Payload: []byte(s)}}
	}
	assertVenueDedup(t, "bitget", "positionTiers", venueDedupFixture{Original: row(`{"data":[{"level":"1","symbol":"BTCUSDT"}]}`), Dropped: row(`{"data":[{"level":"1","symbol":"ETHUSDT"}]}`), Kept: row(`{"data":[{"level":"2","symbol":"BTCUSDT"}]}`)})
}

func TestBitgetTimestampRegistration(t *testing.T) {
	// Ratio channels have no policy: their ts remains untouched. A dropped
	// channel still advances its cursor from the original history row.
	for _, channel := range []string{"longShort", "accountLongShort", "positionLongShort"} {
		p, record, err := ApplyPolicy("bitget", channel, []byte(` { "ts": "42" } `))
		if err != nil || !record || !strings.Contains(string(p), `"ts": "42"`) {
			t.Fatalf("%s timestamp", channel)
		}
	}
	policies.RLock()
	p, ok := policies.m[policyKey{"bitget", "takerBuySell"}]
	policies.RUnlock()
	if !ok || !p.DropChannel || p.TimestampKey != "ts" {
		t.Fatal("taker channel timestamp registration")
	}
}
