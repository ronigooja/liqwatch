package kucoin

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/venue"
)

func TestPayloadFields(t *testing.T) {
	oldNow, oldLast := samples.now, samples.last
	defer func() { samples.now, samples.last = oldNow, oldLast }()
	var trade, mark, funding []byte
	for _, f := range fixtureFrames(t) {
		s := string(f)
		switch {
		case trade == nil && strings.Contains(s, `"subject":"match"`):
			trade = f
		case mark == nil && strings.Contains(s, `"subject":"mark.index.price"`):
			mark = f
		case funding == nil && strings.Contains(s, `"subject":"funding.rate"`):
			funding = f
		}
	}
	cases := []struct {
		name, symbol, child string
		frame               []byte
		drop, keep          []string
	}{
		{"trade", "XBTUSDTM", "data", trade, []string{"topic", "type", "subject", "data.symbol"}, []string{"data.side", "data.size", "data.price", "data.tradeId", "data.ts"}},
		{"mark", "XBTUSDTM", "data", mark, []string{"topic", "type", "subject"}, []string{"data.markPrice", "data.indexPrice", "data.timestamp"}},
		{"funding", "0GUSDTM", "data", funding, []string{"topic", "type", "subject"}, []string{"data.fundingRate", "data.timestamp"}},
		{"utamark", "XBTUSDTM", "d", []byte(`{"T":"mark-price","P":123,"d":{"s":"XBTUSDTM","mp":1.2300,"ip":"2","oi":"3","ts":123}}`), []string{"T", "d.s"}, []string{"P", "d.mp", "d.ip", "d.oi", "d.ts"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.frame == nil {
				t.Fatal("missing fixture")
			}
			var original map[string]json.RawMessage
			if err := json.Unmarshal(tc.frame, &original); err != nil {
				t.Fatal(err)
			}
			var child map[string]json.RawMessage
			if err := json.Unmarshal(original[tc.child], &child); err != nil {
				t.Fatal(err)
			}
			child["unrecognizedChild"] = json.RawMessage(`123`)
			original[tc.child], _ = json.Marshal(child)
			original["unrecognizedTop"] = json.RawMessage(`"keep"`)
			frame, _ := json.Marshal(original)
			samples.last = map[string]time.Time{}
			before := unknownFields.Load()
			records := (&Adapter{}).Split(venue.Shard{Name: tc.name}, frame)
			if len(records) != 2 || records[0].Symbol != tc.symbol || records[1].Symbol != tc.symbol || !bytes.Equal(records[1].Payload, append(append([]byte(`{"sample":`), frame...), '}')) {
				t.Fatalf("records: %v", records)
			}
			channel := tc.name
			if channel == "funding" {
				channel = "mark"
			}
			if records[0].Channel != channel || records[1].Channel != channel {
				t.Fatalf("channels: %v", records)
			}
			var result map[string]json.RawMessage
			if err := json.Unmarshal(records[0].Payload, &result); err != nil {
				t.Fatal(err)
			}
			var resultChild map[string]json.RawMessage
			if err := json.Unmarshal(result[tc.child], &resultChild); err != nil {
				t.Fatal(err)
			}
			get := func(top, nested map[string]json.RawMessage, path string) json.RawMessage {
				parts := strings.Split(path, ".")
				if len(parts) == 2 {
					return nested[parts[1]]
				}
				return top[path]
			}
			for _, path := range tc.drop {
				if get(result, resultChild, path) != nil {
					t.Errorf("%s retained", path)
				}
			}
			for _, path := range append(tc.keep, "unrecognizedTop", tc.child+".unrecognizedChild") {
				want, got := get(original, child, path), get(result, resultChild, path)
				if want == nil || !bytes.Equal(want, got) {
					t.Errorf("%s: got %s want %s", path, got, want)
				}
			}
			// P is deliberately unknown in the existing table, as are the injected fields.
			wantUnknown := uint64(2)
			if tc.name == "utamark" {
				wantUnknown++
			}
			if got := unknownFields.Load() - before; got != wantUnknown {
				t.Errorf("unknown count %d, want %d", got, wantUnknown)
			}
		})
	}
}
