package bybit

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/venuetest"
)

func TestRecordFieldsFixtures(t *testing.T) {
	seen := map[string]map[string]bool{}
	for _, frame := range venuetest.Fixture(t, "testdata/frames.jsonl") {
		var src map[string]json.RawMessage
		if json.Unmarshal(frame, &src) != nil {
			t.Fatal("fixture")
		}
		var topic string
		_ = json.Unmarshal(src["topic"], &topic)
		prefix, sym, ok := strings.Cut(topic, ".")
		ch := map[string]string{"tickers": "mark", "publicTrade": "trade", "allLiquidation": "liquidation"}[prefix]
		if !ok || ch == "" {
			continue
		}
		a := &Adapter{}
		records := a.Split(venue.Shard{Name: t.Name() + topic + string(frame)}, frame)
		if seen[ch] == nil {
			seen[ch] = map[string]bool{}
		}
		var items []map[string]json.RawMessage
		if ch == "mark" {
			var item map[string]json.RawMessage
			if json.Unmarshal(src["data"], &item) != nil {
				t.Fatal("data")
			}
			items = append(items, item)
		} else if json.Unmarshal(src["data"], &items) != nil {
			t.Fatal("data")
		}
		for _, r := range records {
			if r.Symbol != sym || r.Channel != ch {
				t.Fatalf("envelope: %+v", r)
			}
			var out map[string]json.RawMessage
			if json.Unmarshal(r.Payload, &out) != nil {
				t.Fatal(string(r.Payload))
			}
			if sample, ok := out["sample"]; ok {
				if string(sample) != string(frame) {
					t.Fatal("sample changed")
				}
				continue
			}
			for _, k := range []string{"topic", "type"} {
				if _, ok := out[k]; ok {
					t.Errorf("%s root %s retained", ch, k)
				}
			}
			for _, k := range []string{"cs", "ts"} {
				if v, ok := src[k]; ok && string(out[k]) != string(v) {
					t.Errorf("%s root %s changed", ch, k)
				}
			}
			var data []map[string]json.RawMessage
			if ch == "mark" {
				var item map[string]json.RawMessage
				_ = json.Unmarshal(out["data"], &item)
				data = append(data, item)
			} else {
				_ = json.Unmarshal(out["data"], &data)
			}
			if len(data) != 1 {
				t.Fatalf("data: %s", r.Payload)
			}
			for _, k := range map[string][]string{"mark": {"symbol", "fundingIntervalHour", "fundingCap"}, "trade": {"BT", "RPI"}}[ch] {
				if _, ok := data[0][k]; ok {
					t.Errorf("%s data %s retained", ch, k)
				}
				for _, item := range items {
					if _, ok := item[k]; ok {
						seen[ch][k] = true
					}
				}
			}
			for _, k := range map[string][]string{"mark": {"singleOpenInterest", "singleOpenInterestValue"}, "trade": {"seq", "i", "T", "S", "v", "p"}, "liquidation": {"T", "S", "v", "p"}}[ch] {
				for _, item := range items {
					if v, ok := item[k]; ok && string(data[0][k]) == string(v) {
						seen[ch][k] = true
					}
				}
			}
		}
	}
	for ch, fields := range map[string][]string{"mark": {"symbol", "fundingIntervalHour", "fundingCap", "singleOpenInterest", "singleOpenInterestValue"}, "trade": {"BT", "RPI", "seq", "i", "T", "S", "v", "p"}, "liquidation": {"T", "S", "v", "p"}} {
		for _, k := range fields {
			if !seen[ch][k] {
				t.Errorf("%s %s not exercised or changed", ch, k)
			}
		}
	}
}

func TestRecordFieldClassification(t *testing.T) {
	for ch, fields := range map[string][]string{"mark": {"symbol", "fundingIntervalHour", "fundingCap"}, "trade": {"BT", "RPI"}} {
		for _, k := range fields {
			if !dropFields[ch][k] || keepFields[ch][k] {
				t.Errorf("%s %s must be dropped", ch, k)
			}
		}
	}
	for ch, fields := range map[string][]string{"mark": {"singleOpenInterest", "singleOpenInterestValue"}, "trade": {"seq", "i", "T", "S", "v", "p"}, "liquidation": {"T", "S", "v", "p"}} {
		for _, k := range fields {
			if !keepFields[ch][k] || dropFields[ch][k] {
				t.Errorf("%s %s must be kept", ch, k)
			}
		}
	}
}
