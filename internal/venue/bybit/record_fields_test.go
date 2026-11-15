package bybit

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/venuetest"
)

func TestRecordFieldsFixtures(t *testing.T) {
	t.Run("golden", TestRecordedGolden)
	seen := map[string]map[string]bool{}
	frames := append(venuetest.Fixture(t, "testdata/frames.jsonl"), venuetest.Fixture(t, "testdata/recorded-20261115.jsonl")...)
	for _, frame := range frames {
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
		before := a.UnknownFieldsSeen()
		records := a.Split(venue.Shard{Name: t.Name() + topic + string(frame)}, frame)
		if got := a.UnknownFieldsSeen() - before; got != 0 {
			t.Errorf("%s unexpected unknown fields: %d", ch, got)
		}
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

func TestRecordedGolden(t *testing.T) {
	frames := venuetest.Fixture(t, "testdata/recorded-20261115.jsonl")
	want := []string{
		`{"data":{"markPrice":"6.824","indexPrice":"6.832","openInterest":"6085129.9","openInterestValue":"41524926.44","singleOpenInterest":"3042565","singleOpenInterestValue":"20762463.56","nextFundingTime":"1794729600000","fundingRate":"-0.00021278"},"cs":289203257541,"ts":1794706489605}`,
		`{"data":{"markPrice":"6.824","indexPrice":"6.831","openInterest":"6085129.9","openInterestValue":"41524926.44","singleOpenInterest":"3042565","singleOpenInterestValue":"20762463.56","nextFundingTime":"1794729600000","fundingRate":"-0.00021278"},"cs":289203259955,"ts":1794706490505,"state":"partial"}`,
		`{"ts":1794706489678,"data":[{"T":1794706489676,"S":"Buy","v":"0.9","p":"6.823","i":"3208e107-52d5-5beb-a2d5-e199cc1b6558","seq":289203257722}]}`,
		`{"ts":1794706617301,"data":[{"T":1794706617117,"S":"Buy","v":"75.0","p":"6.764"}]}`,
		`{"ts":1794706617301,"data":[{"T":1794706617291,"S":"Buy","v":"7.6","p":"6.764"}]}`,
	}
	var got []string
	for i, frame := range frames {
		a := &Adapter{}
		for _, r := range a.Split(venue.Shard{Name: fmt.Sprintf("golden-%d", i)}, frame) {
			if !strings.HasPrefix(string(r.Payload), `{"sample":`) {
				if r.Symbol != "AVAXUSDT" {
					t.Fatalf("symbol %q", r.Symbol)
				}
				got = append(got, string(r.Payload))
			}
		}
	}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d: %q", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("record %d: got %s want %s", i, got[i], want[i])
		}
	}
}
