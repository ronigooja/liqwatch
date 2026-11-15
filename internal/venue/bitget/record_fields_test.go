package bitget

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/venuetest"
)

func TestRecordedFixtureFields(t *testing.T) {
	topics := map[string]string{"publicTrade": "trade", "ticker": "ticker", "funding-rate": "funding", "position-tier": "tier", "liquidation": "liquidation"}
	keeps := map[string][]string{"trade": {"i", "p", "v", "S", "T"}, "ticker": {"indexPrice", "markPrice", "fundingRate", "nextFundingTime", "openInterest"}, "funding": {"fundingRate", "nextUpdate"}, "tier": {"list"}, "liquidation": {"side", "price", "amount", "ts"}}
	seen := map[string]bool{}
	for n, frame := range venuetest.Fixture(t, "testdata/frames.jsonl") {
		var input struct {
			Action json.RawMessage `json:"action"`
			TS     json.RawMessage `json:"ts"`
			Arg    struct {
				Topic  string `json:"topic"`
				Symbol string `json:"symbol"`
			} `json:"arg"`
			Data json.RawMessage `json:"data"`
		}
		if json.Unmarshal(frame, &input) != nil || topics[input.Arg.Topic] == "" || len(input.Data) == 0 {
			continue
		}
		channel := topics[input.Arg.Topic]
		if seen[channel] {
			continue
		}
		var items []map[string]json.RawMessage
		array := input.Data[0] == '['
		if array {
			if json.Unmarshal(input.Data, &items) != nil {
				t.Fatal("bad fixture")
			}
		} else {
			var item map[string]json.RawMessage
			if json.Unmarshal(input.Data, &item) != nil {
				t.Fatal("bad fixture")
			}
			items = append(items, item)
		}
		if len(items) == 0 {
			continue
		}
		symbol := input.Arg.Symbol
		if v := items[0]["symbol"]; len(v) > 0 {
			_ = json.Unmarshal(v, &symbol)
		}
		if symbol == "" {
			continue
		}
		a := &Adapter{}
		_, _ = a.Shards(context.Background(), []string{symbol})
		records := a.Split(venue.Shard{Name: fmt.Sprintf("fixture-fields-%d", n)}, frame)
		if len(records) < 2 {
			t.Fatalf("%s: missing record/sample", channel)
		}
		var sample map[string]json.RawMessage
		if json.Unmarshal(records[len(records)-1].Payload, &sample) != nil || !bytes.Equal(sample["sample"], frame) {
			t.Fatalf("%s: incomplete sample", channel)
		}
		var got struct {
			Action json.RawMessage `json:"action"`
			TS     json.RawMessage `json:"ts"`
			Arg    json.RawMessage `json:"arg"`
			Data   json.RawMessage `json:"data"`
		}
		if json.Unmarshal(records[0].Payload, &got) != nil || records[0].Symbol != symbol || !bytes.Equal(got.Action, input.Action) || !bytes.Equal(got.TS, input.TS) {
			t.Fatalf("%s: envelope or symbol: %s", channel, records[0].Payload)
		}
		if len(got.Arg) != 0 {
			t.Errorf("%s: arg retained: %s", channel, got.Arg)
		}
		var output map[string]json.RawMessage
		if array {
			var out []map[string]json.RawMessage
			if json.Unmarshal(got.Data, &out) != nil || len(out) != 1 {
				t.Fatalf("%s array shape", channel)
			}
			output = out[0]
		} else if json.Unmarshal(got.Data, &output) != nil {
			t.Fatalf("%s object shape", channel)
		}
		for _, key := range append([]string{"symbol"}, map[string][]string{"trade": {"L", "isRPI"}}[channel]...) {
			if _, ok := output[key]; ok {
				t.Errorf("%s: dropped %s retained", channel, key)
			}
		}
		for _, key := range keeps[channel] {
			if !reflect.DeepEqual(output[key], items[0][key]) || len(output[key]) == 0 {
				t.Errorf("%s: kept %s differs", channel, key)
			}
		}
		seen[channel] = true
	}
	for _, channel := range topics {
		if !seen[channel] {
			t.Errorf("missing fixture %s", channel)
		}
	}
}

func TestFieldTableContract(t *testing.T) {
	keeps := map[string][]string{
		"trade": {"i", "p", "v", "S", "T"}, "ticker": {"ts", "indexPrice", "markPrice", "fundingRate", "openInterest", "nextFundingTime"},
		"funding": {"ts", "fundingRate", "nextUpdate"}, "tier": {"ts", "list", "leverage", "maxTierValue", "minTierValue", "mmr", "tier"},
		"liquidation": {"side", "price", "amount", "ts"},
	}
	drops := map[string][]string{
		"arg": {"instType", "topic", "symbol"}, "trade": {"symbol", "L", "isRPI"},
		"ticker":  {"symbol", "highPrice24h", "lowPrice24h", "openPrice24h", "lastPrice", "turnover24h", "volume24h", "bid1Price", "ask1Price", "bid1Size", "ask1Size", "price24hPcnt", "deliveryTime", "deliveryStartTime", "deliveryStatus"},
		"funding": {"symbol", "baseInterestRate", "cashDividend", "cashDividendNextUpdate", "fundingRateInterval", "impactMarginNotional", "lastFundingRate", "maxFundingRate", "minFundingRate", "nextFundingRate"},
		"tier":    {"symbol"}, "liquidation": {"symbol"},
	}
	for channel, fields := range keeps {
		for _, field := range fields {
			t.Run("keep/"+channel+"/"+field, func(t *testing.T) {
				a := &Adapter{}
				out, _, err := a.filterObject(channel, []byte(fmt.Sprintf(`{%q:"value"}`, field)), false)
				if err != nil || !bytes.Contains(out, []byte(`"value"`)) || a.UnknownFields() != 0 {
					t.Fatalf("keep: %s unknown=%d err=%v", out, a.UnknownFields(), err)
				}
			})
		}
	}
	for channel, fields := range drops {
		for _, field := range fields {
			t.Run("drop/"+channel+"/"+field, func(t *testing.T) {
				a := &Adapter{}
				out, _, err := a.filterObject(channel, []byte(fmt.Sprintf(`{%q:"value"}`, field)), false)
				if err != nil || string(out) != "{}" || a.UnknownFields() != 0 {
					t.Fatalf("drop: %s unknown=%d err=%v", out, a.UnknownFields(), err)
				}
			})
		}
	}
}

func TestUnknownArgAndDataRetained(t *testing.T) {
	for _, frame := range []string{
		`{"arg":{"instType":"x","topic":"publicTrade","symbol":"BTCUSDT","extraArg":1},"data":[{"i":"1","extraData":2}]}`,
		`{"arg":{"instType":"x","topic":"position-tier","extraArg":1},"data":{"symbol":"BTCUSDT","list":[{"tier":"1","extraData":2}]}}`,
	} {
		a := &Adapter{}
		r := a.Split(venue.Shard{Name: t.Name() + frame}, []byte(frame))
		if len(r) == 0 || r[0].Symbol != "BTCUSDT" || !bytes.Contains(r[0].Payload, []byte(`"extraArg":1`)) || !bytes.Contains(r[0].Payload, []byte(`"extraData":2`)) || a.UnknownFields() != 2 {
			t.Fatalf("records=%+v unknown=%d", r, a.UnknownFields())
		}
	}
}
