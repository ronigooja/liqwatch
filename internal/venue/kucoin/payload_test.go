package kucoin

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

func TestPayloadFields(t *testing.T) {
	oldNow, oldLast := samples.now, samples.last
	defer func() { samples.now, samples.last = oldNow, oldLast }()
	var trade, mark, funding, uta []byte
	for _, f := range fixtureFrames(t) {
		s := string(f)
		switch {
		case trade == nil && strings.Contains(s, `"subject":"match"`):
			trade = f
		case mark == nil && strings.Contains(s, `"subject":"mark.index.price"`):
			mark = f
		case funding == nil && strings.Contains(s, `"subject":"funding.rate"`):
			funding = f
		case uta == nil && strings.Contains(s, `"T":"mark-price"`):
			uta = f
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
		{"utamark", "SOLUSDTM", "d", uta, []string{"T", "d.s"}, []string{"P", "d.mp", "d.ip", "d.oi", "d.ts"}},
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

func TestUTAMarkRealGolden(t *testing.T) {
	data, err := os.ReadFile("testdata/utamark.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	frames := strings.Split(strings.TrimSpace(string(data)), "\n")
	want := []rawlog.Record{
		{Channel: "utamark", Symbol: "SOLUSDTM", Payload: []byte(`{"P":1794706499432698555,"d":{"mp":"68.247","ip":"68.287","oi":"16305024","ts":1794706499000}}`)},
		{Channel: "utamark", Symbol: "ETHUSDTM", Payload: []byte(`{"P":1794706500350034897,"d":{"mp":"1720.92","ip":"1721.53","oi":"18332669","ts":1794706500000}}`)},
		{Channel: "utamark", Symbol: "XBTUSDTM", Payload: []byte(`{"P":1794706499494869441,"d":{"mp":"62410.1","ip":"62440.41","oi":"9301076","ts":1794706499000}}`)},
	}
	if len(frames) != len(want) {
		t.Fatalf("got %d frames", len(frames))
	}
	oldNow, oldLast := samples.now, samples.last
	defer func() { samples.now, samples.last = oldNow, oldLast }()
	for i, frame := range frames {
		t.Run(want[i].Symbol, func(t *testing.T) {
			samples.last = map[string]time.Time{}
			before := unknownFields.Load()
			got := (&Adapter{}).Split(venue.Shard{Name: "golden"}, []byte(frame))
			sample := rawlog.Record{Channel: "utamark", Symbol: want[i].Symbol, Payload: []byte(`{"sample":` + frame + `}`)}
			if !reflect.DeepEqual(got, []rawlog.Record{want[i], sample}) {
				t.Fatalf("got %#v", got)
			}
			if n := unknownFields.Load() - before; n != 1 {
				t.Fatalf("unknown fields = %d, want 1", n)
			}
		})
	}
}
