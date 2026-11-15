package binance

import (
	"context"
	"reflect"
	"testing"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

func TestAdapterSplit(t *testing.T) {
	a := &Adapter{MaxStreamsPerWS: 2}
	shards, err := a.Shards(context.Background(), []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"})
	if err != nil || len(shards) != 3 || len(shards[0].Streams) != 2 || shards[0].URL != WSMarket+"btcusdt@aggTrade/ethusdt@aggTrade" {
		t.Fatalf("shards=%+v err=%v", shards, err)
	}
	for _, tt := range []struct {
		input string
		want  []rawlog.Record
	}{
		{`{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT","a":42}}`, []rawlog.Record{{Channel: "aggTrade", Symbol: "BTCUSDT", Payload: []byte(`{"data":{"a":42}}`)}}},
		{`{"stream":"!markPrice@arr@1s","data":[{"s":"BTCUSDT","p":"1"},{"s":"OUT"}]}`, []rawlog.Record{{Channel: "markPrice_all", Symbol: "BTCUSDT", Payload: []byte(`{"data":{"p":"1"}}`)}}},
		{`{"stream":"!forceOrder@arr","data":{"o":{"s":"BTCUSDT"}}}`, []rawlog.Record{{Channel: "forceOrder_all", Symbol: "BTCUSDT", Payload: []byte(`{"stream":"!forceOrder@arr","data":{"o":{"s":"BTCUSDT"}}}`)}}},
		{`{"result":null,"id":1}`, nil},
		{`{"hello":"world"}`, nil},
		{`{"stream":"btcusdt@newThing","data":{"x":1}}`, []rawlog.Record{{Channel: "newThing", Symbol: "BTCUSDT", Payload: []byte(`{"stream":"btcusdt@newThing","data":{"x":1}}`)}}},
		{`{"data":{"x":1}}`, []rawlog.Record{{Channel: "gap", Payload: []byte(`{"bytes":16,"event":"unrecognized_frame","head":"{\"data\":{\"x\":1}}"}`)}}},
	} {
		if got := a.Split(venue.Shard{}, []byte(tt.input)); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("Split(%s)=%+v want %+v", tt.input, got, tt.want)
		}
	}
}
