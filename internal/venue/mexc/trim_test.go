package mexc

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ronigooja/liqwatch/internal/venue"
)

func TestOrdinaryTradeTrimming(t *testing.T) {
	a := &Adapter{}
	a.Shards(context.Background(), []string{"BTC_USDT"})
	frame := []byte(`{"channel":"push.deal","symbol":"BTC_USDT","data":[{"p":1,"v":2,"T":3,"O":4,"M":5,"t":6,"i":7,"cts":8,"symbol":"BTC_USDT","unknown":9}],"ts":10}`)
	records := a.Split(venue.Shard{Name: "trim-trade"}, frame)
	if len(records) != 2 || records[0].Symbol != "BTC_USDT" || string(records[1].Payload) != `{"sample":`+string(frame)+`}` {
		t.Fatalf("records: %+v", records)
	}
	var payload struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	var envelope map[string]json.RawMessage
	if json.Unmarshal(records[0].Payload, &payload) != nil || json.Unmarshal(records[0].Payload, &envelope) != nil || len(payload.Data) != 1 {
		t.Fatalf("payload: %s", records[0].Payload)
	}
	for _, field := range []string{"channel", "symbol"} {
		if _, ok := envelope[field]; ok {
			t.Errorf("envelope retained %s", field)
		}
	}
	for _, field := range []string{"M", "symbol"} {
		if _, ok := payload.Data[0][field]; ok {
			t.Errorf("data retained %s", field)
		}
	}
	for _, field := range []string{"p", "v", "T", "O", "t", "i", "cts", "unknown"} {
		if _, ok := payload.Data[0][field]; !ok {
			t.Errorf("data missing %s", field)
		}
	}
	if a.UnknownFields() != 1 {
		t.Errorf("unknown: %d", a.UnknownFields())
	}
}

func TestContractExtraIsIgnored(t *testing.T) {
	a := &Adapter{}
	a.Shards(context.Background(), []string{"BTC_USDT"})
	frame := []byte(`{"channel":"push.contract.extra.update","symbol":"BTC_USDT","data":{"unknown":1}}`)
	if got := a.Split(venue.Shard{Name: "trim-extra"}, frame); len(got) != 0 || a.UnknownFields() != 0 {
		t.Fatalf("records: %+v unknown: %d", got, a.UnknownFields())
	}
}
