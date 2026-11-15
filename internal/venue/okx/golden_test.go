package okx

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/venuetest"
)

func TestRecordedGolden(t *testing.T) {
	cases := []struct{ channel, symbol, want string }{
		{"trades-all", "BTC-USDT-SWAP", `{"data":[{"tradeId":"3071358636","px":"69150","sz":"12.43","side":"buy","ts":"1794554432738"}]}`},
		{"mark-price", "BTC-USDT-SWAP", `{"data":[{"markPx":"69148.5","ts":"1794554432074"}]}`},
		{"index-tickers", "BTC-USDT-SWAP", `{"data":[{"instId":"BTC-USDT","idxPx":"69182.9","ts":"1794554432091"}]}`},
		{"funding-rate", "BTC-USDT-SWAP", `{"data":[{"fundingRate":"-0.0000469094731554","fundingTime":"1794556800000","nextFundingTime":"1794585600000","ts":"1794554408854"}]}`},
		{"open-interest", "ETH-USDT-SWAP", `{"data":[{"oi":"7092583.3300000119","oiCcy":"709258.33300000119","oiUsd":"1378925865.8519423135742","ts":"1794554423524"}]}`},
		{"liquidation-orders", "NEAR-USDT-SWAP", `{"data":[{"details":[{"bkPx":"3.553","posSide":"short","side":"buy","sz":"18","ts":"1794554436256"}]}]}`},
	}
	frames := append(venuetest.Fixture(t, "testdata/public.jsonl"), venuetest.Fixture(t, "testdata/business.jsonl")...)
	for _, tc := range cases {
		t.Run(tc.channel, func(t *testing.T) {
			var frame []byte
			for _, f := range frames {
				var v struct {
					Arg struct {
						Channel string `json:"channel"`
					} `json:"arg"`
					Data []json.RawMessage `json:"data"`
				}
				if json.Unmarshal(f, &v) == nil && v.Arg.Channel == tc.channel && len(v.Data) > 0 {
					frame = f
					break
				}
			}
			if frame == nil {
				t.Fatal("missing recorded frame")
			}
			a := &Adapter{universe: map[string]bool{tc.symbol: true}, indexSymbols: map[string]string{"BTC-USDT": "BTC-USDT-SWAP"}}
			sh := venue.Shard{Name: "golden-" + tc.channel}
			sampler.Lock()
			sampler.last = map[string]time.Time{}
			sampler.Unlock()
			before := unknownFields.Load()
			got := a.Split(sh, frame)
			if len(got) != 2 {
				t.Fatalf("records %+v", got)
			}
			if got[0].Symbol != tc.symbol || string(got[0].Payload) != tc.want {
				t.Errorf("symbol %s payload %s; want %s %s", got[0].Symbol, got[0].Payload, tc.symbol, tc.want)
			}
			if n := unknownFields.Load() - before; n != 0 {
				t.Errorf("unexpected unknown fields: %d", n)
			}
			if !bytes.Equal(got[1].Payload, append(append([]byte(`{"sample":`), frame...), '}')) {
				t.Errorf("sample not complete: %s", got[1].Payload)
			}
		})
	}
}
