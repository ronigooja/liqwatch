package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/bitget"
	"github.com/ronigooja/liqwatch/internal/venue/bybit"
	"github.com/ronigooja/liqwatch/internal/venue/gate"
	"github.com/ronigooja/liqwatch/internal/venue/kucoin"
	"github.com/ronigooja/liqwatch/internal/venue/mexc"
	"github.com/ronigooja/liqwatch/internal/venue/okx"
)

func TestVenueCoverageRecords(t *testing.T) {
	// Values from the adapters' captured fixtures, except MEXC (public contract-data
	// example). The global liquidation batches also contain one out-of-scope market.
	inst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"code":"0","data":[{"instId":"NEAR-USDT-SWAP","instType":"SWAP","state":"live","ctType":"linear","settleCcy":"USDT","instFamily":"NEAR-USDT"}]}`)
	}))
	defer inst.Close()
	x := &okx.Adapter{InstrumentsURL: inst.URL}
	if _, err := x.Symbols(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		adapter                      venue.Adapter
		channel, symbol, frame, want string
		more                         string
	}{
		{&bybit.Adapter{}, "liquidation", "AVAXUSDT",
			`{"topic":"allLiquidation.AVAXUSDT","type":"snapshot","ts":1794706617301,"data":[{"T":1794706617117,"s":"AVAXUSDT","S":"Buy","v":"75.0","p":"6.764"},{"T":1794706617291,"s":"AVAXUSDT","S":"Buy","v":"7.6","p":"6.764"}]}`,
			`{"ts":1794706617301,"data":[{"T":1794706617117,"S":"Buy","v":"75.0","p":"6.764"}]}`,
			`{"ts":1794706617301,"data":[{"T":1794706617291,"S":"Buy","v":"7.6","p":"6.764"}]}`},
		{x, "liquidation", "NEAR-USDT-SWAP",
			`{"arg":{"channel":"liquidation-orders","instType":"SWAP"},"data":[{"details":[{"bkLoss":"0","bkPx":"3.553","ccy":"","posSide":"short","side":"buy","sz":"18","ts":"1794554436256"}],"instFamily":"NEAR-USDT","instId":"NEAR-USDT-SWAP","instType":"SWAP","uly":"NEAR-USDT"},{"instId":"NEAR-USD-SWAP","instType":"SWAP","details":[{"side":"buy","sz":"1"}]}]}`,
			`{"data":[{"details":[{"bkPx":"3.553","posSide":"short","side":"buy","sz":"18","ts":"1794554436256"}]}]}`, ""},
		{&bitget.Adapter{}, "liquidation", "BTCUSDT",
			`{"action":"update","arg":{"instType":"usdt-futures","topic":"liquidation"},"data":[{"symbol":"AEROUSDT","side":"buy","price":"0.483120660124","amount":"98.556614665296","ts":"1794554760239"},{"symbol":"BTCUSDT","side":"buy","price":"68720.8077713294792112","amount":"35631.738829434334971","ts":"1794554940816"}],"ts":1794554942738}`,
			`{"action":"update","data":[{"side":"buy","price":"68720.8077713294792112","amount":"35631.738829434334971","ts":"1794554940816"}],"ts":1794554942738}`, ""},
		{&gate.Adapter{}, "liquidations", "BTC_USDT",
			`{"time":1794554971,"time_ms":1794554971370,"channel":"futures.public_liquidates","event":"update","result":[{"contract":"AKE_USDT","price":"0.004758","size":"111","time":1794554458458},{"contract":"BTC_USDT","price":"68758.2","size":"-76","time":1794554970431},{"contract":"BTC_USDT","price":"68755.6","size":"-8","time":1794554971312}]}`,
			`{"time":1794554971,"time_ms":1794554971370,"result":[{"price":"68758.2","size":"-76","time":1794554970431}]}`,
			`{"time":1794554971,"time_ms":1794554971370,"result":[{"price":"68755.6","size":"-8","time":1794554971312}]}`},
		{&gate.Adapter{}, "contract_info", "ICP_USDT",
			`{"time":1794706492,"time_ms":1794706492396,"channel":"futures.contract_info","event":"all","result":{"contract":"ICP_USDT","quanto_multiplier":"0.001","order_price_round":"0.001","risk_limit_tiers":[{"tier":1,"risk_limit":"20000","initial_rate":"0.02","maintenance_rate":"0.01","leverage_max":"50","deduction":"0"}]}}`,
			`{"time":1794706492,"time_ms":1794706492396,"event":"all","result":{"quanto_multiplier":"0.001","risk_limit_tiers":[{"tier":1,"risk_limit":"20000","initial_rate":"0.02","maintenance_rate":"0.01","leverage_max":"50","deduction":"0"}]}}`, ""},
		{&mexc.Adapter{}, "contract", "CLO_USDT",
			`{"channel":"push.contract","data":{"contractSize":10,"displayNameEn":"CLO_USDT PERPETUAL","maxLeverage":50,"minLeverage":1,"symbol":"CLO_USDT"},"symbol":"CLO_USDT","ts":1760942212002}`,
			`{"data":{"contractSize":10,"maxLeverage":50,"minLeverage":1},"ts":1760942212002}`, ""},
		{&kucoin.Adapter{}, "utamark", "SOLUSDTM",
			`{"T":"mark-price","P":1794706499432698555,"d":{"s":"SOLUSDTM","mp":"68.247","ip":"68.287","oi":"16305024","ts":1794706499000}}`,
			`{"P":1794706499432698555,"d":{"mp":"68.247","ip":"68.287","oi":"16305024","ts":1794706499000}}`, ""},
	} {
		t.Run(tc.adapter.Name()+"/"+tc.channel, func(t *testing.T) {
			if tc.channel != "utamark" { // KuCoin Shards would acquire a live WS token.
				if _, err := tc.adapter.Shards(context.Background(), []venue.Symbol{tc.symbol}); err != nil {
					t.Fatal(err)
				}
			}
			var want []rawlog.Record
			for _, payload := range []string{tc.want, tc.more, `{"sample":` + tc.frame + `}`} {
				if payload != "" {
					want = append(want, rawlog.Record{Channel: tc.channel, Symbol: tc.symbol, Payload: []byte(payload)})
				}
			}
			got := tc.adapter.Split(venue.Shard{Name: t.Name()}, []byte(tc.frame))
			if !reflect.DeepEqual(got, want) {
				for _, r := range got {
					t.Logf("got %s/%s: %s", r.Channel, r.Symbol, r.Payload)
				}
				t.Fatalf("want %s/%s: %s, then unfiltered sample", tc.channel, tc.symbol, tc.want)
			}
		})
	}
}
