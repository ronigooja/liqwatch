package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/ronigooja/liqwatch/internal/venuemap"
)

type fixtureFetcher map[string]string

func (f fixtureFetcher) Get(url string, out any) error {
	body, ok := f[url]
	if !ok {
		return fmt.Errorf("unexpected request: %s", url)
	}
	return json.Unmarshal([]byte(body), out)
}

func TestMEXCDiscoveryAndMergePruneMissingContracts(t *testing.T) {
	// Synthetic current-list fixture: stale ticker data must not restore an
	// absent contract; only state 0 USDT-settled perpetuals remain eligible.
	f := fixtureFetcher{
		"https://contract.mexc.com/api/v1/contract/detail": `{"data":[
			{"symbol":"BTC_USDT","baseCoin":"BTC","state":0,"futureType":1,"quoteCoin":"USDT","settleCoin":"USDT"},
			{"symbol":"ETH_USDT","baseCoin":"ETH","state":0,"futureType":1,"quoteCoin":"USDT","settleCoin":"USDT"},
			{"symbol":"HALTED_USDT","baseCoin":"HALTED","state":1,"futureType":1,"quoteCoin":"USDT","settleCoin":"USDT"},
			{"symbol":"OTHER_USDT","baseCoin":"OTHER","state":0,"futureType":2,"quoteCoin":"USDT","settleCoin":"USDT"},
			{"symbol":"BTC_USDC","baseCoin":"BTC","state":0,"futureType":1,"quoteCoin":"USDC","settleCoin":"USDC"}
		]}`,
		"https://contract.mexc.com/api/v1/contract/ticker": `{"data":[
			{"symbol":"BTC_USDT","fairPrice":100},
			{"symbol":"CROSS_USDT","fairPrice":1}
		]}`,
	}
	symbols, err := discover(f, "mexc")
	if err != nil {
		t.Fatal(err)
	}
	want := []instrument{{"BTC_USDT", "BTC", 100}, {"ETH_USDT", "ETH", 0}}
	if !reflect.DeepEqual(symbols, want) {
		t.Fatalf("discovered %+v, want %+v", symbols, want)
	}
	active := map[string]map[string]bool{"mexc": {}}
	for _, s := range symbols {
		active["mexc"][s.Symbol] = true
	}
	bn := []instrument{{"BTCUSDT", "BTC", 100}, {"ETHUSDT", "ETH", 10}, {"CROSSUSDT", "CROSS", 1}}
	fresh := propose(bn, symbols, "mexc")
	for _, status := range []string{"auto", "reviewed", "review", "rejected"} {
		t.Run(status, func(t *testing.T) {
			kept := venuemap.Pair{Binance: "ETHUSDT", Venue: "mexc", Symbol: "ETH_USDT", Status: status, Note: "no current price"}
			other := venuemap.Pair{Binance: "CROSSUSDT", Venue: "gate", Symbol: "CROSS_USDT", Status: status}
			old := []venuemap.Pair{
				{Binance: "CROSSUSDT", Venue: "mexc", Symbol: "CROSS_USDT", Status: status},
				{Binance: "HALTEDUSDT", Venue: "mexc", Symbol: "HALTED_USDT", Status: status},
				kept, other,
			}
			got, diff := merge(old, fresh, active)
			want := []venuemap.Pair{other, fresh[0], kept}
			if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(diff, []string{
				"added mexc BTC_USDT auto", "removed mexc CROSS_USDT", "removed mexc HALTED_USDT",
			}) {
				t.Fatalf("got %+v diff %v, want %+v", got, diff, want)
			}
		})
	}
}
