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

func TestMEXCDiscoveryDoesNotRegenerateRemovedAssociations(t *testing.T) {
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
			{"symbol":"CROSS_USDT","fairPrice":1},
			{"symbol":"CTK_USDT","fairPrice":1},
			{"symbol":"DASH_USDT","fairPrice":1},
			{"symbol":"RIF_USDT","fairPrice":1},
			{"symbol":"XMR_USDT","fairPrice":1}
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
	bn := []instrument{{"BTCUSDT", "BTC", 100}, {"ETHUSDT", "ETH", 10}}
	for _, base := range []string{"CROSS", "CTK", "DASH", "RIF", "XMR"} {
		bn = append(bn, instrument{base + "USDT", base, 1})
	}
	fresh := propose(bn, symbols, "mexc")
	wantPairs := []venuemap.Pair{{Binance: "BTCUSDT", Venue: "mexc", Symbol: "BTC_USDT", Status: "auto", Note: "ratio=1"}}
	if !reflect.DeepEqual(fresh, wantPairs) {
		t.Fatalf("proposed %+v, want %+v", fresh, wantPairs)
	}
	// The checked-in cleanup removes only MEXC associations, not other venues.
	other := venuemap.Pair{Binance: "CROSSUSDT", Venue: "gate", Symbol: "CROSS_USDT", Status: "reviewed"}
	got, _ := merge([]venuemap.Pair{other}, fresh, map[string]bool{"mexc": true})
	if !reflect.DeepEqual(got, append([]venuemap.Pair{other}, wantPairs...)) {
		t.Fatalf("stale associations regenerated: %+v", got)
	}
}

func TestMergePreservesDecisionsAcrossIncompleteDiscovery(t *testing.T) {
	for _, status := range []string{"auto", "reviewed", "review", "rejected"} {
		t.Run(status, func(t *testing.T) {
			old := []venuemap.Pair{
				{Binance: "ETHUSDT", Venue: "mexc", Symbol: "ETH_USDT", Exp10: 3, Status: status, Note: "absent from partial response"},
				{Binance: "HALTEDUSDT", Venue: "mexc", Symbol: "HALTED_USDT", Exp10: 2, Status: status, Note: "temporarily suspended"},
			}
			got, _ := merge(old, nil, map[string]bool{"mexc": true})
			if !reflect.DeepEqual(got, old) {
				t.Fatalf("incomplete discovery changed decisions: %+v, want %+v", got, old)
			}
			fresh := append([]venuemap.Pair(nil), old...)
			for i := range fresh {
				fresh[i].Status = "auto"
				fresh[i].Exp10 = 0
				fresh[i].Note = "ratio=1"
			}
			returned, _ := merge(got, fresh, map[string]bool{"mexc": true})
			want := old
			if status == "auto" {
				want = fresh
			}
			if !reflect.DeepEqual(returned, want) {
				t.Fatalf("returning discovery lost decisions: %+v, want %+v", returned, want)
			}
		})
	}
}
