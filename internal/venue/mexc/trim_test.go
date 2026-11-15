package mexc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
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

// These independent expectations are also used by the mutation checks.
var expectedFields = map[string]struct{ keep, drop string }{
	"trade":    {"p v T O t i cts", "symbol M"},
	"mark":     {"price", "symbol"},
	"index":    {"price", "symbol"},
	"funding":  {"rate nextSettleTime", "symbol"},
	"ticker":   {"fairPrice indexPrice holdVol fundingRate nextSettleTime timestamp", "symbol lastPrice riseFallRate volume24 amount24 maxBidPrice minAskPrice lower24Price high24Price bid1 ask1 riseFallValue zone riseFallRates riseFallRatesOfTimezone"},
	"contract": {"contractSize minLeverage maxLeverage maintenanceMarginRate initialMarginRate riskBaseVol riskIncrVol riskIncrMmr riskIncrImr riskLevelLimit riskLongShortSwitch riskLimitType riskLimitMode openingTime riskLimitCustom", "symbol displayName displayNameEn positionOpenType baseCoin quoteCoin baseCoinName quoteCoinName futureType settleCoin priceScale volScale amountScale priceUnit volUnit minVol maxVol limitMaxVol bidLimitPriceRate askLimitPriceRate takerFeeRate makerFeeRate liquidationFeeRate feeRateMode priceCoefficientVariation indexOrigin state isNew isHot isHidden conceptPlate conceptPlateId tagIdList maxNumOrders depthStepList openingCountdownOption triggerProtect apiAllowed baseCoinIconUrl baseCoinId vid id fn isMaxLeverage isZeroFeeRate isZeroFeeSymbol showBeforeOpen marketOrderMaxLevel marketOrderPriceLimitRate1 marketOrderPriceLimitRate2 appraisal showAppraisalCountdown automaticDelivery stopOnlyFair threshold type typeLabel preMarket feeRateType leverageFeeRates tieredFeeRates createTime countryConfigContractMaxLeverage"},
}

func TestFieldTableEntries(t *testing.T) {
	for channel, fields := range expectedFields {
		for _, group := range []struct {
			name, fields string
			keep         bool
		}{{"keep", fields.keep, true}, {"drop", fields.drop, false}} {
			for _, field := range strings.Fields(group.fields) {
				t.Run(channel+"/"+group.name+"/"+field, func(t *testing.T) {
					a := &Adapter{}
					out, err := a.filterObject(channel, []byte(fmt.Sprintf(`{"%s":123}`, field)))
					if err != nil {
						t.Fatal(err)
					}
					if group.keep && (string(out) != fmt.Sprintf(`{"%s":123}`, field) || a.UnknownFields() != 0) {
						t.Fatalf("keep: %s unknown=%d", out, a.UnknownFields())
					}
					if !group.keep && (string(out) != "{}" || a.UnknownFields() != 0) {
						t.Fatalf("drop: %s unknown=%d", out, a.UnknownFields())
					}
				})
			}
		}
	}
}
func TestFixtureTrimming(t *testing.T) {
	channels := map[string]string{"push.deal": "trade", "push.fair.price": "mark", "push.index.price": "index", "push.funding.rate": "funding", "push.ticker": "ticker"}
	seen := map[string]bool{}
	for n, frame := range frames(t) {
		var input struct {
			Channel, Symbol string
			Data            json.RawMessage
		}
		if json.Unmarshal(frame, &input) != nil {
			t.Fatal("fixture")
		}
		ch := channels[input.Channel]
		if ch == "" {
			continue
		}
		seen[ch] = true
		a := &Adapter{}
		r := a.Split(venue.Shard{Name: fmt.Sprintf("fixture-%d", n)}, frame)
		if len(r) != 2 || r[0].Channel != ch || r[0].Symbol != input.Symbol || string(r[1].Payload) != `{"sample":`+string(frame)+`}` {
			t.Fatalf("%s: %+v", ch, r)
		}
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(r[0].Payload, &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope["symbol"] != nil || envelope["channel"] != nil || envelope["ts"] == nil {
			t.Fatalf("envelope: %s", r[0].Payload)
		}
		var before, after []map[string]json.RawMessage
		if len(input.Data) > 0 && input.Data[0] == '[' {
			json.Unmarshal(input.Data, &before)
			json.Unmarshal(envelope["data"], &after)
		} else {
			var b, c map[string]json.RawMessage
			json.Unmarshal(input.Data, &b)
			json.Unmarshal(envelope["data"], &c)
			before = []map[string]json.RawMessage{b}
			after = []map[string]json.RawMessage{c}
		}
		if len(before) != len(after) {
			t.Fatalf("shape %s", r[0].Payload)
		}
		var unknown uint64
		for i, b := range before {
			for k, v := range b {
				dropped := strings.Contains(" "+expectedFields[ch].drop+" ", " "+k+" ")
				if dropped && after[i][k] != nil {
					t.Errorf("%s retained %s", ch, k)
				}
				if !dropped && !reflect.DeepEqual(v, after[i][k]) {
					t.Errorf("%s changed %s", ch, k)
				}
				if !dropped && !strings.Contains(" "+expectedFields[ch].keep+" ", " "+k+" ") {
					unknown++
				}
			}
		}
		if a.UnknownFields() != unknown {
			t.Errorf("%s unknown %d want %d", ch, a.UnknownFields(), unknown)
		}
	}
	for _, ch := range []string{"trade", "mark", "index", "funding", "ticker"} {
		if !seen[ch] {
			t.Errorf("missing %s fixture", ch)
		}
	}
}
func TestContractFixtureAndDiscovery(t *testing.T) {
	b, err := os.ReadFile("testdata/detail.json")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(b) }))
	defer server.Close()
	a := &Adapter{DetailURL: server.URL}
	syms, err := a.Symbols(context.Background())
	if err != nil || len(syms) != 4 {
		t.Fatalf("discovery %v %v", syms, err)
	}
	var detail struct{ Data []json.RawMessage }
	if json.Unmarshal(b, &detail) != nil || len(detail.Data) == 0 {
		t.Fatal("detail fixture")
	}
	a.Shards(context.Background(), syms)
	frame := []byte(fmt.Sprintf(`{"channel":"push.contract","symbol":"BTC_USDT","data":%s,"ts":123}`, detail.Data[0]))
	r := a.Split(venue.Shard{Name: "contract-fixture"}, frame)
	if len(r) != 2 || r[0].Symbol != "BTC_USDT" || string(r[1].Payload) != `{"sample":`+string(frame)+`}` {
		t.Fatalf("records %+v", r)
	}
	var before, after map[string]json.RawMessage
	var env map[string]json.RawMessage
	json.Unmarshal(detail.Data[0], &before)
	json.Unmarshal(r[0].Payload, &env)
	json.Unmarshal(env["data"], &after)
	if env["symbol"] != nil || env["channel"] != nil || string(env["ts"]) != "123" {
		t.Fatalf("envelope %s", r[0].Payload)
	}
	var unknown uint64
	for k, v := range before {
		dropped := strings.Contains(" "+expectedFields["contract"].drop+" ", " "+k+" ")
		if dropped && after[k] != nil {
			t.Errorf("retained %s", k)
		}
		if !dropped && !reflect.DeepEqual(v, after[k]) {
			t.Errorf("changed %s", k)
		}
		if !dropped && !strings.Contains(" "+expectedFields["contract"].keep+" ", " "+k+" ") {
			unknown++
			t.Logf("unknown contract field: %s", k)
		}
	}
	if a.UnknownFields() != unknown {
		t.Errorf("unknown %d want %d", a.UnknownFields(), unknown)
	}
	for _, name := range []string{"tempMaxLeverageLimited", "regularMaxLeverage"} {
		if keepFields["contract"][name] || dropFields["contract"][name] || !reflect.DeepEqual(before[name], after[name]) || before[name] == nil {
			t.Errorf("unknown field %s not retained and counted", name)
		}
	}
	if len(before["riskLimitCustom"]) == 0 || !reflect.DeepEqual(before["riskLimitCustom"], after["riskLimitCustom"]) {
		t.Error("custom risk tiers not preserved")
	}
	// Discovery must still apply every REST filter even though those fields are dropped from records.
	cases := `{"success":true,"code":0,"data":[{"symbol":"OK","state":0,"futureType":1,"quoteCoin":"USDT","settleCoin":"USDT"},{"symbol":"STATE","state":1,"futureType":1,"quoteCoin":"USDT","settleCoin":"USDT"},{"symbol":"TYPE","state":0,"futureType":2,"quoteCoin":"USDT","settleCoin":"USDT"},{"symbol":"QUOTE","state":0,"futureType":1,"quoteCoin":"USD","settleCoin":"USDT"},{"symbol":"SETTLE","state":0,"futureType":1,"quoteCoin":"USDT","settleCoin":"USD"}]}`
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, cases) }))
	defer server2.Close()
	a.DetailURL = server2.URL
	got, err := a.Symbols(context.Background())
	if err != nil || !reflect.DeepEqual(got, []string{"OK"}) {
		t.Fatalf("filtered discovery %v %v", got, err)
	}
}
