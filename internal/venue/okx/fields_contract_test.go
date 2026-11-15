package okx

import (
 "encoding/json"
 "reflect"
 "strings"
 "testing"
 "time"

 "github.com/ronigooja/liqwatch/internal/venue"
)

func TestOrdinaryFieldContract(t *testing.T) {
 cases := []struct{ channel, keep, drop string }{
  {"trades-all", "tradeId px sz side ts", "instId instType source"},
  {"mark-price", "markPx ts", "instId instType"},
  {"index-tickers", "instId idxPx ts", "instType open24h high24h low24h sodUtc0 sodUtc8"},
  {"funding-rate", "fundingRate fundingTime nextFundingTime ts", "instId instType nextFundingRate settFundingRate settState formulaType method impactValue interestRate maxFundingRate minFundingRate premium prevFundingTime"},
  {"open-interest", "oi oiCcy oiUsd ts", "instId instType"},
  {"liquidation-orders", "details", "instId instType uly instFamily"},
 }
 for _, tc := range cases { t.Run(tc.channel, func(t *testing.T) {
  item := map[string]any{"instId":"BTC-USDT-SWAP", "extra":"unknown"}
  if tc.channel == "index-tickers" { item["instId"] = "BTC-USDT" }
  for _, k := range strings.Fields(tc.keep+" "+tc.drop) { if k != "instId" && k != "details" { item[k] = k+"-value" } }
  item["details"] = []any{map[string]any{"side":"sell","posSide":"long","bkPx":"1","sz":"2","ts":"3","ccy":"USDT","bkLoss":"4","extraDetail":"unknown"}}
  if tc.channel != "liquidation-orders" { delete(item,"details") }
  frame, _ := json.Marshal(map[string]any{"arg":map[string]any{"channel":tc.channel,"instId":item["instId"],"instType":"SWAP"},"data":[]any{item}})
  a := &Adapter{universe:map[string]bool{"BTC-USDT-SWAP":true},indexSymbols:map[string]string{"BTC-USDT":"BTC-USDT-SWAP"}}
  sh := venue.Shard{Name:"contract-"+tc.channel}
  sampler.Lock(); delete(sampler.last, sh.Name+"\x00"+map[string]string{"trades-all":"trade","mark-price":"mark","index-tickers":"index","funding-rate":"funding","open-interest":"oi","liquidation-orders":"liquidation"}[tc.channel]); sampler.Unlock()
  before:=unknownFields.Load(); records:=a.Split(sh,frame)
  if len(records)!=2 || records[0].Symbol!="BTC-USDT-SWAP" { t.Fatalf("records: %+v",records) }
  var payload map[string]any; if err:=json.Unmarshal(records[0].Payload,&payload); err!=nil { t.Fatal(err) }
  if _,ok:=payload["arg"]; ok { t.Errorf("arg retained: %s",records[0].Payload) }
  got:=payload["data"].([]any)[0].(map[string]any)
  for _,k:=range strings.Fields(tc.keep) { if k!="details" && !reflect.DeepEqual(got[k],item[k]) { t.Errorf("keep %s: got %v want %v",k,got[k],item[k]) } }
  for _,k:=range strings.Fields(tc.drop) { if _,ok:=got[k];ok { t.Errorf("drop %s retained",k) } }
  if got["extra"]!="unknown" { t.Error("unknown lost") }
  if tc.channel=="liquidation-orders" { d:=got["details"].([]any)[0].(map[string]any); orig:=item["details"].([]any)[0].(map[string]any); for _,k:=range strings.Fields("side posSide bkPx sz ts extraDetail") { if d[k]!=orig[k] { t.Errorf("detail keep %s",k) } }; for _,k:=range []string{"ccy","bkLoss"} { if _,ok:=d[k];ok { t.Errorf("detail drop %s",k) } } }
  wantUnknown:=uint64(1); if tc.channel=="liquidation-orders" { wantUnknown++ }; if delta:=unknownFields.Load()-before; delta!=wantUnknown { t.Errorf("unknown count %d want %d",delta,wantUnknown) }
  var sample struct { Sample json.RawMessage `json:"sample"` }; if json.Unmarshal(records[1].Payload,&sample)!=nil || !reflect.DeepEqual([]byte(sample.Sample),frame) { t.Error("sample changed") }
  _ = time.Second
 }) }
}
