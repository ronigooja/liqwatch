package poller

import (
 "bytes"
 "context"
 "encoding/json"
 "os"
 "path/filepath"
 "sort"
 "strings"
 "testing"
 "time"
)

type binanceSample struct {
 Venue string `json:"venue"`
 Channel string `json:"ch"`
 Symbol string `json:"sym"`
 Meta map[string]any `json:"meta"`
 Payload json.RawMessage `json:"p"`
}

func binanceSamples(t *testing.T) []binanceSample {
 t.Helper()
 paths, err := filepath.Glob("testdata/binance/*.sample.jsonl"); if err != nil || len(paths) != 9 { t.Fatalf("samples: %v (%d)", err, len(paths)) }
 var samples []binanceSample
 for _, path := range paths { b, err := os.ReadFile(path); if err != nil { t.Fatal(err) }; var s binanceSample; if err := json.Unmarshal(b, &s); err != nil { t.Fatal(err) }; samples = append(samples, s) }
 return samples
}

func TestBinancePolicyGolden(t *testing.T) {
 var lines [][]byte
 for _, sample := range binanceSamples(t) {
  s := sample
  t.Run(s.Channel, func(t *testing.T) {
   clock := &fakeClock{time.Unix(1800000000, 0)}
   sink := &memorySink{}
   c := Channel{Venue: s.Venue, Name: s.Channel, Fetch: func(context.Context, string) ([]Record, error) { return []Record{{Venue:s.Venue, Channel:s.Channel, Symbol:s.Symbol, Meta:s.Meta, Payload:s.Payload}}, nil }}
   scheduler, err := NewScheduler([]Channel{c}, sink, clock, "", 240); if err != nil { t.Fatal(err) }
   if _, err := scheduler.Step(context.Background()); err != nil { t.Fatal(err) }
   for _, r := range sink.records { b, err := json.Marshal(binanceSample{Venue:r.Venue, Channel:r.Channel, Symbol:r.Symbol, Meta:r.Meta, Payload:r.Payload}); if err != nil { t.Fatal(err) }; lines = append(lines,b) }
  })
 }
 sort.Slice(lines,func(i,j int) bool { return bytes.Compare(lines[i],lines[j])<0 })
 got := append(bytes.Join(lines,[]byte("\n")), '\n')
 want, err := os.ReadFile("testdata/binance/golden-filtered.txt"); if err != nil { t.Fatal(err) }; if !bytes.Equal(got,want) { t.Fatal("filtered whole records differ from golden") }
}

func TestBinancePolicyClassification(t *testing.T) {
 cases := []struct{channel, raw, want string; unknown uint64}{
 {"openInterestHist", `{"symbol":"X","sumOpenInterest":1.2300,"sumOpenInterestValue":2,"timestamp":42,"CMCCirculatingSupply":5,"newField":7}`, `{"sumOpenInterest":1.2300,"sumOpenInterestValue":2,"timestamp":42,"newField":7}`, 1},
 {"fundingRate", `{"symbol":"X","fundingTime":42,"fundingRate":1.2300,"markPrice":2,"rateType":"R","newField":7}`, `{"fundingTime":42,"fundingRate":1.2300,"markPrice":2,"rateType":"R","newField":7}`, 1},
 {"brackets", `{"symbol":"X","updateTime":1,"notionalLimit":2,"riskBrackets":[{"bracketSeq":1,"bracketNotionalFloor":1.2300,"bracketNotionalCap":2,"bracketMaintenanceMarginRate":3,"cumFastMaintenanceAmount":4,"minOpenPosLeverage":5,"maxOpenPosLeverage":6,"newField":7}]}`, `{"updateTime":1,"notionalLimit":2,"riskBrackets":[{"bracketSeq":1,"bracketNotionalFloor":1.2300,"bracketNotionalCap":2,"bracketMaintenanceMarginRate":3,"cumFastMaintenanceAmount":4,"minOpenPosLeverage":5,"maxOpenPosLeverage":6,"newField":7}]}`, 1},
 }
 for _, channel := range []string{"topLongShortPositionRatio","topLongShortAccountRatio","globalLongShortAccountRatio"} { cases = append(cases, struct{channel,raw,want string; unknown uint64}{channel, `{"symbol":"X","longShortRatio":1.2300,"longAccount":2,"shortAccount":3,"timestamp":42,"newField":7}`, `{"longShortRatio":1.2300,"longAccount":2,"shortAccount":3,"timestamp":42,"newField":7}`,1}) }
 for _, tc := range cases { t.Run(tc.channel,func(t *testing.T){ before:=UnknownFields("binance",tc.channel); got,record,err:=ApplyPolicy("binance",tc.channel,[]byte(tc.raw)); if err!=nil || !record || string(got)!=tc.want || UnknownFields("binance",tc.channel)-before!=tc.unknown { t.Fatalf("got %s record=%v err=%v unknown=%d",got,record,err,UnknownFields("binance",tc.channel)-before) } }) }
 for _, channel := range []string{"constituents","takerlongshortRatio"} { _,record,err:=ApplyPolicy("binance",channel,[]byte(`{}`)); if err!=nil || record { t.Errorf("%s not dropped",channel) } }
}

func TestBinancePolicyTimestamps(t *testing.T) {
 for _, channel := range []string{"openInterestHist","topLongShortPositionRatio","topLongShortAccountRatio","globalLongShortAccountRatio","fundingRate"} { want:="timestamp"; if channel=="fundingRate" { want="fundingTime" }; policies.RLock(); p,ok:=policies.m[policyKey{"binance",channel}]; policies.RUnlock(); if !ok || p.TimestampKey!=want || p.TimestampPath!="" || p.TimestampIndex!=nil { t.Errorf("%s timestamp: %+v",channel,p) } }
}

func TestBinancePolicyDedup(t *testing.T) {
 for _, channel := range []string{"openInterestHist","fundingRate"} { t.Run(channel,func(t *testing.T){ key,value:="timestamp","sumOpenInterest"; if channel=="fundingRate" { key,value="fundingTime","fundingRate" }; row:=func(symbol, v string) []Record { return []Record{{Venue:"binance",Channel:channel,Symbol:"BTCUSDT",Meta:map[string]any{"last_row_timestamp":int64(42)},Payload:[]byte(`{"`+key+`":42,"`+value+`":`+v+`,"symbol":"`+symbol+`"}`)}} }; assertVenueDedup(t,"binance",channel,venueDedupFixture{Original:row("X","1"),Dropped:row("Y","1"),Kept:row("X","2")}) }) }
}

// Every explicitly classified field must affect either unknown counting or output.
func TestBinancePolicyAllFields(t *testing.T) {
 cases:=[]struct{channel, path, keep, drop string}{
 {"exchangeInfo","", "symbols", "timezone serverTime futuresType rateLimits exchangeFilters assets"},
 {"exchangeInfo","symbols[]", "symbol pair contractType status deliveryDate onboardDate baseAsset quoteAsset marginAsset liquidationFee", "pricePrecision quantityPrecision baseAssetPrecision quotePrecision maintMarginPercent requiredMarginPercent underlyingType underlyingSubType settlePlan triggerProtect marketTakeBound orderTypes timeInForce permissionSets filters maxMoveOrderLimit"},
 {"brackets","", "updateTime notionalLimit riskBrackets", "symbol"},
 {"brackets","riskBrackets[]", "bracketSeq bracketNotionalFloor bracketNotionalCap bracketMaintenanceMarginRate cumFastMaintenanceAmount minOpenPosLeverage maxOpenPosLeverage", ""},
 {"openInterestHist","", "sumOpenInterest sumOpenInterestValue timestamp", "symbol CMCCirculatingSupply"},
 {"fundingRate","", "fundingTime fundingRate markPrice rateType", "symbol"},
 }
 for _, c:=range []string{"topLongShortPositionRatio","topLongShortAccountRatio","globalLongShortAccountRatio"} { cases=append(cases,struct{channel,path,keep,drop string}{c,"","longShortRatio longAccount shortAccount timestamp","symbol"}) }
 for _, tc:=range cases { for _, field:=range strings.Fields(tc.keep+" "+tc.drop) { t.Run(tc.channel+"/"+tc.path+"/"+field,func(t *testing.T){ dropped:=strings.Contains(" "+tc.drop+" "," "+field+" "); v:=`1.2300`; if field=="symbols" || field=="riskBrackets" {v=`[]`}; raw:=`{"`+field+`":`+v+`}`; if tc.path!="" { raw=`{"`+strings.TrimSuffix(tc.path,"[]")+`":[`+raw+`]}` }; before:=UnknownFields("binance",tc.channel); got,record,err:=ApplyPolicy("binance",tc.channel,[]byte(raw)); if err!=nil || !record || UnknownFields("binance",tc.channel)!=before || bytes.Contains(got,[]byte(`"`+field+`"`))==dropped {t.Fatalf("field %s: %s record=%v err=%v unknown=%d",field,got,record,err,UnknownFields("binance",tc.channel)-before)} }) } }
}
