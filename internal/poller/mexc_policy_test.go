package poller

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

const mexcKeep = "contractSize minLeverage maxLeverage countryConfigContractMaxLeverage maintenanceMarginRate initialMarginRate riskBaseVol riskIncrVol riskIncrMmr riskIncrImr riskLevelLimit riskLongShortSwitch riskLimitType riskLimitMode riskLimitCustom"
const mexcDrop = "symbol displayName displayNameEn fn baseCoin quoteCoin baseCoinName quoteCoinName settleCoin positionOpenType futureType priceScale volScale amountScale priceUnit volUnit minVol maxVol limitMaxVol maxNumOrders bidLimitPriceRate askLimitPriceRate marketOrderMaxLevel marketOrderPriceLimitRate1 marketOrderPriceLimitRate2 triggerProtect threshold takerFeeRate makerFeeRate liquidationFeeRate feeRateMode feeRateType leverageFeeRates tieredFeeRates priceCoefficientVariation indexOrigin state automaticDelivery type typeLabel preMarket stopOnlyFair apiAllowed isNew isHot isHidden isMaxLeverage isZeroFeeRate isZeroFeeSymbol showBeforeOpen openingCountdownOption appraisal showAppraisalCountdown conceptPlate conceptPlateId tagIdList depthStepList baseCoinIconUrl baseCoinId vid id createTime openingTime tempMaxLeverageLimited regularMaxLeverage tempFeeRateEffectiveStartTime"

func TestMEXCPolicyFields(t *testing.T) {
	for _, group := range []struct {
		fields string
		keep   bool
	}{{mexcKeep, true}, {mexcDrop, false}} {
		for _, name := range strings.Fields(group.fields) {
			t.Run(name, func(t *testing.T) {
				value := `123`
				if name == "riskLimitCustom" {
					value = `[{"level":1,"maxVol":2,"mmr":3,"imr":4,"maxLeverage":5}]`
				}
				raw := []byte(fmt.Sprintf(`{"%s":%s}`, name, value))
				before := UnknownFields("mexc", "contractDetail")
				got, record, err := ApplyPolicy("mexc", "contractDetail", raw)
				if err != nil || !record {
					t.Fatal(err)
				}
				want := raw
				if !group.keep {
					want = []byte(`{}`)
				}
				if !bytes.Equal(got, want) || UnknownFields("mexc", "contractDetail") != before {
					t.Fatalf("got %s unknown delta %d", got, UnknownFields("mexc", "contractDetail")-before)
				}
			})
		}
	}
	for _, name := range strings.Fields("level maxVol mmr imr maxLeverage") {
		t.Run("nested/"+name, func(t *testing.T) {
			before := UnknownFields("mexc", "contractDetail")
			raw := []byte(fmt.Sprintf(`{"riskLimitCustom":[{"%s":1}]}`, name))
			got, _, err := ApplyPolicy("mexc", "contractDetail", raw)
			if err != nil || !bytes.Equal(got, raw) || UnknownFields("mexc", "contractDetail") != before {
				t.Fatalf("%s %v", got, err)
			}
		})
	}
	before := UnknownFields("mexc", "contractDetail")
	got, _, err := ApplyPolicy("mexc", "contractDetail", []byte(`{"novel":1,"riskLimitCustom":[{"novel":2}]}`))
	if err != nil || string(got) != `{"novel":1,"riskLimitCustom":[{"novel":2}]}` || UnknownFields("mexc", "contractDetail")-before != 2 {
		t.Fatalf("unknown: %s %v", got, err)
	}
}
func TestMEXCRecords(t *testing.T) {
	f, err := os.Open("testdata/mexc/records.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		n++
		var rec struct {
			Payload json.RawMessage `json:"p"`
		}
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatal(err)
		}
		var input, output map[string]json.RawMessage
		json.Unmarshal(rec.Payload, &input)
		before := UnknownFields("mexc", "contractDetail")
		filtered, record, err := ApplyPolicy("mexc", "contractDetail", rec.Payload)
		if err != nil || !record {
			t.Fatal(err)
		}
		json.Unmarshal(filtered, &output)
		for k, v := range input {
			keep := strings.Contains(" "+mexcKeep+" ", " "+k+" ")
			drop := strings.Contains(" "+mexcDrop+" ", " "+k+" ")
			if !keep && !drop {
				t.Errorf("unexpected field %s", k)
			}
			if keep && !bytes.Equal(v, output[k]) {
				t.Errorf("changed %s", k)
			}
			if drop && output[k] != nil {
				t.Errorf("retained %s", k)
			}
		}
		if UnknownFields("mexc", "contractDetail") != before {
			t.Error("sample contains unknown fields")
		}
	}
	if sc.Err() != nil || n != 12 {
		t.Fatalf("samples %d %v", n, sc.Err())
	}
}
func TestMEXCVenueGolden(t *testing.T) { assertVenueGolden(t, "mexc") }
func TestMEXCVenueDedup(t *testing.T) {
	row := func(s string) []Record {
		return []Record{{Venue: "mexc", Channel: "contractDetail", Symbol: "BTC_USDT", Meta: map[string]any{"last_row_timestamp": int64(42)}, Payload: []byte(s)}}
	}
	assertVenueDedup(t, "mexc", "contractDetail", venueDedupFixture{Original: row(`{"contractSize":1,"symbol":"A"}`), Dropped: row(`{"contractSize":1,"symbol":"B"}`), Kept: row(`{"contractSize":2,"symbol":"A"}`)})
}

// TestMEXCPolicyMutationResistance checks each classification against independent
// expectations, including unknown counters (removing a keep retains the value).
func TestMEXCPolicyMutationResistance(t *testing.T) {
	key := policyKey{"mexc", "contractDetail"}
	policies.RLock()
	original := policies.m[key]
	policies.RUnlock()
	check := func() bool {
		for _, row := range mexcCaptured(t) {
			var beforeFields, afterFields map[string]json.RawMessage
			if json.Unmarshal(row.Payload, &beforeFields) != nil {
				return false
			}
			before := UnknownFields("mexc", "contractDetail")
			got, record, err := ApplyPolicy("mexc", "contractDetail", row.Payload)
			if err != nil || !record || json.Unmarshal(got, &afterFields) != nil || UnknownFields("mexc", "contractDetail") != before {
				return false
			}
			for name, value := range beforeFields {
				keep := strings.Contains(" "+mexcKeep+" ", " "+name+" ")
				drop := strings.Contains(" "+mexcDrop+" ", " "+name+" ")
				if !keep && !drop || keep && !bytes.Equal(value, afterFields[name]) || drop && afterFields[name] != nil {
					return false
				}
			}
		}
		for _, group := range []struct {
			names        string
			keep, nested bool
		}{{mexcKeep, true, false}, {mexcDrop, false, false}, {"level maxVol mmr imr maxLeverage", true, true}} {
			for _, name := range strings.Fields(group.names) {
				value := `123`
				if name == "riskLimitCustom" {
					value = `[]`
				}
				raw := []byte(fmt.Sprintf(`{"%s":%s}`, name, value))
				if group.nested {
					raw = []byte(fmt.Sprintf(`{"riskLimitCustom":[{"%s":123}]}`, name))
				}
				before := UnknownFields("mexc", "contractDetail")
				got, record, err := ApplyPolicy("mexc", "contractDetail", raw)
				want := raw
				if !group.keep {
					want = []byte(`{}`)
				}
				if err != nil || !record || !bytes.Equal(got, want) || UnknownFields("mexc", "contractDetail") != before {
					return false
				}
			}
		}
		return true
	}
	if !check() {
		t.Fatal("baseline classification incorrect")
	}
	remove := func(xs []string, name string) []string {
		out := make([]string, 0, len(xs))
		for _, x := range xs {
			if x != name {
				out = append(out, x)
			}
		}
		return out
	}
	for _, group := range []struct{ names, kind string }{{mexcKeep, "keep"}, {mexcDrop, "drop"}, {"level maxVol mmr imr maxLeverage", "nested"}} {
		for _, name := range strings.Fields(group.names) {
			for _, mode := range []string{"remove", "reverse"} {
				t.Run(group.kind+"/"+name+"/"+mode, func(t *testing.T) {
					p := original
					p.Keep = append([]string(nil), original.Keep...)
					p.Drop = append([]string(nil), original.Drop...)
					nested := original.Nested["riskLimitCustom[]"]
					nested.Keep = append([]string(nil), nested.Keep...)
					nested.Drop = append([]string(nil), nested.Drop...)
					p.Nested = map[string]Policy{"riskLimitCustom[]": nested}
					switch group.kind {
					case "keep":
						p.Keep = remove(p.Keep, name)
						if mode == "reverse" {
							p.Drop = append(p.Drop, name)
						}
					case "drop":
						p.Drop = remove(p.Drop, name)
						if mode == "reverse" {
							p.Keep = append(p.Keep, name)
						}
					case "nested":
						nested.Keep = remove(nested.Keep, name)
						if mode == "reverse" {
							nested.Drop = append(nested.Drop, name)
						}
						p.Nested["riskLimitCustom[]"] = nested
					}
					policies.Lock()
					policies.m[key] = p
					policies.Unlock()
					defer func() { policies.Lock(); policies.m[key] = original; policies.Unlock() }()
					if check() {
						t.Fatal("mutated policy accepted")
					}
				})
			}
		}
	}
}

func mexcCaptured(t *testing.T) []Record {
	t.Helper()
	f, err := os.Open("testdata/mexc/records.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var rows []Record
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r struct {
			Symbol  string          `json:"sym"`
			Payload json.RawMessage `json:"p"`
		}
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, Record{Venue: "mexc", Channel: "contractDetail", Symbol: r.Symbol, Payload: r.Payload, Meta: map[string]any{"snapshot": true}})
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 12 {
		t.Fatal(len(rows))
	}
	return rows
}
func TestMEXCContractDetailGolden(t *testing.T) {
	rows := mexcCaptured(t)
	clock := &fakeClock{time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)}
	sink := &memorySink{}
	ch := Channel{Venue: "mexc", Name: "contractDetail", Fetch: func(context.Context, string) ([]Record, error) { return rows, nil }}
	scheduler, err := NewScheduler([]Channel{ch}, sink, clock, "", 240)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := json.MarshalIndent(sink.records, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	path := "testdata/mexc/golden-contractDetail.txt"
	if os.Getenv("GENERATE_MEXC_CONTRACT_GOLDEN") == "1" {
		if err := os.WriteFile(path, got, 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("contractDetail scheduler golden mismatch")
	}
}
func TestMEXCCapturedDedup(t *testing.T) {
	original := mexcCaptured(t)[0]
	original.Meta = map[string]any{"last_row_timestamp": int64(42)}
	change := func(field string, value json.RawMessage) Record {
		r := original
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(r.Payload, &obj); err != nil {
			t.Fatal(err)
		}
		if obj[field] == nil {
			t.Fatal("missing field", field)
		}
		obj[field] = value
		var err error
		r.Payload, err = json.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	// Marshal every variant identically so a dropped-only change has identical ordering.
	base := change("state", json.RawMessage(`0`))
	dropped := change("state", json.RawMessage(`999`))
	kept := change("maintenanceMarginRate", json.RawMessage(`999`))
	assertVenueDedup(t, "mexc", "contractDetail", venueDedupFixture{Original: []Record{base}, Dropped: []Record{dropped}, Kept: []Record{kept}})
}
