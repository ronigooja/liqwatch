package poller

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
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
