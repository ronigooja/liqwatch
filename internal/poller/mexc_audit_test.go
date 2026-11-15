package poller

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

// Captured post-policy payloads from root@64.118.133.169:
// /root/liqwatch-rest5-20261115T102846Z/data/raw/mexc/{symbol}/20261115/contractDetail.p.jsonl.
// Only the three previously unknown members are removed in the independent golden.
func mexcAuditMatches(t *testing.T) bool {
	t.Helper()
	f, err := os.Open("testdata/mexc/audited-undefined.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	count := 0
	for sc.Scan() {
		var sample struct {
			Symbol string          `json:"symbol"`
			Input  json.RawMessage `json:"input"`
			Want   json.RawMessage `json:"want"`
		}
		if err := json.Unmarshal(sc.Bytes(), &sample); err != nil {
			t.Fatal(err)
		}
		before := UnknownFields("mexc", "contractDetail")
		got, record, err := ApplyPolicy("mexc", "contractDetail", sample.Input)
		if err != nil || !record || !bytes.Equal(got, sample.Want) || UnknownFields("mexc", "contractDetail") != before {
			return false
		}
		count++
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("captured samples: %d, want 2", count)
	}
	return true
}

func TestMEXCAuditedUndefinedGolden(t *testing.T) {
	if !mexcAuditMatches(t) {
		t.Fatal("captured audited-field payload differs from independent golden or increments unknown count")
	}
	before := UnknownFields("mexc", "contractDetail")
	got, record, err := ApplyPolicy("mexc", "contractDetail", []byte(`{"contractSize":0.001,"unauditedNovel":42}`))
	if err != nil || !record || string(got) != `{"contractSize":0.001,"unauditedNovel":42}` || UnknownFields("mexc", "contractDetail")-before != 1 {
		t.Fatalf("unaudited unknown must remain: %s, record=%v, err=%v", got, record, err)
	}
}

func TestMEXCAuditedUndefinedMutationResistance(t *testing.T) {
	key := policyKey{"mexc", "contractDetail"}
	policies.RLock()
	original := policies.m[key]
	policies.RUnlock()
	if !mexcAuditMatches(t) {
		t.Fatal("baseline differs from independent golden")
	}
	for _, field := range []string{"tempMaxLeverage", "tempMaxLeverageReason", "tempFeeRateEffectiveEndTime"} {
		for _, mode := range []string{"remove", "move-to-keep"} {
			t.Run(field+"/"+mode, func(t *testing.T) {
				mutated := original
				mutated.Keep = append([]string(nil), original.Keep...)
				mutated.Drop = nil
				for _, name := range original.Drop {
					if name != field {
						mutated.Drop = append(mutated.Drop, name)
					}
				}
				if mode == "move-to-keep" {
					mutated.Keep = append(mutated.Keep, field)
				}
				policies.Lock()
				policies.m[key] = mutated
				policies.Unlock()
				defer func() {
					policies.Lock()
					policies.m[key] = original
					policies.Unlock()
				}()
				if mexcAuditMatches(t) {
					t.Fatal("independent golden accepted mutated classification")
				}
			})
		}
	}
}
