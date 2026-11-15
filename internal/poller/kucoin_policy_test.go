package poller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// checkKucoinRecord is independent of the registered policy tables.
func checkKucoinRecord(channel string, raw []byte) error {
	keeps := map[string]string{
		"contracts": "symbol baseCurrency quoteCurrency settleCurrency multiplier isInverse isQuanto type fundingFeeRate nextFundingRateDateTime nextFundingRateTime fundingRateGranularity currentFundingRateGranularity effectiveFundingRateCycleStartTime openInterest maxLeverage initialMargin maintainMargin maxRiskLimit minRiskLimit riskStep supportCross",
		"riskLimit": "level maxRiskLimit minRiskLimit maxLeverage initialMargin maintainMargin",
	}
	var source struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &source); err != nil {
		return err
	}
	before := UnknownFields("kucoin", channel)
	got, record, err := ApplyPolicy("kucoin", channel, raw)
	if err != nil || !record {
		return fmt.Errorf("filter: %v", err)
	}
	var out map[string]json.RawMessage
	if err = json.Unmarshal(got, &out); err != nil {
		return err
	}
	if len(out) != 1 {
		return fmt.Errorf("wrapper fields")
	}
	var rows []map[string]json.RawMessage
	if err = json.Unmarshal(out["data"], &rows); err != nil {
		return err
	}
	if len(rows) != len(source.Data) {
		return fmt.Errorf("row count")
	}
	keep := map[string]bool{}
	for _, k := range strings.Fields(keeps[channel]) {
		keep[k] = true
	}
	for i, row := range source.Data {
		for k, v := range row {
			if keep[k] {
				if !bytes.Equal(rows[i][k], v) {
					return fmt.Errorf("row %d retained %s", i, k)
				}
			} else if _, ok := rows[i][k]; ok {
				return fmt.Errorf("row %d dropped %s", i, k)
			}
		}
	}
	if n := UnknownFields("kucoin", channel) - before; n != 0 {
		return fmt.Errorf("unknown count %d", n)
	}
	return nil
}
func kucoinSample(t *testing.T, channel string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/kucoin/" + channel + ".sample.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSpace(b)
}
func TestKucoinPolicyRecords(t *testing.T) {
	for _, ch := range []string{"contracts", "riskLimit"} {
		t.Run(ch, func(t *testing.T) {
			raw := kucoinSample(t, ch)
			if err := checkKucoinRecord(ch, raw); err != nil {
				t.Fatal(err)
			}
			before := UnknownFields("kucoin", ch)
			got, _, err := ApplyPolicy("kucoin", ch, []byte(`{"code":"200000","data":[{"futureField":1}]}`))
			if err != nil || !bytes.Contains(got, []byte(`"futureField":1`)) || UnknownFields("kucoin", ch)-before != 1 {
				t.Fatal("unknown field not retained and counted")
			}
		})
	}
}

// Each subtest mutates one registered entry and requires the independent real-record check to reject it.
func TestKucoinPolicyMutations(t *testing.T) {
	for _, ch := range []string{"contracts", "riskLimit"} {
		raw := kucoinSample(t, ch)
		key := policyKey{"kucoin", ch}
		policies.RLock()
		original := policies.m[key]
		policies.RUnlock()
		for _, level := range []string{"wrapper", "data"} {
			p := original
			if level == "data" {
				p = original.Nested["data[]"]
			}
			for _, group := range []string{"keep", "drop"} {
				fields := p.Keep
				if group == "drop" {
					fields = p.Drop
				}
				for _, field := range fields {
					for _, action := range []string{"remove"} {
						if group == "keep" {
							action = "remove"
						}
						t.Run(ch+"/"+level+"/"+group+"/"+field+"/"+action, func(t *testing.T) {
							changed := p
							changed.Keep = append([]string(nil), p.Keep...)
							changed.Drop = append([]string(nil), p.Drop...)
							remove := func(a []string, k string) []string {
								var out []string
								for _, x := range a {
									if x != k {
										out = append(out, x)
									}
								}
								return out
							}
							if group == "keep" {
								changed.Keep = remove(changed.Keep, field)
							} else {
								changed.Drop = remove(changed.Drop, field)
								changed.Keep = append(changed.Keep, field)
							}
							q := original
							if level == "data" {
								q.Nested = map[string]Policy{"data[]": changed}
							} else {
								q = changed
							}
							policies.Lock()
							policies.m[key] = q
							policies.Unlock()
							defer func() { policies.Lock(); policies.m[key] = original; policies.Unlock() }()
							if err := checkKucoinRecord(ch, raw); err == nil {
								t.Fatal("mutation survived independent real-record assertion")
							}
						})
					}
					if group == "keep" {
						t.Run(ch+"/"+level+"/keep/"+field+"/drop", func(t *testing.T) {
							changed := p
							changed.Keep = append([]string(nil), p.Keep...)
							changed.Drop = append(append([]string(nil), p.Drop...), field)
							q := original
							if level == "data" {
								q.Nested = map[string]Policy{"data[]": changed}
							} else {
								q = changed
							}
							policies.Lock()
							policies.m[key] = q
							policies.Unlock()
							defer func() { policies.Lock(); policies.m[key] = original; policies.Unlock() }()
							if err := checkKucoinRecord(ch, raw); err == nil {
								t.Fatal("mutation survived independent real-record assertion")
							}
						})
					}
				}
			}
		}
	}
}
func TestKucoinPolicyGolden(t *testing.T) {
	// Preserve the shared fixture coverage and append real capture scheduler records.
	got := fixtureGolden(t, "kucoin")
	for _, ch := range []string{"contracts", "riskLimit"} {
		raw := kucoinSample(t, ch)
		clock := &fakeClock{time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)}
		sink := &memorySink{}
		fetch := SnapshotFetch("kucoin", ch, clock, SnapshotSpec{Mode: SnapshotPerSymbol, Fetch: func(context.Context, string) ([]byte, error) { return raw, nil }})
		c := Channel{Venue: "kucoin", Name: ch, Fetch: func(ctx context.Context, _ string) ([]Record, error) { return fetch(ctx, "FIXTURE") }}
		scheduler, err := NewScheduler([]Channel{c}, sink, clock, "", 240)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = scheduler.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, r := range sink.records {
			b, err := json.Marshal(fixtureGoldenRecord{"testdata/kucoin/" + ch + ".sample.jsonl", r.Venue, r.Channel, r.Symbol, r.RecvWall, r.Meta, r.Payload})
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, b...)
			got = append(got, '\n')
		}
	}
	path := "testdata/kucoin/golden-filtered.txt"
	if os.Getenv("GENERATE_VENUE_GOLDEN") == "kucoin" {
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
		t.Fatal("KuCoin filtered golden differs")
	}
}
func TestKucoinPolicyRegistrationAndDedup(t *testing.T) {
	for _, ch := range []string{"contracts", "riskLimit"} {
		policies.RLock()
		p, ok := policies.m[policyKey{"kucoin", ch}]
		policies.RUnlock()
		if !ok || p.TimestampKey != "" || p.TimestampIndex != nil || p.TimestampPath != "" {
			t.Fatalf("%s registration or snapshot timestamp", ch)
		}
	}
	row := func(s string) []Record {
		return []Record{{Venue: "kucoin", Channel: "contracts", Meta: map[string]any{"last_row_timestamp": int64(42)}, Payload: []byte(s)}}
	}
	assertVenueDedup(t, "kucoin", "contracts", venueDedupFixture{Original: row(`{"code":"200000","data":[{"symbol":"X","status":"Open"}]}`), Dropped: row(`{"code":"200000","data":[{"symbol":"X","status":"Closed"}]}`), Kept: row(`{"code":"200000","data":[{"symbol":"Y","status":"Open"}]}`)})
}

func TestKucoinPolicyRealDedup(t *testing.T) {
	raw := kucoinSample(t, "contracts")
	mutate := func(field, value string) []byte {
		t.Helper()
		var obj struct {
			Code json.RawMessage              `json:"code"`
			Data []map[string]json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(raw, &obj); err != nil {
			t.Fatal(err)
		}
		obj.Data[0][field] = json.RawMessage(value)
		b, err := json.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	// Marshal all variants identically, so only the intended field changes.
	var obj any
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatal(err)
	}
	base, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	// Use the same typed representation for each variant.
	var typed struct {
		Code json.RawMessage              `json:"code"`
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err = json.Unmarshal(raw, &typed); err != nil {
		t.Fatal(err)
	}
	base, err = json.Marshal(typed)
	if err != nil {
		t.Fatal(err)
	}
	row := func(p []byte) []Record {
		return []Record{{Venue: "kucoin", Channel: "contracts", Meta: map[string]any{"last_row_timestamp": int64(42)}, Payload: p}}
	}
	assertVenueDedup(t, "kucoin", "contracts", venueDedupFixture{Original: row(base), Dropped: row(mutate("markPrice", "123456789")), Kept: row(mutate("fundingFeeRate", "123456789"))})
}
