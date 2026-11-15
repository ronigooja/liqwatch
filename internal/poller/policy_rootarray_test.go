package poller

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestRootArrayGateFixtures(t *testing.T) {
	cases := []struct {
		name, keep, drop, removed string
		unknown                   uint64
		timestamp                 bool
	}{
		{"contract_stats", "time", "lsr_taker", `"lsr_taker":1.0962807988326935,|"lsr_taker":1.5781299774487076,`, 46, true},
		{"risk_limit_tiers", "tier", "initial_rate", `"initial_rate":"0.1",|"initial_rate":"0.1111",`, 10, false},
		{"contracts", "name", "funding_rate_indicative", `"funding_rate_indicative":"0.0001",|"funding_rate_indicative":"0.0001",`, 102, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/gate/fixture-" + tc.name + ".json")
			if err != nil {
				t.Fatal(err)
			}
			p := Policy{Nested: map[string]Policy{"[]": {Keep: []string{tc.keep}, Drop: []string{tc.drop}}}}
			if tc.timestamp {
				p.TimestampPath, p.TimestampKey = "[]", tc.keep
			}
			if err := RegisterPolicy("root-fixture", tc.name, p); err != nil {
				t.Fatal(err)
			}
			var compact bytes.Buffer
			if err := json.Compact(&compact, raw); err != nil {
				t.Fatal(err)
			}
			want := compact.String()
			for _, fragment := range strings.Split(tc.removed, "|") {
				if !strings.Contains(want, fragment) {
					t.Fatalf("missing fixture fragment %s", fragment)
				}
				want = strings.Replace(want, fragment, "", 1)
			}
			got, record, err := ApplyPolicy("root-fixture", tc.name, raw)
			if err != nil || !record || string(got) != want {
				t.Fatalf("got %s, want %s; record=%v err=%v", got, want, record, err)
			}
			if n := UnknownFields("root-fixture", tc.name); n != tc.unknown {
				t.Fatalf("unknown=%d want %d", n, tc.unknown)
			}
		})
	}
}

func TestRootArrayMembers(t *testing.T) {
	p := Policy{Nested: map[string]Policy{
		"[]":         {Keep: []string{"id", "tiers"}, Drop: []string{"remove"}},
		"[].tiers[]": {Keep: []string{"v"}, Drop: []string{"remove"}},
	}}
	if err := RegisterPolicy("root-members", "c", p); err != nil {
		t.Fatal(err)
	}
	raw := `[{"id":123456789012345678901234567890,"remove":0,"tiers":[{"v":1.2300,"remove":2,"extra":3}]},1.2300,[1, 2],null,{"id":2,"extra":0}]`
	want := `[{"id":123456789012345678901234567890,"tiers":[{"v":1.2300,"extra":3}]},1.2300,[1, 2],null,{"id":2,"extra":0}]`
	got, record, err := ApplyPolicy("root-members", "c", []byte(raw))
	if err != nil || !record || string(got) != want {
		t.Fatalf("got %s want %s: %v", got, want, err)
	}
	if n := UnknownFields("root-members", "c"); n != 2 {
		t.Fatalf("unknown=%d want 2", n)
	}
}

func TestRootArrayRegistration(t *testing.T) {
	cases := []struct {
		name  string
		p     Policy
		valid bool
	}{
		{"keep-index", Policy{KeepIndexes: []int{0}, Nested: map[string]Policy{"[]": {}}}, false},
		{"drop-index", Policy{DropIndexes: []int{0}, Nested: map[string]Policy{"[]": {}}}, false},
		{"timestamp-drop", Policy{TimestampPath: "[]", TimestampKey: "time", Nested: map[string]Policy{"[]": {Drop: []string{"time"}}}}, false},
		{"timestamp-omit", Policy{TimestampPath: "[]", TimestampKey: "time", Nested: map[string]Policy{"[]": {Keep: []string{"id"}}}}, false},
		{"timestamp-missing", Policy{TimestampPath: "[]", TimestampKey: "time"}, false},
		{"timestamp-valid", Policy{TimestampPath: "[]", TimestampKey: "time", Nested: map[string]Policy{"[]": {Keep: []string{"time"}}}}, true},
		{"timestamp-nested", Policy{TimestampPath: "[].tiers[]", TimestampKey: "time", Nested: map[string]Policy{"[]": {Keep: []string{"tiers"}}, "[].tiers[]": {Keep: []string{"time"}}}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := RegisterPolicy("root-registration", tc.name, tc.p)
			if (err == nil) != tc.valid {
				t.Fatalf("registration: %v", err)
			}
		})
	}
}

func TestRootArrayPositionalCompatibility(t *testing.T) {
	p := Policy{KeepIndexes: []int{0, 2}, DropIndexes: []int{1}}
	if err := RegisterPolicy("root-positional", "c", p); err != nil {
		t.Fatal(err)
	}
	got, record, err := ApplyPolicy("root-positional", "c", []byte(`[1794729600000,1.2300,1e-7]`))
	if err != nil || !record || string(got) != `[1794729600000,1e-7]` || UnknownFields("root-positional", "c") != 0 {
		t.Fatalf("got %s record=%v err=%v", got, record, err)
	}
}
