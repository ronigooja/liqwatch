package poller

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestPolicyShapes(t *testing.T) {
	tests := []struct {
		name, input, want string
		policy            Policy
		unknown           uint64
	}{
		{"object", `{"ts":1794729600000,"price":1.2300,"unused":3,"new":1e-7}`, `{"ts":1794729600000,"price":1.2300,"new":1e-7}`, Policy{Keep: []string{"ts", "price"}, Drop: []string{"unused"}}, 1},
		{"wrapper", `{"code":0,"result":{"list":[{"id":123456789012345678,"rate":0.00003,"extra":1}],"nextPageCursor":"x"}}`, `{"result":{"list":[{"id":123456789012345678,"extra":1}]} }`, Policy{Drop: []string{"code"}, Keep: []string{"result"}, Nested: map[string]Policy{"result": {Keep: []string{"list"}, Drop: []string{"nextPageCursor"}}, "result.list[]": {Keep: []string{"id"}, Drop: []string{"rate"}}}}, 1},
		{"positional", `[1794729600000,1.2300,1e-7]`, `[1794729600000,1e-7]`, Policy{KeepIndexes: []int{0, 2}, DropIndexes: []int{1}}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			venue := "policy-test-" + tt.name
			if err := RegisterPolicy(venue, "channel", tt.policy); err != nil {
				t.Fatal(err)
			}
			got, record, err := ApplyPolicy(venue, "channel", []byte(tt.input))
			if err != nil || !record {
				t.Fatalf("apply: %v %v", record, err)
			}
			var a, b bytes.Buffer
			if err := json.Compact(&a, got); err != nil {
				t.Fatal(err)
			}
			if err := json.Compact(&b, []byte(tt.want)); err != nil {
				t.Fatal(err)
			}
			if a.String() != b.String() {
				t.Fatalf("got %s want %s", a.String(), b.String())
			}
			if n := UnknownFields(venue, "channel"); n != tt.unknown {
				t.Fatalf("unknown=%d want %d", n, tt.unknown)
			}
		})
	}
}

func TestPolicyDropAndTimestamp(t *testing.T) {
	if err := RegisterPolicy("policy-test-drop", "c", Policy{DropChannel: true}); err != nil {
		t.Fatal(err)
	}
	_, record, err := ApplyPolicy("policy-test-drop", "c", []byte(`{"ts":1}`))
	if err != nil || record {
		t.Fatalf("drop: %v %v", record, err)
	}
	if err := RegisterPolicy("policy-test-bad", "c", Policy{Drop: []string{"ts"}, TimestampKey: "ts"}); err == nil {
		t.Fatal("timestamp drop accepted")
	}
	i := 0
	if err := RegisterPolicy("policy-test-bad-index", "c", Policy{DropIndexes: []int{0}, TimestampIndex: &i}); err == nil {
		t.Fatal("timestamp index drop accepted")
	}
}

func TestPolicyUnregistered(t *testing.T) {
	raw := []byte(" { \"x\": 1.2300 } \n")
	got, record, err := ApplyPolicy("policy-test-absent", "c", raw)
	if err != nil || !record || !bytes.Equal(got, raw) {
		t.Fatalf("got %q %v %v", got, record, err)
	}
}
