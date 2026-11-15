package poller

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestPolicyUnregisteredFixtures(t *testing.T) {
	paths, err := filepath.Glob("testdata/*/*.json")
	if err != nil || len(paths) == 0 {
		t.Fatalf("fixtures: %v", err)
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			got, record, err := ApplyPolicy("fixture-no-policy", path, raw)
			if err != nil || !record || !bytes.Equal(raw, got) {
				t.Fatalf("fixture changed: %v", err)
			}
		})
	}
}

func TestPolicySchedulerIntegration(t *testing.T) {
	const venue = "policy-test-scheduler"
	if err := RegisterPolicy(venue, "history", Policy{Keep: []string{"ts"}, Drop: []string{"discard"}, TimestampKey: "ts"}); err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	sink := &memorySink{}
	c := Channel{Venue: venue, Name: "history", Fetch: func(context.Context, string) ([]Record, error) {
		return []Record{{Venue: venue, Channel: "history", Meta: map[string]any{"last_row_timestamp": int64(42)}, Payload: []byte(`{"ts":42,"discard":1,"unknown":2}`)}}, nil
	}}
	s, err := NewScheduler([]Channel{c}, sink, clock, "", 240)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.records) != 1 || string(sink.records[0].Payload) != `{"ts":42,"unknown":2}` {
		t.Fatalf("records: %+v", sink.records)
	}
	if got := s.RowTimestamp(venue, "history", ""); got != 42 {
		t.Fatalf("cursor=%d", got)
	}
	if got := UnknownFields(venue, "history"); got != 1 {
		t.Fatalf("unknown=%d", got)
	}
}

func TestPolicyTimestampPaths(t *testing.T) {
	zero := 0
	cases := []struct {
		name  string
		p     Policy
		valid bool
	}{
		{"nested-drop", Policy{TimestampPath: "result.list[]", TimestampKey: "timestamp", Nested: map[string]Policy{"result": {Keep: []string{"list"}}, "result.list[]": {Drop: []string{"timestamp"}}}}, false},
		{"nested-omit", Policy{TimestampPath: "data[]", TimestampKey: "ts", Nested: map[string]Policy{"data[]": {Keep: []string{"ratio"}}}}, false},
		{"ancestor-drop", Policy{TimestampPath: "result.list[]", TimestampKey: "ts", Drop: []string{"result"}, Nested: map[string]Policy{"result": {}, "result.list[]": {Keep: []string{"ts"}}}}, false},
		{"missing-path", Policy{TimestampPath: "data[]", TimestampKey: "ts"}, false},
		{"nested-index-drop", Policy{TimestampPath: "data[]", TimestampIndex: &zero, Nested: map[string]Policy{"data[]": {DropIndexes: []int{0}}}}, false},
		{"nested-index-omit", Policy{TimestampPath: "data[]", TimestampIndex: &zero, Nested: map[string]Policy{"data[]": {KeepIndexes: []int{1}}}}, false},
		{"nested-valid", Policy{TimestampPath: "data[]", TimestampKey: "ts", Nested: map[string]Policy{"data[]": {Keep: []string{"ts"}}}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := RegisterPolicy("policy-path-test", tc.name, tc.p)
			if (err == nil) != tc.valid {
				t.Fatalf("registration: %v", err)
			}
		})
	}
}
