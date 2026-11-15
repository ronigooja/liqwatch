package binance

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/ronigooja/liqwatch/internal/venue"
)

func TestRecordedFieldPolicy(t *testing.T) {
	for _, tc := range []struct {
		channel    string
		keep, drop []string
	}{
		{"aggTrade", []string{"E", "T", "a", "f", "l", "p", "q", "m"}, []string{"e", "s", "nq", "st"}},
		{"markPrice_all", []string{"E", "p", "i", "r", "T"}, []string{"e", "s", "st", "ap", "P"}},
	} {
		t.Run(tc.channel, func(t *testing.T) {
			f, err := os.Open("testdata/" + tc.channel + ".jsonl")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			a := &Adapter{}
			_, err = a.Shards(context.Background(), []string{"AAVEUSDT"})
			if err != nil {
				t.Fatal(err)
			}
			counter, ok := any(a).(interface{ UnknownFields() uint64 })
			if !ok {
				t.Fatal("missing unknown-field counter")
			}
			scan := bufio.NewScanner(f)
			n := 0
			for scan.Scan() {
				var rec struct {
					Payload struct {
						Stream string                     `json:"stream"`
						Data   map[string]json.RawMessage `json:"data"`
					} `json:"p"`
				}
				if err := json.Unmarshal(scan.Bytes(), &rec); err != nil {
					t.Fatal(err)
				}
				original := rec.Payload.Data
				original["mystery"] = json.RawMessage(`{"value":1.2300}`)
				var data any = original
				if tc.channel == "markPrice_all" {
					data = []any{original}
				}
				frame, err := json.Marshal(map[string]any{"stream": rec.Payload.Stream, "data": data})
				if err != nil {
					t.Fatal(err)
				}
				before := counter.UnknownFields()
				got := a.Split(venue.Shard{}, frame)
				if len(got) != 1 || got[0].Channel != tc.channel || got[0].Symbol != "AAVEUSDT" {
					t.Fatalf("records: %+v", got)
				}
				var payload map[string]json.RawMessage
				if err := json.Unmarshal(got[0].Payload, &payload); err != nil {
					t.Fatal(err)
				}
				if _, ok := payload["stream"]; ok {
					t.Fatal("stream retained")
				}
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(payload["data"], &fields); err != nil {
					t.Fatal(err)
				}
				for _, key := range tc.drop {
					if _, ok := fields[key]; ok {
						t.Errorf("dropped field %s retained", key)
					}
				}
				for _, key := range append(tc.keep, "mystery") {
					if !reflect.DeepEqual(fields[key], original[key]) || fields[key] == nil {
						t.Errorf("field %s: got %s want %s", key, fields[key], original[key])
					}
				}
				if delta := counter.UnknownFields() - before; delta != 1 {
					t.Errorf("unknown fields = %d, want 1", delta)
				}
				n++
			}
			if err := scan.Err(); err != nil {
				t.Fatal(err)
			}
			if n < 3 {
				t.Fatalf("only %d fixtures", n)
			}
		})
	}
}
