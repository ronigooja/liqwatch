package gate

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/venuetest"
)

func TestGoldenRecords(t *testing.T) {
	for i, line := range venuetest.Fixture(t, "testdata/golden-records.jsonl") {
		var fixture struct {
			Channel  string          `json:"channel"`
			Symbol   string          `json:"symbol"`
			Input    json.RawMessage `json:"input"`
			Expected json.RawMessage `json:"expected"`
		}
		if err := json.Unmarshal(line, &fixture); err != nil {
			t.Fatal(err)
		}
		t.Run(fixture.Channel+string(rune('a'+i)), func(t *testing.T) {
			a := &Adapter{}
			a.Shards(context.Background(), []string{fixture.Symbol})
			records := a.Split(venue.Shard{Name: t.Name()}, fixture.Input)
			if len(records) != 2 || records[0].Channel != fixture.Channel || records[0].Symbol != fixture.Symbol {
				t.Fatalf("records: %+v", records)
			}
			var got, want any
			if json.Unmarshal(records[0].Payload, &got) != nil || json.Unmarshal(fixture.Expected, &want) != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("got %s\nwant %s", records[0].Payload, fixture.Expected)
			}
			if string(records[1].Payload) != `{"sample":`+string(fixture.Input)+`}` {
				t.Fatal("sample changed")
			}
		})
	}
}
