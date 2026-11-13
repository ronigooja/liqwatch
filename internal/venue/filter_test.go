package venue

import (
	"reflect"
	"testing"

	"github.com/ronigooja/liqwatch/internal/venuemap"
)

func TestFilter(t *testing.T) {
	m := &venuemap.Map{Pairs: []venuemap.Pair{{Venue: "gate", Symbol: "A", Status: "auto"}, {Venue: "gate", Symbol: "B", Status: "reviewed"}, {Venue: "gate", Symbol: "C", Status: "review"}}}
	in := []Symbol{"B", "C", "A", "D"}
	if got := Filter(in, m, "gate"); !reflect.DeepEqual(got, []Symbol{"B", "A"}) {
		t.Fatal(got)
	}
	if got := Filter(in, m, "binance"); !reflect.DeepEqual(got, in) {
		t.Fatal(got)
	}
}
