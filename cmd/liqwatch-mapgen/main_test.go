package main

import (
	"testing"

	"github.com/ronigooja/liqwatch/internal/venuemap"
)

func TestNormalize(t *testing.T) {
	for in, want := range map[string]string{"1000PEPE": "PEPE", "1MBABYDOGE": "BABYDOGE", "kSHIB": "SHIB", "XBT": "BTC", "ETH": "ETH"} {
		if got := normalize(in); got != want {
			t.Errorf("%s: %s", in, got)
		}
	}
}
func TestPriceAndAmbiguity(t *testing.T) {
	bn := []instrument{{"CATBN", "1000CAT", 1}, {"ONBN", "ON", 1}, {"PEPEBN", "1000PEPE", .001}}
	vs := []instrument{{"CAT", "CAT", 1}, {"1000CAT", "1000CAT", 1}, {"ON", "ON", 3}, {"PEPE", "PEPE", 1}}
	p := propose(bn, vs, "bitget")
	got := map[string]venuemap.Pair{}
	for _, x := range p {
		got[x.Symbol] = x
	}
	if got["CAT"].Status != "review" || got["1000CAT"].Status != "review" || got["ON"].Status != "rejected" || got["PEPE"].Exp10 != 3 || got["PEPE"].Status != "auto" {
		t.Fatalf("%+v", got)
	}
}
func TestDifferentAndChinese(t *testing.T) {
	bn := []instrument{{"SHIB", "1000SHIB", 1}, {"NEIRO", "NEIRO", 2}, {"LUNA2", "LUNA2", 3}, {"DATAIP", "DATAIP", 4}, {"DATA", "DATA", 40}, {"XAUT", "XAUT", 5}, {"XAU", "XAU", 50}, {"CN", "龙虾", 6}}
	vs := []instrument{{"SHIB", "SHIB1000", 1}, {"NEIRO", "NEIROCTO", 2}, {"LUNA", "LUNA", 3}, {"DATA", "DATA", 4}, {"XAU", "XAU", 5}, {"CN", "LONGXIA", 6}}
	got := map[string]venuemap.Pair{}
	for _, p := range propose(bn, vs, "gate") {
		got[p.Symbol] = p
	}
	if got["SHIB"].Status != "review" || got["NEIRO"].Status != "review" || got["LUNA"].Status != "review" || got["CN"].Status != "review" {
		t.Fatal(got)
	}
	if got["DATA"].Status == "auto" || got["XAU"].Status == "auto" {
		t.Fatal(got)
	}
}

func TestShortNameRule(t *testing.T) {
	for _, tc := range []struct{ binance, venue, want string }{
		{"DODOX", "DODO", "auto"}, {"LAYER", "SOLAYER", "auto"},
		{"RAYSOL", "RAY", "auto"}, {"RONIN", "RON", "review"},
		{"PLAY", "PLAYSOUT", "review"}, {"TOKEN", "TOKENCOIN", "auto"},
		{"LONGNAME", "LONGNAMEOTHER", "auto"},
	} {
		t.Run(tc.binance+"/"+tc.venue, func(t *testing.T) {
			got := propose([]instrument{{"B", tc.binance, 1}}, []instrument{{"V", tc.venue, 1}}, "gate")
			if len(got) != 1 || got[0].Status != tc.want {
				t.Fatalf("got %+v, want %s", got, tc.want)
			}
		})
	}
}

func TestMergeKeepsAllReviewedFields(t *testing.T) {
	old := []venuemap.Pair{{Binance: "LAYER", Venue: "bybit", Symbol: "SOLAYER", Exp10: 3, Status: "reviewed", Note: "human"}, {Binance: "X", Venue: "gate", Symbol: "XAU", Exp10: 4, Status: "rejected", Note: "human rejection"}}
	fresh := []venuemap.Pair{{Binance: "OTHER", Venue: "bybit", Symbol: "SOLAYER", Status: "auto", Note: "new ratio"}, {Binance: "X", Venue: "gate", Symbol: "XAU", Status: "auto", Note: "new ratio"}}
	got, diff := merge(old, fresh, map[string]map[string]bool{"bybit": {"SOLAYER": true}, "gate": {"XAU": true}})
	if len(got) != 2 || got[0] != old[0] || got[1] != old[1] || len(diff) != 0 {
		t.Fatalf("got %+v diff %v", got, diff)
	}
}

func TestMergeDoesNotPromoteReview(t *testing.T) {
	old := []venuemap.Pair{{Binance: "RAY", Venue: "gate", Symbol: "RAY", Status: "review", Note: "ratio=1"}}
	fresh := []venuemap.Pair{{Binance: "RAY", Venue: "gate", Symbol: "RAY", Status: "auto", Note: "ratio=1.001"}}
	got, diff := merge(old, fresh, map[string]map[string]bool{"gate": {"RAY": true}})
	if len(got) != 1 || got[0] != old[0] || len(diff) != 0 {
		t.Fatalf("got %+v diff %v", got, diff)
	}
}
func TestMergePreservesDecisions(t *testing.T) {
	old := []venuemap.Pair{{Binance: "A", Venue: "gate", Symbol: "A", Status: "reviewed", Note: "manual"}, {Binance: "B", Venue: "gate", Symbol: "B", Status: "rejected"}}
	fresh := []venuemap.Pair{{Binance: "A", Venue: "gate", Symbol: "A", Status: "auto"}}
	got, _ := merge(old, fresh, map[string]map[string]bool{"gate": {"A": true, "B": true}})
	if len(got) != 2 || got[0] != old[0] || got[1] != old[1] {
		t.Fatal(got)
	}
}
