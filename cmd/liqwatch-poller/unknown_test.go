package main

import (
	"reflect"
	"testing"

	"github.com/ronigooja/liqwatch/internal/poller"
)

func TestUnknownFieldReporter(t *testing.T) {
	channels := []poller.Channel{
		{Venue: "gate", Name: "contracts"}, {Venue: "okx", Name: "instruments"},
		{Venue: "bitget", Name: "contracts"}, {Venue: "gate", Name: "contracts"},
	}
	reporter := make(unknownFieldReporter)
	for i, sweep := range []struct {
		gate, bitget uint64
		want         []string
	}{
		{0, 0, nil},
		{12, 0, []string{"poller unknown fields venue=gate channel=contracts count=12"}},
		{12, 0, nil},
		{13, 1, []string{"poller unknown fields venue=bitget channel=contracts count=1", "poller unknown fields venue=gate channel=contracts count=13"}},
		{13, 1, nil},
	} {
		selected := channels[:3]
		if i >= 3 {
			selected = channels // Also exercise a duplicate channel descriptor.
		}
		got := reporter.lines(selected, func(v, c string) uint64 {
			if c != "contracts" {
				return 0
			}
			if v == "bitget" {
				return sweep.bitget
			}
			return sweep.gate
		})
		if !reflect.DeepEqual(got, sweep.want) {
			t.Fatalf("sweep %d: got %v want %v", i, got, sweep.want)
		}
	}
}
