package main

import (
	"reflect"
	"testing"

	"github.com/ronigooja/liqwatch/internal/poller"
)

func TestUnknownFieldLines(t *testing.T) {
	channels := []poller.Channel{{Venue: "bybit", Name: "instruments"}, {Venue: "okx", Name: "quiet"}, {Venue: "bybit", Name: "instruments"}}
	got := unknownFieldLines(channels, func(v, c string) uint64 {
		if v == "bybit" && c == "instruments" {
			return 12
		}
		return 0
	})
	want := []string{"poller unknown fields venue=bybit channel=instruments count=12"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if got := unknownFieldLines(channels, func(string, string) uint64 { return 0 }); len(got) != 0 {
		t.Fatalf("unexpected lines %v", got)
	}
}
