package kucoin

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

func TestClassicInstrumentFixtureRouting(t *testing.T) {
	oldNow, oldLast := samples.now, samples.last
	defer func() { samples.now, samples.last = oldNow, oldLast }()
	wants := map[string]rawlog.Record{
		"mark.index.price": {Channel: "mark", Symbol: "XBTUSDTM", Payload: []byte(`{"data":{"markPrice":69161.86,"indexPrice":69194.98,"timestamp":1794554438000}}`)},
		"funding.rate":     {Channel: "funding", Symbol: "0GUSDTM", Payload: []byte(`{"data":{"fundingRate":0.000050,"timestamp":1794554460000}}`)},
	}
	for subject, want := range wants {
		var frame []byte
		for _, f := range fixtureFrames(t) {
			if strings.Contains(string(f), `"subject":"`+subject+`"`) {
				frame = f
				break
			}
		}
		if frame == nil {
			t.Fatalf("missing fixture for %s", subject)
		}
		for _, legacy := range []bool{false, true} {
			path := "fast"
			if legacy {
				path = "compatibility"
			}
			t.Run(subject+"/"+path, func(t *testing.T) {
				samples.last = map[string]time.Time{}
				a := &Adapter{}
				sh := venue.Shard{Name: "classic-routing"}
				var got []rawlog.Record
				if legacy {
					got = a.legacySplit(sh, frame)
				} else {
					got = a.Split(sh, frame)
				}
				sample := rawlog.Record{Channel: want.Channel, Symbol: want.Symbol, Payload: append(append([]byte(`{"sample":`), frame...), '}')}
				if !reflect.DeepEqual(got, []rawlog.Record{want, sample}) {
					t.Fatalf("got %#v, want %#v", got, []rawlog.Record{want, sample})
				}
			})
		}
	}
}

func TestThrottleDefaults(t *testing.T) {
	want := map[string]time.Duration{"mark": time.Second, "utamark": time.Second, "funding": time.Minute}
	if got := (&Adapter{}).Throttles(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestChannelDropClasses(t *testing.T) {
	classify := (&Adapter{}).Classes()
	for _, tc := range []struct {
		channel, class string
		droppable      bool
	}{
		{"trade", "trade", true},
		{"mark", "mark", true},
		{"funding", "mark", true},
		{"utamark", "mark", true},
		{"gap", "", false},
		{"unknown", "", false},
	} {
		t.Run(tc.channel, func(t *testing.T) {
			class, ok := classify(tc.channel)
			if class != tc.class || ok != tc.droppable {
				t.Fatalf("Classes(%q) = (%q, %v), want (%q, %v)", tc.channel, class, ok, tc.class, tc.droppable)
			}
		})
	}
}
