package collector

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/kucoin"
)

func TestKuCoinClassicThrottleSeparation(t *testing.T) {
	const mark = `{"type":"message","topic":"/contract/instrument:XBTUSDTM","subject":"mark.index.price","data":{"markPrice":69161.86,"indexPrice":69194.98,"timestamp":1794554438000}}`
	const funding = `{"type":"message","topic":"/contract/instrument:XBTUSDTM","subject":"funding.rate","data":{"fundingRate":0.000014,"timestamp":1794554460000}}`
	for _, reverse := range []bool{false, true} {
		name := "mark-then-funding"
		if reverse {
			name = "funding-then-mark"
		}
		t.Run(name, func(t *testing.T) {
			a := &kucoin.Adapter{}
			s := &throttleSink{}
			c := New(s, a, nil)
			c.ConfigureThrottle(true, nil)
			put := func(frame string, ms int64, seq uint64, conn string) {
				// Only the filtered data record enters throttling; diagnostic samples
				// are deliberately outside this routing/bucket regression.
				r := a.Split(venue.Shard{Name: name}, []byte(frame))[0]
				r.Venue, r.Conn, r.CSeq = "kucoin", conn, seq
				r.RecvWal, r.RecvMon = ms*int64(time.Millisecond), int64(seq)*123
				c.throttle.write(r)
			}
			wantMark := rawlog.Record{Venue: "kucoin", Conn: "classic", CSeq: 1, RecvWal: 60100 * int64(time.Millisecond), RecvMon: 123, Channel: "mark", Symbol: "XBTUSDTM", Payload: []byte(`{"data":{"markPrice":69161.86,"indexPrice":69194.98,"timestamp":1794554438000}}`)}
			wantFunding := rawlog.Record{Venue: "kucoin", Conn: "classic", CSeq: 2, RecvWal: 60200 * int64(time.Millisecond), RecvMon: 246, Channel: "funding", Symbol: "XBTUSDTM", Payload: []byte(`{"data":{"fundingRate":0.000014,"timestamp":1794554460000}}`)}
			if reverse {
				put(funding, 60200, 2, "classic")
				put(mark, 60100, 1, "classic")
			} else {
				put(mark, 60100, 1, "classic")
				put(funding, 60200, 2, "classic")
			}
			check := func(want ...rawlog.Record) {
				t.Helper()
				got := append([]rawlog.Record(nil), s.records...)
				sort.Slice(got, func(i, j int) bool { return got[i].CSeq < got[j].CSeq })
				sort.Slice(want, func(i, j int) bool { return want[i].CSeq < want[j].CSeq })
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %#v, want %#v", got, want)
				}
			}
			check()
			c.throttle.flush(time.Time{}, "", true)
			check(wantMark, wantFunding)
			s.records = nil
			put(mark, 60100, 1, "classic")
			put(funding, 60200, 2, "classic")
			// Same-class replacements keep the complete last record independently.
			put(mark, 60900, 3, "classic")
			put(funding, 119900, 4, "classic")
			wantMark.CSeq, wantMark.RecvWal, wantMark.RecvMon = 3, 60900*int64(time.Millisecond), 369
			wantFunding.CSeq, wantFunding.RecvWal, wantFunding.RecvMon = 4, 119900*int64(time.Millisecond), 492
			check()
			if got := c.Thinned(); got["mark"] != 1 || got["funding"] != 1 {
				t.Fatalf("thinned: %v", got)
			}
			c.throttle.flush(time.UnixMilli(61499), "", false)
			check()
			c.throttle.flush(time.UnixMilli(61500), "", false)
			check(wantMark)
			c.throttle.flush(time.UnixMilli(120499), "", false)
			check(wantMark)
			c.throttle.flush(time.UnixMilli(120500), "", false)
			check(wantMark, wantFunding)
			// New buckets survive close/shutdown exactly once, with connection isolation.
			put(mark, 121000, 5, "other")
			put(funding, 121100, 6, "classic")
			wantMark.Conn, wantMark.CSeq, wantMark.RecvWal, wantMark.RecvMon = "other", 5, 121000*int64(time.Millisecond), 615
			wantFunding.CSeq, wantFunding.RecvWal, wantFunding.RecvMon = 6, 121100*int64(time.Millisecond), 738
			s.records = nil
			c.throttle.flush(time.Time{}, "classic", false)
			check(wantFunding)
			c.throttle.flush(time.Time{}, "", true)
			check(wantMark, wantFunding)
			c.throttle.flush(time.Time{}, "", true)
			check(wantMark, wantFunding)
			s.records = nil
			put(mark, 180100, 7, "classic")
			put(funding, 180200, 8, "classic")
			wantMark.Conn, wantMark.CSeq, wantMark.RecvWal, wantMark.RecvMon = "classic", 7, 180100*int64(time.Millisecond), 861
			wantFunding.CSeq, wantFunding.RecvWal, wantFunding.RecvMon = 8, 180200*int64(time.Millisecond), 984
			put(mark, 181100, 9, "classic")
			check(wantMark)
			put(funding, 240100, 10, "classic")
			check(wantMark, wantFunding)
		})
	}
}
