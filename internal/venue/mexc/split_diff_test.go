package mexc

import (
	"bufio"
	"bytes"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

func frames(t testing.TB) [][]byte {
	t.Helper()
	f, e := os.Open("testdata/frames.jsonl")
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 4096), 16<<20)
	var r [][]byte
	for s.Scan() {
		r = append(r, append([]byte(nil), s.Bytes()...))
	}
	if s.Err() != nil {
		t.Fatal(s.Err())
	}
	return r
}
func compare(t *testing.T, b []byte) {
	t.Helper()
	run := func(old bool) ([]rawlog.Record, uint64, uint64, uint64, map[string]time.Time) {
		samples.last = map[string]time.Time{}
		now := time.Unix(123456, 0)
		samples.now = func() time.Time { return now }
		a := &Adapter{universe: map[string]struct{}{"BTC_USDT": {}, "中文_USDT": {}}}
		var r []rawlog.Record
		for i := 0; i < 3; i++ {
			if i == 2 {
				now = now.Add(time.Hour)
			}
			if old {
				r = append(r, a.referenceSplit(venue.Shard{Name: "diff"}, b)...)
			} else {
				r = append(r, a.Split(venue.Shard{Name: "diff"}, b)...)
			}
		}
		return r, a.UnknownFields(), a.SubscribeAcks(), a.SubscribeErrors(), samples.last
	}
	a, b1, c, d, e := run(true)
	x, y, z, u, v := run(false)
	if !reflect.DeepEqual(a, x) || b1 != y || c != z || d != u || !reflect.DeepEqual(e, v) {
		t.Fatalf("frame %q\nold %q %d %d %d %v\nnew %q %d %d %d %v", b, a, b1, c, d, e, x, y, z, u, v)
	}
}
func TestSplitDifferential(t *testing.T) {
	old := samples.now
	defer func() { samples.now = old }()
	fs := frames(t)
	for _, s := range []string{` {"channel":"push.ticker","symbol":"中文_USDT","data":{"x":{"a":[1,"a\\b",null]},"lastPrice":1e-2},"ts":12} `, `{"channel":"push.deal","symbol":"BTC_USDT","data":[{"O":1,"p":2}]}`, `{"channel":"push.ticker","symbol":"BTC_USDT","data":{}}`, `{"channel":"push.ticker","symbol":"BTC_USDT","data":null}`, `{"channel":"push.ticker","channel":"pong","data":{}}`, `{"channel":"rs.sub.x","data":{"success":false}}`, `{"channel":"push.contract","symbol":"BTC_USDT","data":{}}`, `{"channel":"push.contract","symbol":"OTHER","data":{}}`, `{"channel":"push.ticker","symbol":"BTC_USDT","data":{"\u0078":1}}`, `{"channel":"push.ticker","data":[}`, `null`} {
		fs = append(fs, []byte(s))
	}
	for _, f := range fs {
		compare(t, f)
		for i := range f {
			compare(t, f[:i])
			m := append([]byte(nil), f...)
			m[i] ^= 1
			compare(t, m)
		}
	}
}
func FuzzSplitDifferential(f *testing.F) {
	for _, b := range frames(f) {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			t.Skip()
		}
		old := samples.now
		defer func() { samples.now = old }()
		compare(t, b)
	})
}
func BenchmarkSplit(b *testing.B) {
	fs := frames(b)
	for _, channel := range []string{"push.deal", "push.fair.price", "push.index.price", "push.ticker", "push.funding.rate", "push.contract.extra.update"} {
		for _, frame := range fs {
			if !contains(frame, []byte(`"channel":"`+channel+`"`)) {
				continue
			}
			for _, old := range []bool{true, false} {
				name := "new"
				if old {
					name = "old"
				}
				b.Run(channel+"/"+name, func(b *testing.B) {
					a := &Adapter{universe: map[string]struct{}{"BTC_USDT": {}}}
					sh := venue.Shard{Name: "bench"}
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if old {
							a.referenceSplit(sh, frame)
						} else {
							a.Split(sh, frame)
						}
					}
				})
			}
			break
		}
	}
}
func contains(b, sub []byte) bool { return bytes.Contains(b, sub) }
