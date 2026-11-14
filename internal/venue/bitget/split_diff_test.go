package bitget

import (
	"bufio"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

func fixtureFrames(t testing.TB) [][]byte {
	t.Helper()
	f, err := os.Open("testdata/frames.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 4096), 16<<20)
	var frames [][]byte
	for s.Scan() {
		frames = append(frames, append([]byte(nil), s.Bytes()...))
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	return frames
}
func compareSplits(t *testing.T, frames ...[]byte) {
	t.Helper()
	run := func(old bool) ([]rawlog.Record, [4]uint64, map[string]time.Time) {
		samples.Lock()
		samples.last = map[string]time.Time{}
		samples.Unlock()
		now := time.Unix(123456, 0)
		a := &Adapter{clock: func() time.Time { return now }, universe: map[string]struct{}{"BTCUSDT": {}, "龙虾USDT": {}}}
		sh := venue.Shard{Name: "diff"}
		var out []rawlog.Record
		for _, f := range frames {
			for i := 0; i < 3; i++ {
				if old {
					out = append(out, a.referenceSplit(sh, f)...)
				} else {
					out = append(out, a.Split(sh, f)...)
				}
				now = now.Add(31 * time.Second)
			}
		}
		return out, [4]uint64{a.UnknownFields(), a.Deduplicated(), a.SubscribeAcks(), a.SubscribeErrors()}, samples.last
	}
	a, b, c := run(true)
	x, y, z := run(false)
	if !reflect.DeepEqual(a, x) || b != y || !reflect.DeepEqual(c, z) {
		t.Fatalf("frames %q\nold=%+v %v %v\nnew=%+v %v %v", frames, a, b, c, x, y, z)
	}
}
func TestSplitDifferential(t *testing.T) {
	frames := fixtureFrames(t)
	for _, s := range []string{
		` { "arg": {"topic":"ticker","symbol":"龙虾USDT"},"action":"snapshot","ts":1e+2,"data":[{"symbol":"龙虾USDT","markPrice":"1","x":{"a":[1,"a\\\"b",null]}}]} `,
		`{"arg":{"topic":"publicTrade","symbol":"BTCUSDT"},"data":[{"p":1},{"symbol":"ETHUSDT","p":2}]}`,
		`{"arg":{"topic":"ticker","symbol":"BTCUSDT"},"data":{"symbol":"BTCUSDT","markPrice":"2","ts":1}}`,
		`{"arg":{"topic":"funding-rate","symbol":"BTCUSDT"},"data":[]}`, `{"data":null}`, `{"data":[{},]}`,
		`{"event":"subscribe"}`, `{"event":"error","msg":"bad"}`, `{"data":{},"data":[]}`,
		`{"arg":{"topic":"position-tier","symbol":"BTCUSDT"},"data":[{"list":[{"tier":1,"x":2}]}]}`,
		`{"arg":{"topic":"liquidation"},"data":[{"symbol":"BTCUSDT","price":"1"}]}`,
		`{"arg":{"topic":"ticker","symbol":"BTCUSDT"},"action":"update","data":{"\u0073ymbol":"BTCUSDT","x":-1.02e+20}}`,
	} {
		frames = append(frames, []byte(s))
	}
	for _, f := range frames {
		compareSplits(t, f)
	}
	compareSplits(t, frames...)
}
func TestSplitMutations(t *testing.T) {
	for _, f := range fixtureFrames(t) {
		for i := range f {
			compareSplits(t, f[:i])
			m := append([]byte(nil), f...)
			m[i] ^= 1
			compareSplits(t, m)
		}
	}
}
func FuzzSplitDifferential(f *testing.F) {
	for _, b := range fixtureFrames(f) {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			t.Skip()
		}
		compareSplits(t, b)
	})
}
func BenchmarkSplit(b *testing.B) {
	frames := map[string]string{
		"trade":       `{"arg":{"topic":"publicTrade","symbol":"BTCUSDT"},"data":[{"p":"1","v":"2","T":123}]}`,
		"ticker":      `{"arg":{"topic":"ticker","symbol":"BTCUSDT"},"data":[{"symbol":"BTCUSDT","markPrice":"1","lastPrice":"2","ts":1}]}`,
		"funding":     `{"arg":{"topic":"funding-rate","symbol":"BTCUSDT"},"data":[{"fundingRate":"1"}]}`,
		"tier":        `{"arg":{"topic":"position-tier","symbol":"BTCUSDT"},"data":[{"list":[{"tier":1,"mmr":"1"}]}]}`,
		"liquidation": `{"arg":{"topic":"liquidation"},"data":[{"symbol":"BTCUSDT","price":"1"}]}`,
	}
	for name, frame := range frames {
		for _, old := range []bool{true, false} {
			label := "new"
			if old {
				label = "old"
			}
			b.Run(name+"/"+label, func(b *testing.B) {
				a := &Adapter{universe: map[string]struct{}{"BTCUSDT": {}}}
				sh := venue.Shard{Name: "bench"}
				msg := []byte(frame)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if old {
						a.referenceSplit(sh, msg)
					} else {
						a.Split(sh, msg)
					}
				}
			})
		}
	}
}
