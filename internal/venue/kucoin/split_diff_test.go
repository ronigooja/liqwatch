package kucoin

import (
	"bufio"
	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func fixtureFrames(t testing.TB) [][]byte {
	t.Helper()
	paths, err := filepath.Glob("testdata/*.jsonl")
	if err != nil || len(paths) == 0 {
		t.Fatal("fixtures", err)
	}
	var frames [][]byte
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		s := bufio.NewScanner(f)
		s.Buffer(make([]byte, 4096), 16<<20)
		for s.Scan() {
			frames = append(frames, append([]byte(nil), s.Bytes()...))
		}
		if err := s.Err(); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	return frames
}
func compareSplits(t *testing.T, frame []byte) {
	t.Helper()
	run := func(reference bool) ([]rawlog.Record, uint64, uint64, uint64, map[string]time.Time) {
		unknownFields.Store(0)
		samples.last = map[string]time.Time{}
		now := time.Unix(123456, 0)
		samples.now = func() time.Time { return now }
		a := &Adapter{}
		var out []rawlog.Record
		for i := 0; i < 3; i++ {
			if i == 2 {
				now = now.Add(time.Hour)
			}
			if reference {
				out = append(out, a.referenceSplit(venue.Shard{Name: "diff"}, frame)...)
			} else {
				out = append(out, a.Split(venue.Shard{Name: "diff"}, frame)...)
			}
		}
		return out, unknownFields.Load(), a.SubscribeAcks(), a.SubscribeErrors(), samples.last
	}
	a, b, c, d, e := run(true)
	x, y, z, u, v := run(false)
	if !reflect.DeepEqual(a, x) || b != y || c != z || d != u || !reflect.DeepEqual(e, v) {
		t.Fatalf("frame %q\nold %v %d %d %d %v\nnew %v %d %d %d %v", frame, a, b, c, d, e, x, y, z, u, v)
	}
}
func TestSplitDifferential(t *testing.T) {
	oldNow, oldLast := samples.now, samples.last
	defer func() { samples.now, samples.last = oldNow, oldLast }()
	frames := fixtureFrames(t)
	frames = append(frames, []byte(`{"type":"ack","T":"mark-price","d":{"s":"X"}}`), []byte(`{"code":"","T":"mark-price","d":{"s":"X"}}`), []byte(`{"result":"true","T":"mark-price","d":{"s":"X"}}`))
	for _, s := range []string{` {"type":"message","topic":"/contractMarket/execution:X","subject":"match","data":{"x":-1e+10,"a":[{},null],"symbol":"\u20ac"}} `, `{"T":"mark-price","d":{"s":"X","mp":1,"bp":2}}`, `{"T":"mark-price","d":null}`, `{"type":"message","type":null,"data":{}}`, `{"\u0074ype":"ack"}`, `{"type":"error","code":400}`, `{"result":false}`, `null`, `[]`, `{"x":`, `{"T":"mark-price","d":{"s":"X","d":{}}}`} {
		frames = append(frames, []byte(s))
	}
	for _, frame := range frames {
		compareSplits(t, frame)
		for i := range frame {
			compareSplits(t, frame[:i])
			m := append([]byte(nil), frame...)
			m[i] ^= 1
			compareSplits(t, m)
		}
	}
}
func FuzzSplitDifferential(f *testing.F) {
	for _, frame := range fixtureFrames(f) {
		f.Add(frame)
	}
	f.Add([]byte(`{"T":"mark-price","d":{"s":"X","mp":1}}`))
	f.Fuzz(func(t *testing.T, frame []byte) {
		if len(frame) > 1<<20 {
			t.Skip()
		}
		oldNow, oldLast := samples.now, samples.last
		defer func() { samples.now, samples.last = oldNow, oldLast }()
		compareSplits(t, frame)
	})
}
func BenchmarkSplit(b *testing.B) {
	frames := map[string]string{"trade": `{"type":"message","topic":"/contractMarket/execution:X","subject":"match","data":{"symbol":"X","price":"1","size":2,"sequence":3}}`, "mark": `{"type":"message","topic":"/contract/instrument:X","subject":"mark.index.price","data":{"symbol":"X","markPrice":"1","indexPrice":"2"}}`, "utamark": `{"T":"mark-price","d":{"s":"X","mp":"1","ip":"2","oi":"3","ts":123,"bp":"4"}}`}
	for name, frame := range frames {
		for _, old := range []bool{true, false} {
			label := "new"
			if old {
				label = "old"
			}
			b.Run(name+"/"+label, func(b *testing.B) {
				a := &Adapter{}
				msg := []byte(frame)
				sh := venue.Shard{Name: "bench"}
				b.ReportAllocs()
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
