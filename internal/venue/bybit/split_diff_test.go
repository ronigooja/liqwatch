package bybit

import (
	"bufio"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

func TestSplitDifferential(t *testing.T) {
	f, err := os.Open("testdata/frames.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var frames []string
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 4096), 16<<20)
	for s.Scan() {
		frames = append(frames, s.Text())
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	frames = append(frames,
		` { "topic" : "publicTrade.BTCUSDT", "data" : [ {"s":"BTCUSDT","p":1e-9,"L":null,"x":{"a":[1,"a\\\"b",null]}},{"s":"ETHUSDT","p":"\u20ac"} ], "ts":1 } `,
		`{"topic":"tickers.BTCUSDT","type":"delta","data":{"symbol":"BTCUSDT","bid1Price":"2"}}`,
		`{"topic":"tickers.BTCUSDT","type":"snapshot","data":{}}`,
		`{"topic":"allLiquidation.BTCUSDT","data":[{"s":"BTCUSDT","S":"Buy","v":"1"}]}`,
		`{"topic":"publicTrade.BTCUSDT","data":[]}`, `{"topic":"publicTrade.BTCUSDT","data":null}`,
		`{"topic":"publicTrade.BTCUSDT","data":[{"s":"BTCUSDT","p":1},{"s":"BTCUSDT","p":2}]}`,
		`{"topic":"tickers.BTCUSDT","data":{"x":1,"x":2},"data":{"s":"BTCUSDT"}}`,
		`{"TOPIC":"tickers.BTCUSDT","TYPE":"snapshot","DATA":{"x":1},"data":{"y":2}}`,
		`{"topic":"tickers.BTCUSDT","topic":null,"data":{"x":1}}`,
		`{"topic":"publicTrade.BTCUSDT","data":[{"x":1},null]}`,
		`{"op":"subscribe","success":false,"success":null}`,
		`{"topic":"tickers.BTCUSDT","data":{"a\u2028b":1,"\ud800":2,"<>&":3}}`,
		`{"topic":"tickers.BTCUSDT","data":{"x":-1.02e+20,"y":[{},[true,false,null]]}}`,

		`{"topic":"tickers.BTCUSDT","data":{"\u0073ymbol":"BTCUSDT","a\"b":"<>&"}}`,
		`{"topic":"tickers.BTCUSDT","data":{"x":1`, `{"topic":"tickers.BTCUSDT","data":[{},]}`,
		`{"op":"subscribe","success":true}`, `{"op":"subscribe","success":false,"ret_msg":"bad"}`,
		`{"topic":"tickers.BTCUSDT","data":{"x":"`+strings.Repeat("a", 100000)+`"}}`,
	)
	now := time.Unix(123456, 0)
	oldNow := sampleNow
	defer func() { sampleNow = oldNow }()
	sampleNow = func() time.Time { return now }
	for i, frame := range frames {
		t.Run(stringIndex(i), func(t *testing.T) {
			run := func(legacy bool) ([]rawlog.Record, uint64, uint64, uint64, uint64, map[string]time.Time) {
				unknownFields.Store(0)
				droppedDeltas.Store(0)
				sampleTimes = map[string]time.Time{}
				a := &Adapter{}
				sh := venue.Shard{Name: "test"}
				var records []rawlog.Record
				for j := 0; j < 2; j++ {
					if legacy {
						records = append(records, a.legacySplit(sh, []byte(frame))...)
					} else {
						records = append(records, a.Split(sh, []byte(frame))...)
					}
				}
				return records, unknownFields.Load(), droppedDeltas.Load(), a.SubscribeAcks(), a.SubscribeErrors(), sampleTimes
			}
			a, b, c, d, e, f := run(true)
			x, y, z, u, v, w := run(false)
			if !reflect.DeepEqual(a, x) || b != y || c != z || d != u || e != v || !reflect.DeepEqual(f, w) {
				t.Fatalf("frame %s\nlegacy=%+v %d %d %d %d %v\nnew=%+v %d %d %d %d %v", frame, a, b, c, d, e, f, x, y, z, u, v, w)
			}
		})
	}
}
func stringIndex(i int) string { return fmt.Sprint(i) }

func BenchmarkSplit(b *testing.B) {
	frames := map[string]string{
		"mark":        `{"topic":"tickers.BTCUSDT","type":"delta","data":{"symbol":"BTCUSDT","tickDirection":"PlusTick","markPrice":"69101.12","indexPrice":"69133.39","openInterest":"69827.276","bid1Price":"69095.80"},"ts":1794554843169}`,
		"trade":       `{"topic":"publicTrade.BTCUSDT","data":[{"T":123,"s":"BTCUSDT","S":"Buy","v":"1","p":"2","L":"PlusTick"},{"T":124,"s":"BTCUSDT","S":"Sell","v":"2","p":"3"}],"ts":123}`,
		"liquidation": `{"topic":"allLiquidation.BTCUSDT","data":[{"T":123,"s":"BTCUSDT","S":"Buy","v":"1","p":"2"}],"ts":123}`,
	}
	for name, frame := range frames {
		for _, legacy := range []bool{true, false} {
			label := "new"
			if legacy {
				label = "old"
			}
			b.Run(name+"/"+label, func(b *testing.B) {
				a := &Adapter{}
				msg := []byte(frame)
				sh := venue.Shard{Name: "bench"}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if legacy {
						a.legacySplit(sh, msg)
					} else {
						a.Split(sh, msg)
					}
				}
			})
		}
	}
}

func compareSplits(t *testing.T, frame []byte) {
	t.Helper()
	run := func(legacy bool) ([]rawlog.Record, uint64, uint64, uint64, uint64, map[string]time.Time) {
		unknownFields.Store(0)
		droppedDeltas.Store(0)
		sampleTimes = map[string]time.Time{}
		a := &Adapter{}
		sh := venue.Shard{Name: "fuzz"}
		var r []rawlog.Record
		for i := 0; i < 2; i++ {
			if legacy {
				r = append(r, a.legacySplit(sh, frame)...)
			} else {
				r = append(r, a.Split(sh, frame)...)
			}
		}
		return r, unknownFields.Load(), droppedDeltas.Load(), a.SubscribeAcks(), a.SubscribeErrors(), sampleTimes
	}
	a, b, c, d, e, f := run(true)
	x, y, z, u, v, w := run(false)
	if !reflect.DeepEqual(a, x) || b != y || c != z || d != u || e != v || !reflect.DeepEqual(f, w) {
		t.Fatalf("frame %q\nold %+v %d %d %d %d %v\nnew %+v %d %d %d %d %v", frame, a, b, c, d, e, f, x, y, z, u, v, w)
	}
}
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
func TestSplitMutations(t *testing.T) {
	old := sampleNow
	defer func() { sampleNow = old }()
	sampleNow = func() time.Time { return time.Unix(123456, 0) }
	for _, frame := range fixtureFrames(t) {
		for i := range frame {
			compareSplits(t, frame[:i])
			mut := append([]byte(nil), frame...)
			mut[i] ^= 1
			compareSplits(t, mut)
		}
	}
}
func FuzzSplitDifferential(f *testing.F) {
	for _, frame := range fixtureFrames(f) {
		f.Add(frame)
	}
	f.Add([]byte(`{"topic":"publicTrade.X","data":[{"s":"X","x":1}]}`))
	f.Fuzz(func(t *testing.T, frame []byte) {
		if len(frame) > 1<<20 {
			t.Skip()
		}
		old := sampleNow
		defer func() { sampleNow = old }()
		sampleNow = func() time.Time { return time.Unix(123456, 0) }
		compareSplits(t, frame)
	})
}
