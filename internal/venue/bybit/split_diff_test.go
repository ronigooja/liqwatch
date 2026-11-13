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
