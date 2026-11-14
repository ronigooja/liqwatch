package gate

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/venue"
)

func fixtureFrames() [][]byte {
	files, _ := filepath.Glob("testdata/*")
	var frames [][]byte
	for _, f := range files {
		b, e := os.ReadFile(f)
		if e == nil {
			for _, line := range bytes.Split(b, []byte("\n")) {
				if len(line) > 0 {
					frames = append(frames, line)
				}
			}
		}
	}
	return append(frames, []byte(`{"channel":"futures.tickers","event":"update","result":[{"contract":"龙虾_USDT","risk_limit_tiers":[{"x":1e-5}],"last":1,"unknown":"\\u1234"}]}`), []byte(`{"event":"subscribe","result":{"status":"success"}}`), []byte(`{"event":"error","error":{}}`), []byte(`{"channel":"x","channel":"y","result":null}`), []byte(`{"channel":"futures.trades","event":"update","result":[]}`))
}
func compareSplit(t testing.TB, msg []byte) {
	t.Helper()
	a, b := &Adapter{}, &Adapter{}
	syms := []string{"BTC_USDT", "ETH_USDT", "SOL_USDT", "PEPE_USDT", "龙虾_USDT", "AKE_USDT"}
	a.Shards(context.Background(), syms)
	b.Shards(context.Background(), syms)
	sh := venue.Shard{Name: "diff"}
	frameSampler.Lock()
	frameSampler.last = make(map[string]time.Time)
	frameSampler.Unlock()
	got := a.Split(sh, msg)
	frameSampler.Lock()
	frameSampler.last = make(map[string]time.Time)
	frameSampler.Unlock()
	want := b.splitReference(sh, msg)
	if !reflect.DeepEqual(got, want) || a.UnknownFields() != b.UnknownFields() || a.SubscribeAcks() != b.SubscribeAcks() || a.SubscribeErrors() != b.SubscribeErrors() {
		t.Fatalf("frame %q\ngot %#v %d %d %d\nwant %#v %d %d %d", msg, got, a.UnknownFields(), a.SubscribeAcks(), a.SubscribeErrors(), want, b.UnknownFields(), b.SubscribeAcks(), b.SubscribeErrors())
	}
}
func TestSplitDifferential(t *testing.T) {
	frameSampler.Lock()
	oldNow, oldLast := frameSampler.now, frameSampler.last
	frameSampler.now = func() time.Time { return time.Unix(10000, 0) }
	frameSampler.Unlock()
	defer func() {
		frameSampler.Lock()
		frameSampler.now, frameSampler.last = oldNow, oldLast
		frameSampler.Unlock()
	}()
	for _, f := range fixtureFrames() {
		compareSplit(t, f)
		for i := range f {
			compareSplit(t, f[:i])
			g := bytes.Clone(f)
			g[i] ^= 1
			compareSplit(t, g)
		}
	}
}
func FuzzSplit(f *testing.F) {
	for _, b := range fixtureFrames() {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) { compareSplit(t, b) })
}
func BenchmarkSplit(b *testing.B) {
	cases := map[string]string{
		"trades":               `{"time":123,"channel":"futures.trades","event":"update","result":[{"contract":"BTC_USDT","id":1,"size":"1","price":"2"}]}`,
		"tickers":              `{"time":123,"channel":"futures.tickers","event":"update","result":[{"contract":"BTC_USDT","last":"1","mark_price":"1","index_price":"2","volume_24h":"5"}]}`,
		"contract_stats":       `{"time":123,"channel":"futures.contract_stats","event":"update","result":[{"contract":"BTC_USDT","open_interest":"1","lsr_account":"2","mark_price":"3"}]}`,
		"contract_info":        `{"time":123,"channel":"futures.contract_info","event":"all","result":{"contract":"BTC_USDT","quanto_multiplier":"0.001","risk_limit_tiers":[{"risk_limit":"1","leverage":"20"}]}}`,
		"liquidations":         `{"time":123,"channel":"futures.public_liquidates","event":"update","result":[{"contract":"BTC_USDT","price":"2","size":"1"}]}`,
		"control_ack":          `{"channel":"futures.trades","event":"subscribe","result":{"status":"success"}}`,
		"control_pong":         `{"channel":"futures.pong","event":"","result":null}`,
		"fallback_escaped_key": `{"channel":"futures.trades","event":"update","result":[{"contract":"BTC_USDT","pr\u0069ce":"1"}]}`,
	}
	for name, text := range cases {
		frame := []byte(text)
		b.Run(name, func(b *testing.B) {
			for _, ref := range []bool{false, true} {
				label := "new"
				if ref {
					label = "old"
				}
				b.Run(label, func(b *testing.B) {
					a := &Adapter{}
					a.Shards(context.Background(), []string{"BTC_USDT"})
					sh := venue.Shard{Name: "bench"}
					a.Split(sh, frame)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if ref {
							a.splitReference(sh, frame)
						} else {
							a.Split(sh, frame)
						}
					}
					b.StopTimer()
					if !ref {
						b.ReportMetric(float64(a.Fallbacks())/float64(b.N+1)*100, "fallback%")
					}
				})
			}
		})
	}
}
