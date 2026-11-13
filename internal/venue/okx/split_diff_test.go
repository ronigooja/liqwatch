package okx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/venuetest"
)

// legacyFilterItem and legacySplit preserve the pre-optimization byte-level behavior.
func legacyFilterItem(channel string, entry []byte) ([]byte, error) {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(entry, &values); err != nil || values == nil {
		return nil, fmt.Errorf("invalid item")
	}
	d := json.NewDecoder(bytes.NewReader(entry))
	if _, err := d.Token(); err != nil {
		return nil, err
	}
	var b bytes.Buffer
	b.WriteByte('{')
	first := true
	rule := fieldRules[channel]
	for d.More() {
		tok, err := d.Token()
		if err != nil {
			return nil, err
		}
		key := tok.(string)
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, err
		}
		if rule.drop[key] {
			continue
		}
		if !rule.keep[key] {
			unknownFields.Add(1)
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		name, _ := json.Marshal(key)
		b.Write(name)
		b.WriteByte(':')
		b.Write(value)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

func legacySplit(a *Adapter, sh venue.Shard, msg []byte) []rawlog.Record {
	if string(msg) == "pong" {
		return nil
	}
	var e struct {
		Event  string          `json:"event"`
		Arg    arg             `json:"arg"`
		Data   json.RawMessage `json:"data"`
		RawArg json.RawMessage `json:"-"`
	}
	if json.Unmarshal(msg, &e) != nil {
		return gap(msg)
	}
	switch e.Event {
	case "subscribe":
		a.subscribeAcks.Add(1)
		return nil
	case "error", "channel-conn-count-error", "notice":
		a.subscribeErrors.Add(1)
		var frame any = json.RawMessage(msg)
		if len(msg) > 4096 {
			frame = string(msg[:4096])
		}
		b, _ := json.Marshal(map[string]any{"event": "subscribe_error", "frame": frame})
		return []rawlog.Record{{Channel: "gap", Payload: b}}
	}
	var envelope map[string]json.RawMessage
	_ = json.Unmarshal(msg, &envelope)
	e.RawArg = envelope["arg"]
	if len(e.Data) == 0 {
		if e.Event != "" {
			return nil
		}
		return gap(msg)
	}
	channels := map[string]string{"trades-all": "trade", "mark-price": "mark", "index-tickers": "index", "funding-rate": "funding", "open-interest": "oi", "liquidation-orders": "liquidation"}
	ch, ok := channels[e.Arg.Channel]
	if !ok {
		return gap(msg)
	}
	var entries []json.RawMessage
	if json.Unmarshal(e.Data, &entries) != nil {
		return gap(msg)
	}
	var out []rawlog.Record
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, entry := range entries {
		var item struct {
			InstID string `json:"instId"`
		}
		if json.Unmarshal(entry, &item) != nil {
			out = append(out, gap(msg)...)
			continue
		}
		sym := item.InstID
		if sym == "" {
			sym = e.Arg.InstID
		}
		if e.Arg.Channel == "index-tickers" {
			sym = a.indexSymbols[sym]
		}
		if sym == "" {
			out = append(out, gap(msg)...)
			continue
		}
		if !a.universe[sym] {
			if e.Arg.Channel == "liquidation-orders" {
				continue
			}
			out = append(out, gap(msg)...)
			continue
		}
		filtered, err := legacyFilterItem(e.Arg.Channel, entry)
		if err != nil {
			out = append(out, gap(msg)...)
			continue
		}
		payload := append([]byte(`{"arg":`), e.RawArg...)
		payload = append(payload, `,"data":[`...)
		payload = append(payload, filtered...)
		payload = append(payload, ']', '}')
		out = append(out, rawlog.Record{Channel: ch, Symbol: sym, Payload: payload})
		if sampleDue(sh.Name, ch) {
			sample := append([]byte(`{"sample":`), msg...)
			sample = append(sample, '}')
			out = append(out, rawlog.Record{Channel: ch, Symbol: sym, Payload: sample})
		}
	}
	return out
}

func TestSplitDifferential(t *testing.T) {
	a, shards := fixtureAdapter(t)
	var frames [][]byte
	for _, path := range []string{"testdata/public.jsonl", "testdata/business.jsonl"} {
		frames = append(frames, venuetest.Fixture(t, path)...)
	}
	for _, s := range []string{
		`{"arg":{"channel":"mark-price","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","x":"\\\"\u263a","z":[{"a":1e-5},null]}]}`,
		` { "arg" : {"channel":"liquidation-orders","instId":"BTC-USDT-SWAP"}, "data" : [ {"details":[{"a":[1,2]}],"instId":null} ] } `,
		`{"arg":{"channel":"index-tickers","instId":"BTC-USDT"},"data":[{"open24h":1,"idxPx":null,"idxPx":1E+5,"\u0074s":"x"}]}`,
		`{"arg":{"channel":"mark-price"},"data":[]}`,
		`{"arg":{"channel":"mark-price"},"data":null}`,
		`{"arg":{"channel":"mark-price"},"data":[`,
		`{"arg":{"channel":"mark-price"},"data":[{},]}`,
		`{"event":"notice","msg":"` + strings.Repeat("x", 10000) + `"}`,
	} {
		frames = append(frames, []byte(s))
	}
	oldClock := sampleClock
	sampleClock = func() time.Time { return time.Unix(12345, 0) }
	t.Cleanup(func() { sampleClock = oldClock })
	for i, frame := range frames {
		for _, sampled := range []bool{false, true} {
			reset := func() {
				sampler.Lock()
				sampler.last = make(map[string]time.Time)
				if sampled {
					for _, ch := range []string{"trade", "mark", "index", "funding", "oi", "liquidation"} {
						sampler.last[shards[0].Name+"\x00"+ch] = sampleClock()
					}
				}
				sampler.Unlock()
			}
			reset()
			before := unknownFields.Load()
			acks, errs := a.SubscribeAcks(), a.SubscribeErrors()
			want := legacySplit(a, shards[0], frame)
			unknown, ack, errCount := unknownFields.Load()-before, a.SubscribeAcks()-acks, a.SubscribeErrors()-errs
			reset()
			before, acks, errs = unknownFields.Load(), a.SubscribeAcks(), a.SubscribeErrors()
			got := a.Split(shards[0], frame)
			if !reflect.DeepEqual(got, want) || unknownFields.Load()-before != unknown || a.SubscribeAcks()-acks != ack || a.SubscribeErrors()-errs != errCount {
				t.Fatalf("frame %d sampled %v: %s\ngot %+v want %+v", i, sampled, frame, got, want)
			}
		}
	}
}

func compareSplit(t *testing.T, frame []byte) {
	t.Helper()
	a := &Adapter{universe: map[string]bool{"BTC-USDT-SWAP": true}, indexSymbols: map[string]string{"BTC-USDT": "BTC-USDT-SWAP"}}
	sh := venue.Shard{Name: "differential"}
	for _, sampled := range []bool{false, true} {
		reset := func() {
			sampler.Lock()
			sampler.last = make(map[string]time.Time)
			if sampled {
				for _, ch := range []string{"trade", "mark", "index", "funding", "oi", "liquidation"} {
					sampler.last[sh.Name+"\x00"+ch] = time.Now().Add(time.Hour)
				}
			}
			sampler.Unlock()
		}
		reset()
		before, acks, errs := unknownFields.Load(), a.SubscribeAcks(), a.SubscribeErrors()
		want := legacySplit(a, sh, frame)
		unknown, ack, errCount := unknownFields.Load()-before, a.SubscribeAcks()-acks, a.SubscribeErrors()-errs
		reset()
		before, acks, errs = unknownFields.Load(), a.SubscribeAcks(), a.SubscribeErrors()
		got := a.Split(sh, frame)
		if !reflect.DeepEqual(got, want) || unknownFields.Load()-before != unknown || a.SubscribeAcks()-acks != ack || a.SubscribeErrors()-errs != errCount {
			t.Fatalf("sampled %v frame %q: got %+v want %+v; counters got %d/%d/%d want %d/%d/%d", sampled, frame, got, want, unknownFields.Load()-before, a.SubscribeAcks()-acks, a.SubscribeErrors()-errs, unknown, ack, errCount)
		}
	}
}

func TestSplitFixtureMutations(t *testing.T) {
	for _, path := range []string{"testdata/public.jsonl", "testdata/business.jsonl"} {
		for _, frame := range venuetest.Fixture(t, path) {
			for i := range frame {
				compareSplit(t, frame[:i])
				mutated := bytes.Clone(frame)
				mutated[i] ^= 1
				compareSplit(t, mutated)
			}
		}
	}
}

func FuzzSplitDifferential(f *testing.F) {
	for _, path := range []string{"testdata/public.jsonl", "testdata/business.jsonl"} {
		for _, frame := range venuetest.Fixture(f, path) {
			f.Add(frame)
		}
	}
	f.Fuzz(func(t *testing.T, frame []byte) { compareSplit(t, frame) })
}

func BenchmarkSplit(b *testing.B) {
	frames := map[string]string{
		"trade":       `{"arg":{"channel":"trades-all","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","tradeId":"1","px":"123","sz":"1","side":"buy","ts":"123"}]}`,
		"mark":        `{"arg":{"channel":"mark-price","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","markPx":"123","ts":"123"}]}`,
		"index":       `{"arg":{"channel":"index-tickers","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","idxPx":"123","open24h":"1","ts":"123"}]}`,
		"funding":     `{"arg":{"channel":"funding-rate","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","fundingRate":"0.1","ts":"123"}]}`,
		"oi":          `{"arg":{"channel":"open-interest","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","oi":"100","ts":"123"}]}`,
		"liquidation": `{"arg":{"channel":"liquidation-orders"},"data":[{"instId":"BTC-USDT-SWAP","details":[{"px":"123","sz":"1"}]}]}`,
	}
	a := &Adapter{universe: map[string]bool{"BTC-USDT-SWAP": true}, indexSymbols: map[string]string{"BTC-USDT": "BTC-USDT-SWAP"}}
	sh := venue.Shard{Name: "benchmark"}
	for name, frame := range frames {
		for _, impl := range []struct {
			name  string
			split func(venue.Shard, []byte) []rawlog.Record
		}{{"old", func(sh venue.Shard, p []byte) []rawlog.Record { return legacySplit(a, sh, p) }}, {"new", a.Split}} {
			b.Run(name+"/"+impl.name, func(b *testing.B) {
				p := []byte(frame)
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					impl.split(sh, p)
				}
			})
		}
	}
}
