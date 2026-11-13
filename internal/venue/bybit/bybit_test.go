package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/venuetest"
)

func TestSymbols(t *testing.T) {
	var calls int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("category") != "linear" {
			t.Error("category")
		}
		if r.URL.Query().Get("cursor") == "" {
			fmt.Fprint(w, `{"retCode":0,"result":{"nextPageCursor":"next","list":[{"symbol":"BTCUSDT","contractType":"LinearPerpetual","status":"Trading","quoteCoin":"USDT","settleCoin":"USDT"},{"symbol":"NO","contractType":"LinearFutures","status":"Trading","quoteCoin":"USDT","settleCoin":"USDT"}]}}`)
		} else {
			fmt.Fprint(w, `{"retCode":0,"result":{"list":[{"symbol":"ETHUSDT","contractType":"LinearPerpetual","status":"Trading","quoteCoin":"USDT","settleCoin":"USDT"},{"symbol":"NO2","contractType":"LinearPerpetual","status":"Trading","quoteCoin":"USDT","settleCoin":"USDC"}]}}`)
		}
	}))
	defer s.Close()
	a := Adapter{InstrumentsURL: s.URL}
	got, e := a.Symbols(context.Background())
	if e != nil || !reflect.DeepEqual(got, []string{"BTCUSDT", "ETHUSDT"}) || calls != 2 {
		t.Fatalf("%v %v calls %d", got, e, calls)
	}
}
func TestShards(t *testing.T) {
	a := Adapter{}
	var syms []string
	for i := 0; i < 100; i++ {
		syms = append(syms, fmt.Sprintf("S%dUSDT", i))
	}
	sh, e := a.Shards(context.Background(), syms)
	if e != nil || len(sh) != 4 {
		t.Fatalf("%v %v", sh, e)
	}
	n := 0
	for _, s := range sh {
		n += len(s.Streams)
		if len(s.Streams) > 90 {
			t.Fatal("too many")
		}
		for _, b := range a.Subscribe(s) {
			var x struct {
				Args []string `json:"args"`
			}
			if json.Unmarshal(b, &x) != nil || len(x.Args) > 30 {
				t.Fatal(string(b))
			}
		}
	}
	if n != 300 {
		t.Fatal(n)
	}
}
func TestFixturesConcurrent(t *testing.T) {
	a := Adapter{}
	frames := venuetest.Fixture(t, "testdata/frames.jsonl")
	counts := map[string]int{}
	for _, f := range frames {
		for _, r := range a.Split(venue.Shard{}, f) {
			counts[r.Channel]++
			if r.Channel != "gap" && (!json.Valid(r.Payload) || r.Symbol == "") {
				t.Fatalf("bad record %+v", r)
			}
		}
	}
	for _, ch := range []string{"trade", "mark", "liquidation"} {
		if counts[ch] == 0 {
			t.Errorf("missing %s: %v", ch, counts)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, f := range frames {
				a.Split(venue.Shard{}, f)
			}
		}()
	}
	wg.Wait()
	if r := a.Split(venue.Shard{}, []byte(`{"topic":"unknown.BTCUSDT","data":{}}`)); len(r) != 1 || r[0].Channel != "gap" {
		t.Fatal(r)
	}
	if a.Classes() == nil {
		t.Fatal("classes")
	}
	if _, ok := a.Classes()("liquidation"); ok {
		t.Fatal("liquidations must not be droppable")
	}
}
func TestWebSocket(t *testing.T) {
	a := Adapter{}
	sh, _ := a.Shards(context.Background(), []string{"BTCUSDT"})
	frames := venuetest.Fixture(t, "testdata/frames.jsonl")
	s := venuetest.NewServer(t, func(c *venuetest.Conn) {
		for i := 0; i < 2; i++ {
			select {
			case f := <-c.Frames:
				var x map[string]any
				if json.Unmarshal(f.Payload, &x) != nil {
					t.Error("invalid client frame")
				}
				if i == 0 && x["op"] != "subscribe" {
					t.Error(x)
				}
				if i == 1 && x["op"] != "ping" {
					t.Error(x)
				}
			case <-time.After(3 * time.Second):
				t.Error("missing frame")
				return
			}
		}
		for _, f := range frames {
			if c.Send(websocket.MessageText, f) != nil {
				return
			}
		}
	})
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, e := websocket.Dial(ctx, s.URL, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer c.CloseNow()
	for _, b := range a.Subscribe(sh[0]) {
		if e = c.Write(ctx, websocket.MessageText, b); e != nil {
			t.Fatal(e)
		}
	}
	every, ping := a.Heartbeat(sh[0])
	if every != 20*time.Second {
		t.Fatal(every)
	}
	typ, b := ping()
	if e = c.Write(ctx, websocket.MessageType(typ), b); e != nil {
		t.Fatal(e)
	}
	n := 0
	for range frames {
		_, b, e = c.Read(ctx)
		if e != nil {
			t.Fatal(e)
		}
		n += len(a.Split(sh[0], b))
	}
	if n == 0 {
		t.Fatal("no records")
	}
}

func TestSymbolsErrors(t *testing.T) {
	for _, body := range []string{`{"retCode":10006,"retMsg":"Too many visits"}`, `not-json`, `{"retCode":0,"result":{"nextPageCursor":"again"}}`} {
		t.Run(body, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
			defer s.Close()
			a := Adapter{InstrumentsURL: s.URL}
			if _, err := a.Symbols(context.Background()); err == nil {
				t.Error("expected error")
			}
		})
	}
}
func TestSplitControlAndUnknown(t *testing.T) {
	a := Adapter{}
	for _, s := range []string{`{"op":"subscribe","success":true}`, `{"op":"ping","ret_msg":"pong"}`} {
		if r := a.Split(venue.Shard{}, []byte(s)); len(r) != 0 {
			t.Fatal(s, r)
		}
	}
	if a.SubscribeAcks() != 1 || a.SubscribeErrors() != 0 {
		t.Fatal(a.SubscribeAcks(), a.SubscribeErrors())
	}
	for _, s := range []string{`{"data":{},"topic":"other.BTCUSDT"}`, `{"data":{},"topic":"tickers."}`, `{"topic":"tickers.BTCUSDT"}`, `not json`} {
		if r := a.Split(venue.Shard{}, []byte(s)); len(r) != 1 || r[0].Channel != "gap" {
			t.Fatal(s, r)
		}
	}
}

func TestSubscribeResponses(t *testing.T) {
	a := &Adapter{}
	frames := venuetest.Fixture(t, "testdata/frames.jsonl")
	frame := frames[len(frames)-1]
	for _, f := range [][]byte{frame, []byte(`{"op":"subscribe","success":true,"ret_msg":"bad"}`), []byte(`{"op":"subscribe","success":false}`)} {
		r := a.Split(venue.Shard{}, f)
		if len(r) != 1 || r[0].Channel != "gap" || r[0].Symbol != "" {
			t.Fatalf("bad record: %+v", r)
		}
		var p struct {
			Event string          `json:"event"`
			Frame json.RawMessage `json:"frame"`
		}
		if json.Unmarshal(r[0].Payload, &p) != nil || p.Event != "subscribe_error" || string(p.Frame) != string(f) {
			t.Fatalf("bad payload: %s", r[0].Payload)
		}
	}
	large := []byte(`{"op":"subscribe","success":false,"ret_msg":"` + strings.Repeat("x", 5000) + `"}`)
	r := a.Split(venue.Shard{}, large)
	var p struct {
		Frame string `json:"frame"`
	}
	if json.Unmarshal(r[0].Payload, &p) != nil || p.Frame != string(large[:4096]) {
		t.Fatal("bad truncated frame")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				a.Split(venue.Shard{}, frame)
				a.Split(venue.Shard{}, []byte(`{"op":"subscribe","success":true}`))
				_ = a.SubscribeAcks()
				_ = a.SubscribeErrors()
			}
		}()
	}
	wg.Wait()
	if a.SubscribeAcks() != 2000 || a.SubscribeErrors() != 2004 {
		t.Fatal(a.SubscribeAcks(), a.SubscribeErrors())
	}
	if (&Adapter{}).SubscribeAcks() != 0 || (&Adapter{}).SubscribeErrors() != 0 {
		t.Fatal("counters are not per adapter")
	}
}

func TestInstrumentFixture(t *testing.T) {
	b, e := os.ReadFile("testdata/instruments.json")
	if e != nil {
		t.Fatal(e)
	}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(b) }))
	defer s.Close()
	a := Adapter{InstrumentsURL: s.URL}
	got, e := a.Symbols(context.Background())
	if e != nil || !reflect.DeepEqual(got, []string{"0GUSDT", "BTCUSDT", "ETHUSDT"}) {
		t.Fatal(got, e)
	}
}

func TestSubscribeLongSymbols(t *testing.T) {
	a := Adapter{}
	sym := strings.Repeat("X", 5000)
	sh, e := a.Shards(context.Background(), []string{sym, sym})
	if e != nil {
		t.Fatal(e)
	}
	for _, s := range sh {
		b := a.Subscribe(s)[0]
		var x struct {
			Args json.RawMessage `json:"args"`
		}
		if json.Unmarshal(b, &x) != nil || len(x.Args) > 21000 {
			t.Fatal("args length", len(x.Args))
		}
	}
	if _, e = a.Shards(context.Background(), []string{strings.Repeat("X", 21000)}); e == nil {
		t.Fatal("expected length error")
	}
}

func TestRegistry(t *testing.T) {
	if venue.Get("bybit") == nil {
		t.Fatal("adapter not registered")
	}
}

func TestClasses(t *testing.T) {
	a := Adapter{}
	for _, tt := range []struct {
		channel, class string
		ok             bool
	}{{"mark", "mark", true}, {"trade", "trade", true}, {"liquidation", "", false}, {"gap", "", false}} {
		class, ok := a.Classes()(tt.channel)
		if class != tt.class || ok != tt.ok {
			t.Errorf("%s: %q %v", tt.channel, class, ok)
		}
	}
}

func TestFilteringAndSampling(t *testing.T) {
	a := &Adapter{}
	sampleMu.Lock()
	oldNow, oldTimes := sampleNow, sampleTimes
	now := time.Unix(10000, 0)
	sampleNow = func() time.Time { return now }
	sampleTimes = map[string]time.Time{}
	sampleMu.Unlock()
	defer func() { sampleMu.Lock(); sampleNow, sampleTimes = oldNow, oldTimes; sampleMu.Unlock() }()
	sh := venue.Shard{Name: "filter-test"}
	frame := `{"topic":"tickers.BTCUSDT","type":"delta","data":{"symbol":"BTCUSDT","bid1Price":"3","markPrice":"1.2300","newField":"\\u0031"},"cs":123,"ts":456}`
	before := a.UnknownFieldsSeen()
	r := a.Split(sh, []byte(frame))
	want := `{"topic":"tickers.BTCUSDT","type":"delta","data":{"symbol":"BTCUSDT","markPrice":"1.2300","newField":"\\u0031"},"cs":123,"ts":456}`
	if len(r) != 2 || string(r[0].Payload) != want || string(r[1].Payload) != `{"sample":`+frame+`}` || a.UnknownFieldsSeen() != before+1 {
		t.Fatalf("records: %+v unknown: %d", r, a.UnknownFieldsSeen()-before)
	}
	if r = a.Split(sh, []byte(frame)); len(r) != 1 {
		t.Fatal(r)
	}
	now = now.Add(time.Hour)
	if r = a.Split(sh, []byte(frame)); len(r) != 2 {
		t.Fatal(r)
	}
	drop := `{"topic":"tickers.ETHUSDT","type":"delta","data":{"symbol":"ETHUSDT","bid1Size":"1"},"ts":1}`
	before = a.DroppedDeltas()
	if r = a.Split(sh, []byte(drop)); len(r) != 0 || a.DroppedDeltas() != before+1 {
		t.Fatal(r)
	}
	batch := `{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"s":"BTCUSDT","p":"1.00","L":"PlusTick"},{"s":"BTCUSDT","p":"2.00","L":"MinusTick"}]}`
	r = a.Split(sh, []byte(batch))
	if len(r) != 3 || r[0].Symbol != "BTCUSDT" || string(r[0].Payload) != `{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"p":"1.00"}]}` || string(r[1].Payload) != `{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"p":"2.00"}]}` {
		t.Fatal(r)
	}
	conflict := `{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"s":"ETHUSDT","p":"1"}]}`
	r = a.Split(sh, []byte(conflict))
	if len(r) != 1 || !strings.Contains(string(r[0].Payload), `"s":"ETHUSDT"`) {
		t.Fatal(r)
	}
}

func TestFixtureSizes(t *testing.T) {
	a := &Adapter{}
	type totals struct{ frames, records, before, after int }
	all := map[string]*totals{}
	for _, f := range venuetest.Fixture(t, "testdata/frames.jsonl") {
		var x struct {
			Topic string `json:"topic"`
		}
		if json.Unmarshal(f, &x) != nil {
			t.Fatal("bad fixture")
		}
		ch := map[string]string{"tickers": "mark", "publicTrade": "trade", "allLiquidation": "liquidation"}[strings.Split(x.Topic, ".")[0]]
		if ch == "" {
			continue
		}
		if all[ch] == nil {
			all[ch] = &totals{}
		}
		v := all[ch]
		v.frames++
		v.before += len(f)
		for _, r := range a.Split(venue.Shard{Name: "sizes"}, f) {
			if !strings.HasPrefix(string(r.Payload), `{"sample":`) {
				v.records++
				v.after += len(r.Payload)
			}
		}
	}
	for ch, v := range all {
		t.Logf("%s frames=%d records=%d before/frame=%.2f after/record=%.2f before total=%d after total=%d", ch, v.frames, v.records, float64(v.before)/float64(v.frames), float64(v.after)/float64(v.records), v.before, v.after)
	}
}
