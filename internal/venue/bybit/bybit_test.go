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
	for _, s := range []string{`{"op":"subscribe","success":true}`, `{"op":"ping","ret_msg":"pong"}`, `{"op":"subscribe","success":false,"ret_msg":"bad"}`} {
		if r := a.Split(venue.Shard{}, []byte(s)); len(r) != 0 {
			t.Fatal(s, r)
		}
	}
	for _, s := range []string{`{"data":{},"topic":"other.BTCUSDT"}`, `{"data":{},"topic":"tickers."}`, `{"topic":"tickers.BTCUSDT"}`, `not json`} {
		if r := a.Split(venue.Shard{}, []byte(s)); len(r) != 1 || r[0].Channel != "gap" {
			t.Fatal(s, r)
		}
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
