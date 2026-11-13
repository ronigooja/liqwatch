package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/venuetest"
)

func TestDiscoveryAndShards(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/contracts/active":
			fmt.Fprint(w, `{"code":"200000","data":[{"symbol":"中文USDTM","settleCurrency":"USDT","type":"FFWCSX","status":"Open"},{"symbol":"NO","settleCurrency":"USDC","type":"FFWCSX","status":"Open"}]}`)
		case "/api/v1/bullet-public":
			if r.Method != "POST" {
				t.Error("token method")
			}
			fmt.Fprint(w, `{"code":"200000","data":{"token":"abc","instanceServers":[{"endpoint":"wss://example.com/ws","pingInterval":18000,"pingTimeout":10000}]}}`)
		default:
			t.Error(r.URL.Path)
		}
	}))
	defer srv.Close()
	a := &Adapter{BaseURL: srv.URL}
	syms, err := a.Symbols(context.Background())
	if err != nil || len(syms) != 1 || syms[0] != "中文USDTM" {
		t.Fatalf("symbols %v %v", syms, err)
	}
	for i := 0; i < 50; i++ {
		syms = append(syms, fmt.Sprintf("S%d", i))
	}
	shards, err := a.Shards(context.Background(), syms)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != 5 {
		t.Fatalf("shards %d", len(shards))
	}
	ids := map[string]bool{}
	for _, s := range shards[:3] {
		u, _ := url.Parse(s.URL)
		id := u.Query().Get("connectId")
		if id == "" || ids[id] || u.Query().Get("token") != "abc" {
			t.Fatal("bad token or id")
		}
		ids[id] = true
		if s.MaxAge != 12*time.Hour {
			t.Fatal(s.MaxAge)
		}
		d, f := a.Heartbeat(s)
		if d != 9*time.Second {
			t.Fatal(d)
		}
		_, p := f()
		if !strings.Contains(string(p), `"type":"ping"`) {
			t.Fatal(string(p))
		}
	}
}

func TestClassicRefresh(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/bullet-public" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		calls++
		fmt.Fprintf(w, `{"code":"200000","data":{"token":"token%d","instanceServers":[{"endpoint":"wss://example.com/ws","pingInterval":18000}]}}`, calls)
	}))
	defer srv.Close()
	a := &Adapter{BaseURL: srv.URL}
	shards, err := a.Shards(context.Background(), []venue.Symbol{"X"})
	if err != nil {
		t.Fatal(err)
	}
	if shards[0].Refresh == nil || shards[1].Refresh != nil {
		t.Fatal("unexpected refresh functions")
	}
	ids := map[string]bool{}
	initial, _ := url.Parse(shards[0].URL)
	ids[initial.Query().Get("connectId")] = true
	for i := 2; i <= 3; i++ {
		raw, err := shards[0].Refresh(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		id := u.Query().Get("connectId")
		if id == "" || ids[id] || u.Query().Get("token") != fmt.Sprintf("token%d", i) {
			t.Fatalf("invalid refreshed URL: %s", raw)
		}
		ids[id] = true
	}
}

func TestFixtures(t *testing.T) {
	a := &Adapter{}
	counts := map[string]int{}
	frames := venuetest.Fixture(t, "testdata/classic.jsonl")
	for _, f := range frames {
		var x struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(f, &x); err != nil {
			t.Fatal(err)
		}
		r := a.Split(venue.Shard{}, f)
		if x.Type == "message" {
			if len(r) < 1 || r[0].Symbol == "" || !json.Valid(r[0].Payload) {
				t.Fatalf("bad split %s", f)
			}
			counts[r[0].Channel]++
		} else if len(r) != 0 {
			t.Fatalf("control %s", f)
		}
	}
	if counts["mark"] == 0 || counts["trade"] == 0 {
		t.Fatal(counts)
	}
	for _, p := range []string{`{"type":"message","topic":"/new:X","data":{}}`, `{"T":"new","d":{}}`, `not json`} {
		if r := a.Split(venue.Shard{}, []byte(p)); len(r) != 1 || r[0].Channel != "gap" {
			t.Fatal(p)
		}
	}
	r := a.Split(venue.Shard{}, []byte(`{"T":"mark-price","d":{"s":"XBTUSDTM","oi":"123"}}`))
	if len(r) < 1 || r[0].Channel != "utamark" {
		t.Fatal(r)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, f := range frames {
				a.Split(venue.Shard{}, f)
			}
		}()
	}
	wg.Wait()
}

func TestFilteringAndSampling(t *testing.T) {
	samples.Lock()
	oldNow, oldLast := samples.now, samples.last
	now := time.Unix(10000, 0)
	samples.now = func() time.Time { return now }
	samples.last = make(map[string]time.Time)
	samples.Unlock()
	defer func() { samples.Lock(); samples.now, samples.last = oldNow, oldLast; samples.Unlock() }()
	a := &Adapter{}
	sh := venue.Shard{Name: "test-filter"}
	frame := `{"T":"mark-price","d":{"s":"X","mp":1.2300,"ip":"2.00","oi":"3","bp":"4","ts":1,"newField":"\\u0061"}}`
	before := a.UnknownFields()
	r := a.Split(sh, []byte(frame))
	want := `{"T":"mark-price","d":{"s":"X","mp":1.2300,"ip":"2.00","oi":"3","ts":1,"newField":"\\u0061"}}`
	if len(r) != 2 || string(r[0].Payload) != want || string(r[1].Payload) != `{"sample":`+frame+`}` || a.UnknownFields() != before+1 {
		t.Fatalf("records %v unknown %d", r, a.UnknownFields()-before)
	}
	r = a.Split(sh, []byte(frame))
	if len(r) != 1 || string(r[0].Payload) != want {
		t.Fatal(r)
	}
	now = now.Add(time.Hour)
	if r = a.Split(sh, []byte(frame)); len(r) != 2 {
		t.Fatal(r)
	}
	trade := `{"topic":"/contractMarket/execution:X","type":"message","subject":"match","sn":5,"data":{"symbol":"X","sequence":5,"side":"buy","size":1,"price":"1.00","takerOrderId":"a","makerOrderId":"b","tradeId":"5","ts":123}}`
	r = a.Split(sh, []byte(trade))
	if string(r[0].Payload) != `{"topic":"/contractMarket/execution:X","type":"message","subject":"match","data":{"symbol":"X","side":"buy","size":1,"price":"1.00","tradeId":"5","ts":123}}` {
		t.Fatal(string(r[0].Payload))
	}
}

func TestWebsocketFrames(t *testing.T) {
	server := venuetest.NewServer(t, func(c *venuetest.Conn) {
		for i := 0; i < 2; i++ {
			select {
			case f := <-c.Frames:
				var m map[string]any
				if json.Unmarshal(f.Payload, &m) != nil {
					t.Error("invalid frame")
				}
				if i == 0 && m["type"] != "subscribe" {
					t.Error(m)
				}
				if i == 1 && m["type"] != "ping" {
					t.Error(m)
				}
			case <-time.After(time.Second):
				t.Error("timeout")
			}
		}
	})
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	a := &Adapter{intervals: map[string]time.Duration{server.URL: time.Second}}
	sh := venue.Shard{URL: server.URL, Streams: []string{"/contractMarket/execution:XBTUSDTM"}}
	m := a.Subscribe(sh)
	if len(m) != 1 {
		t.Fatal(m)
	}
	ws.Write(ctx, websocket.MessageText, m[0])
	_, f := a.Heartbeat(sh)
	typ, p := f()
	ws.Write(ctx, websocket.MessageType(typ), p)
}
