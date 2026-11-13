package bitget

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/venuetest"
)

func TestSymbols(t *testing.T) {
	data, err := os.ReadFile("testdata/contracts.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(data) }))
	defer server.Close()
	a := &Adapter{ContractsURL: server.URL}
	got, err := a.Symbols(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"BTCUSDT", "ETHUSDT", "龙虾USDT"}) {
		t.Fatalf("symbols %v", got)
	}
}
func TestShardsSubscribeAndHeartbeat(t *testing.T) {
	a := &Adapter{}
	syms := make([]string, 25)
	for i := range syms {
		syms[i] = fmt.Sprintf("X%dUSDT", i)
	}
	shards, err := a.Shards(context.Background(), syms)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != 4 {
		t.Fatalf("shards %d", len(shards))
	}
	for _, sh := range shards {
		if len(sh.Streams) > 48 {
			t.Fatal("too many subscriptions")
		}
		for _, frame := range a.Subscribe(sh) {
			var x struct {
				Op   string         `json:"op"`
				Args []subscription `json:"args"`
			}
			if json.Unmarshal(frame, &x) != nil || x.Op != "subscribe" || len(x.Args) == 0 || len(frame) > 3500 {
				t.Fatalf("bad subscribe %s", frame)
			}
		}
	}
	every, ping := a.Heartbeat(shards[0])
	typ, p := ping()
	if every != 30*time.Second || typ != int(websocket.MessageText) || string(p) != "ping" {
		t.Fatal("bad heartbeat")
	}
}
func TestFixtures(t *testing.T) {
	a := &Adapter{}
	a.Shards(context.Background(), []string{"BTCUSDT", "ETHUSDT", "龙虾USDT", "AEROUSDT", "MAGMAUSDT"})
	counts := map[string]int{}
	for _, frame := range venuetest.Fixture(t, "testdata/frames.jsonl") {
		for _, r := range a.Split(venue.Shard{}, frame) {
			counts[r.Channel]++
			if r.Channel != "gap" && (r.Symbol == "" || !json.Valid(r.Payload)) {
				t.Fatalf("invalid record %+v", r)
			}
		}
	}
	for _, channel := range []string{"trade", "ticker", "funding", "tier", "liquidation", "gap"} {
		if counts[channel] == 0 {
			t.Errorf("no %s fixture record; counts %v", channel, counts)
		}
	}
}
func TestUnknownAndGlobal(t *testing.T) {
	a := &Adapter{}
	a.Shards(context.Background(), []string{"BTCUSDT", "龙虾USDT"})
	msg := []byte(`{"action":"update","arg":{"topic":"liquidation"},"data":[{"symbol":"BTCUSDT","price":"1"},{"symbol":"龙虾USDT","price":"2"},{"symbol":"OTHER","price":"3"}]}`)
	records := a.Split(venue.Shard{}, msg)
	if len(records) != 2 || records[1].Symbol != "龙虾USDT" || bytes.Contains(records[0].Payload, []byte("OTHER")) {
		t.Fatalf("records %+v", records)
	}
	if got := a.Split(venue.Shard{}, []byte(`{"arg":{"topic":"new"},"data":[{}]}`)); len(got) != 1 || got[0].Channel != "gap" {
		t.Fatalf("unknown %+v", got)
	}
	for _, m := range []string{"pong", `{"event":"subscribe"}`, `{"event":"error","code":30001}`} {
		if got := a.Split(venue.Shard{}, []byte(m)); len(got) != 0 {
			t.Fatalf("control %s %+v", m, got)
		}
	}
}
func TestConcurrentSplit(t *testing.T) {
	a := &Adapter{}
	a.Shards(context.Background(), []string{"BTCUSDT"})
	frame := []byte(`{"arg":{"topic":"ticker","symbol":"BTCUSDT"},"data":[{}]}`)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if len(a.Split(venue.Shard{}, frame)) != 1 {
					t.Error("split")
				}
			}
		}()
	}
	wg.Wait()
}
func TestWebsocket(t *testing.T) {
	a := &Adapter{}
	shards, _ := a.Shards(context.Background(), []string{"BTCUSDT"})
	sh := shards[0]
	server := venuetest.NewServer(t, func(c *venuetest.Conn) {
		for i := 0; i < len(a.Subscribe(sh))+1; i++ {
			select {
			case f := <-c.Frames:
				if i == 0 && !bytes.Contains(f.Payload, []byte(`"publicTrade"`)) {
					t.Errorf("subscribe %s", f.Payload)
				}
				if i > 0 && string(f.Payload) != "ping" {
					t.Errorf("ping %s", f.Payload)
				}
			case <-time.After(5 * time.Second):
				t.Error("timeout")
				return
			}
		}
		c.Send(websocket.MessageText, []byte(`{"arg":{"topic":"ticker","symbol":"BTCUSDT"},"data":[{}]}`))
	})
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	for _, frame := range a.Subscribe(sh) {
		if err := ws.Write(ctx, websocket.MessageText, frame); err != nil {
			t.Fatal(err)
		}
	}
	_, ping := a.Heartbeat(sh)
	typ, p := ping()
	if err := ws.Write(ctx, websocket.MessageType(typ), p); err != nil {
		t.Fatal(err)
	}
	_, p, err = ws.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if r := a.Split(sh, p); len(r) != 1 || r[0].Channel != "ticker" {
		t.Fatalf("result %+v", r)
	}
}
func TestClasses(t *testing.T) {
	f := (&Adapter{}).Classes()
	for _, x := range []struct {
		ch, cl string
		ok     bool
	}{{"ticker", "mark", true}, {"trade", "trade", true}, {"liquidation", "", false}, {"tier", "", false}} {
		c, ok := f(x.ch)
		if c != x.cl || ok != x.ok {
			t.Errorf("%s %s %t", x.ch, c, ok)
		}
	}
}
