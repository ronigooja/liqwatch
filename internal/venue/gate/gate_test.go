package gate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/venuetest"
)

func TestSymbols(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"name":"龙虾_USDT","status":"trading","in_delisting":false},{"name":"BAD_USDT","status":"trading","in_delisting":true},{"name":"WAIT_USDT","status":"prelaunch"},{"name":"BTC_USDT","status":"trading"},{"name":"X_BTC","status":"trading"}]`)
	}))
	defer s.Close()
	a := &Adapter{ContractsURL: s.URL}
	syms, e := a.Symbols(context.Background())
	if e != nil || fmt.Sprint(syms) != "[BTC_USDT 龙虾_USDT]" {
		t.Fatalf("%v %v", syms, e)
	}
}
func TestShardsAndSubscriptions(t *testing.T) {
	a := &Adapter{}
	syms := make([]string, 101)
	for i := range syms {
		syms[i] = fmt.Sprintf("S%d_USDT", i)
	}
	sh, e := a.Shards(context.Background(), syms)
	if e != nil || len(sh) != 3 {
		t.Fatal(sh, e)
	}
	for _, shard := range sh {
		if got := shard.Header.Get("X-Gate-Size-Decimal"); got != "1" {
			t.Errorf("shard %s decimal header = %q", shard.Name, got)
		}
	}
	if len(a.Subscribe(sh[0])) != 102 || len(a.Subscribe(sh[1])) != 3 || len(a.Subscribe(sh[2])) != 2 {
		t.Fatal("subscription counts")
	}
	for _, b := range a.Subscribe(sh[0]) {
		var m struct {
			Channel string   `json:"channel"`
			Event   string   `json:"event"`
			Payload []string `json:"payload"`
			Time    int64    `json:"time"`
		}
		if json.Unmarshal(b, &m) != nil || m.Time == 0 || m.Event != "subscribe" {
			t.Fatal(string(b))
		}
		if m.Channel == "futures.contract_stats" && (len(m.Payload) != 2 || m.Payload[1] != "1m") {
			t.Fatal(string(b))
		}
	}
}
func TestShardHeaderOnDial(t *testing.T) {
	shards, err := (&Adapter{}).Shards(context.Background(), []string{"ETH_USDT"})
	if err != nil {
		t.Fatal(err)
	}
	headers := make(chan string, len(shards))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Get("X-Gate-Size-Decimal")
		ws, err := websocket.Accept(w, r, nil)
		if err == nil {
			ws.CloseNow()
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, sh := range shards {
		ws, _, err := websocket.Dial(ctx, server.URL, &websocket.DialOptions{HTTPHeader: sh.Header})
		if err != nil {
			t.Fatal(err)
		}
		ws.CloseNow()
		if got := <-headers; got != "1" {
			t.Errorf("shard %s dial header = %q", sh.Name, got)
		}
	}
}
func TestFrames(t *testing.T) {
	a := &Adapter{}
	sh, _ := a.Shards(context.Background(), []string{"BTC_USDT", "ETH_USDT", "SOL_USDT", "PEPE_USDT", "龙虾_USDT", "AKE_USDT"})
	frames := venuetest.Fixture(t, "testdata/frames.jsonl")
	seen := map[string]int{}
	for _, f := range frames {
		for _, r := range a.Split(sh[0], f) {
			seen[r.Channel]++
			if r.Symbol == "" || !json.Valid(r.Payload) {
				t.Fatalf("bad record %+v", r)
			}
		}
	}
	for _, ch := range []string{"trades", "tickers", "liquidations"} {
		if seen[ch] == 0 {
			t.Errorf("missing %s: %v", ch, seen)
		}
	}
	if len(a.Split(sh[0], []byte(`{"channel":"futures.new","event":"update","result":[{"contract":"BTC_USDT"}]}`))) != 1 || a.Split(sh[0], []byte(`{"channel":"futures.new","event":"update","result":[]}`))[0].Channel != "gap" {
		t.Fatal("missing gap")
	}
	if len(a.Split(sh[0], []byte(`{"channel":"futures.pong","result":null}`))) != 0 {
		t.Fatal("pong")
	}
}
func TestSplitMultiAndConcurrent(t *testing.T) {
	a := &Adapter{}
	sh, _ := a.Shards(context.Background(), []string{"BTC_USDT", "龙虾_USDT"})
	frame := []byte(`{"channel":"futures.public_liquidates","event":"update","time":123,"result":[{"contract":"BTC_USDT","size":1},{"contract":"龙虾_USDT","size":2},{"contract":"OTHER_USDT","size":3}]}`)
	a.Split(sh[0], frame) // consume the hourly sample before concurrent checks
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r := a.Split(sh[0], frame)
				if len(r) != 2 || r[1].Symbol != "龙虾_USDT" || strings.Contains(string(r[0].Payload), "OTHER_USDT") {
					t.Error("split", r)
					return
				}
			}
		}()
	}
	wg.Wait()
}
func TestWebSocket(t *testing.T) {
	a := &Adapter{}
	sh, _ := a.Shards(context.Background(), []string{"BTC_USDT"})
	server := venuetest.NewServer(t, func(c *venuetest.Conn) {
		for i := 0; i < 3; i++ {
			select {
			case f := <-c.Frames:
				if !json.Valid(f.Payload) {
					t.Error("invalid client frame")
				}
			case <-time.After(5 * time.Second):
				t.Error("missing client frame")
				return
			}
		}
	})
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, _, e := websocket.Dial(ctx, server.URL, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer ws.CloseNow()
	for _, b := range a.Subscribe(sh[0]) {
		if e = ws.Write(ctx, websocket.MessageText, b); e != nil {
			t.Fatal(e)
		}
	}
	interval, ping := a.Heartbeat(venue.Shard{})
	typ, b := ping()
	if interval <= 0 || typ != int(websocket.MessageText) || !strings.Contains(string(b), "futures.ping") {
		t.Fatal(interval, string(b))
	}
	if e = ws.Write(ctx, websocket.MessageText, b); e != nil {
		t.Fatal(e)
	}
}

func TestClassesAndObject(t *testing.T) {
	a := &Adapter{}
	sh, _ := a.Shards(context.Background(), []string{"BTC_USDT"})
	for ch, want := range map[string]string{"trades": "trade", "tickers": "mark"} {
		if got, ok := a.Classes()(ch); !ok || got != want {
			t.Errorf("%s: %s %v", ch, got, ok)
		}
	}
	if _, ok := a.Classes()("liquidations"); ok {
		t.Fatal("liquidations must not be droppable")
	}
	msg := []byte(`{"channel":"futures.contract_info","event":"all","result":{"contract":"BTC_USDT","quanto_multiplier":"0.001"}}`)
	r := a.Split(sh[0], msg)
	if len(r) < 1 || r[0].Channel != "contract_info" || string(r[0].Payload) != string(msg) {
		t.Fatal(r)
	}
	msg = []byte(`{"channel":"futures.contract_stats","event":"update","result":[{"contract":"BTC_USDT","open_interest":123}]}`)
	r = a.Split(sh[0], msg)
	if len(r) < 1 || r[0].Channel != "contract_stats" {
		t.Fatal(r)
	}
}

func TestFilterAndHourlySample(t *testing.T) {
	frameSampler.Lock()
	oldNow, oldLast := frameSampler.now, frameSampler.last
	now := time.Unix(10000, 0)
	frameSampler.now = func() time.Time { return now }
	frameSampler.last = make(map[string]time.Time)
	frameSampler.Unlock()
	defer func() {
		frameSampler.Lock()
		frameSampler.now, frameSampler.last = oldNow, oldLast
		frameSampler.Unlock()
	}()
	a := &Adapter{}
	sh, _ := a.Shards(context.Background(), []string{"BTC_USDT"})
	msg := []byte(`{"time":123,"channel":"futures.tickers","event":"update","result":[{"contract":"BTC_USDT","last":"1","mark_price":"1.0000","mystery":1.2300},{"contract":"BTC_USDT","volume_24h":"5","index_price":"2.00"}]}`)
	r := a.Split(sh[0], msg)
	if len(r) != 3 || string(r[0].Payload) != `{"time":123,"channel":"futures.tickers","event":"update","result":[{"contract":"BTC_USDT","mark_price":"1.0000","mystery":1.2300}]}` || strings.Contains(string(r[1].Payload), "volume_24h") || string(r[2].Payload) != `{"sample":`+string(msg)+`}` || a.UnknownFields() != 1 {
		t.Fatalf("records=%+v unknown=%d", r, a.UnknownFields())
	}
	if len(a.Split(sh[0], msg)) != 2 {
		t.Fatal("sample repeated")
	}
	now = now.Add(time.Hour)
	if len(a.Split(sh[0], msg)) != 3 {
		t.Fatal("sample missing after hour")
	}
}
