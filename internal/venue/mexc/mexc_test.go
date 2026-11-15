package mexc

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
	b, err := os.ReadFile("testdata/detail.json")
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(b) }))
	defer s.Close()
	a := &Adapter{DetailURL: s.URL}
	syms, err := a.Symbols(context.Background())
	if err != nil || len(syms) != 4 {
		t.Fatalf("symbols %v: %v", syms, err)
	}
	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"success":true,"code":0,"data":[{"symbol":"中文_USDT","state":0,"futureType":1,"quoteCoin":"USDT","settleCoin":"USDT"},{"symbol":"BAD","state":4,"futureType":1,"quoteCoin":"USDT","settleCoin":"USDT"},{"symbol":"MISSING","futureType":1,"quoteCoin":"USDT","settleCoin":"USDT"}]}`)
	}))
	defer s2.Close()
	a.DetailURL = s2.URL
	syms, err = a.Symbols(context.Background())
	if err != nil || !reflect.DeepEqual(syms, []string{"中文_USDT"}) {
		t.Fatalf("symbols %v: %v", syms, err)
	}
	a.DetailURL = s2.URL + "/bad"
	a.Client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 429, Body: http.NoBody, Header: make(http.Header)}, nil
	})}
	if _, err = a.Symbols(context.Background()); err == nil || !strings.Contains(err.Error(), "429") {
		t.Fatalf("expected 429: %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestShardsSubscribeHeartbeat(t *testing.T) {
	a := &Adapter{}
	var syms []string
	for i := 0; i < 21; i++ {
		syms = append(syms, fmt.Sprintf("S%d_USDT", i))
	}
	shards, err := a.Shards(context.Background(), syms)
	if err != nil || len(shards) != 3 || len(shards[0].Streams) != 100 || len(shards[1].Streams) != 5 || shards[0].SubscribeSpacing != 150*time.Millisecond {
		t.Fatalf("shards %+v err %v", shards, err)
	}
	msgs := a.Subscribe(shards[0])
	if len(msgs) != 100 {
		t.Fatal(len(msgs))
	}
	var v struct {
		Method string         `json:"method"`
		Param  map[string]any `json:"param"`
	}
	if err := json.Unmarshal(msgs[0], &v); err != nil || v.Method != "sub.deal" || v.Param["symbol"] != syms[0] || v.Param["compress"] != false {
		t.Fatalf("subscription %s", msgs[0])
	}
	if got := string(a.Subscribe(shards[2])[0]); got != `{"method":"sub.contract"}` {
		t.Fatal(got)
	}
	interval, ping := a.Heartbeat(shards[0])
	typ, payload := ping()
	if interval != 15*time.Second || typ != int(websocket.MessageText) || string(payload) != `{"method":"ping"}` {
		t.Fatal(interval, typ, string(payload))
	}
	if venue.Get("mexc") == nil {
		t.Fatal("not registered")
	}
}

func TestFixtureSplit(t *testing.T) {
	a := &Adapter{}
	a.Shards(context.Background(), []string{"BTC_USDT", "ETH_USDT", "BARD_USDT", "PLAY_USDT"})
	counts := map[string]int{}
	for _, frame := range venuetest.Fixture(t, "testdata/frames.jsonl") {
		var v struct {
			Channel string `json:"channel"`
			Symbol  string `json:"symbol"`
		}
		if err := json.Unmarshal(frame, &v); err != nil {
			t.Fatal(err)
		}
		records := a.Split(venue.Shard{}, frame)
		if len(records) == 2 {
			records = records[:1]
		}
		if v.Channel == "push.contract.extra.update" || v.Channel == "push.contract.marketOrderSetting.update" {
			counts[v.Channel+":"+v.Symbol]++
			if v.Symbol != "BTC_USDT" || v.Channel == "push.contract.extra.update" {
				if len(records) != 0 {
					t.Fatalf("out-of-universe frame %s records %+v", frame, records)
				}
				continue
			}
			want := "contractextra"
			if v.Channel == "push.contract.marketOrderSetting.update" {
				want = "ordersetting"
			}
			if len(records) != 1 || records[0].Channel != want || records[0].Symbol != v.Symbol || string(records[0].Payload) != string(frame) {
				t.Fatalf("frame %s records %+v", frame, records)
			}
			continue
		}
		if v.Channel == "rs.error" {
			if len(records) != 1 || records[0].Channel != "gap" || records[0].Symbol != "" {
				t.Fatalf("error frame %s: %+v", frame, records)
			}
			continue
		}
		if strings.HasPrefix(v.Channel, "rs.") || v.Channel == "pong" {
			if len(records) != 0 {
				t.Fatal(v.Channel)
			}
			continue
		}
		if len(records) != 1 || records[0].Symbol != v.Symbol || false {
			t.Fatalf("frame %s records %+v", frame, records)
		}
		counts[records[0].Channel]++
	}
	for _, ch := range []string{"trade", "mark", "index", "ticker", "funding"} {
		if counts[ch] == 0 {
			t.Fatal(ch)
		}
	}
	if counts["push.contract.extra.update:BTC_USDT"] != 2 || counts["push.contract.marketOrderSetting.update:BTC_USDT"] != 1 || counts["push.contract.extra.update:BLKSTOCK_USDT"] != 1 || counts["push.contract.extra.update:HK0700_USDT"] != 1 || counts["push.contract.marketOrderSetting.update:HK0700_USDT"] != 1 {
		t.Fatalf("missing observed metadata fixtures: %v", counts)
	}
	unknown := a.Split(venue.Shard{}, []byte(`{"channel":"push.new","data":{"symbol":"BTC_USDT"}}`))
	if len(unknown) != 1 || unknown[0].Channel != "gap" {
		t.Fatal(unknown)
	}
	contract := []byte(`{"channel":"push.contract","symbol":"BTC_USDT","data":{"symbol":"BTC_USDT"}}`)
	if r := a.Split(venue.Shard{}, contract); len(r) < 1 || r[0].Channel != "contract" {
		t.Fatal(r)
	}
	if r := a.Split(venue.Shard{}, []byte(`{"channel":"push.contract","symbol":"OTHER","data":{}}`)); len(r) != 0 {
		t.Fatal(r)
	}
	unknownContract := []byte(`{"channel":"push.contract.future.update","symbol":"BTC_USDT","data":{"value":1}}`)
	if r := a.Split(venue.Shard{}, unknownContract); len(r) < 1 || r[0].Channel != "contractmeta" || r[0].Symbol != "BTC_USDT" || string(r[0].Payload) != string(unknownContract) {
		t.Fatal(r)
	}
	if r := a.Split(venue.Shard{}, []byte(strings.ReplaceAll(string(unknownContract), "BTC_USDT", "OTHER"))); len(r) != 0 {
		t.Fatal(r)
	}
	for _, ch := range []string{"contract", "contractextra", "ordersetting", "contractmeta"} {
		if class, ok := a.Classes()(ch); ok || class != "" {
			t.Fatalf("metadata class %s: %q %v", ch, class, ok)
		}
	}
	for ch, want := range map[string]string{"trade": "trade", "mark": "mark", "index": "mark"} {
		got, ok := a.Classes()(ch)
		if !ok || got != want {
			t.Fatal(ch, got, ok)
		}
	}
	if _, ok := a.Classes()("ticker"); ok {
		t.Fatal("ticker must not be dropped as quote")
	}
}

func TestMetadataUsesShardsSymbolsNotDiscoveredSymbols(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"success":true,"code":0,"data":[{"symbol":"BTC_USDT","state":0,"futureType":1,"quoteCoin":"USDT","settleCoin":"USDT"},{"symbol":"BLKSTOCK_USDT","state":0,"futureType":1,"quoteCoin":"USDT","settleCoin":"USDT"}]}`)
	}))
	defer server.Close()
	a := &Adapter{DetailURL: server.URL}
	syms, err := a.Symbols(context.Background())
	if err != nil || len(syms) != 2 {
		t.Fatalf("discovered %v: %v", syms, err)
	}
	if _, err := a.Shards(context.Background(), []string{"BTC_USDT"}); err != nil {
		t.Fatal(err)
	}
	for _, channel := range []string{"push.contract", "push.contract.marketOrderSetting.update", "push.contract.future.update"} {
		for _, tc := range []struct {
			symbol string
			want   bool
		}{{"BTC_USDT", true}, {"BLKSTOCK_USDT", false}} {
			frame := fmt.Sprintf(`{"channel":%q,"symbol":%q,"data":{}}`, channel, tc.symbol)
			got := a.Split(venue.Shard{Name: "metadata-test"}, []byte(frame))
			if (len(got) > 0) != tc.want {
				t.Fatalf("%s %s: %+v", channel, tc.symbol, got)
			}
		}
	}
	if _, err := a.Shards(context.Background(), []string{"BLKSTOCK_USDT"}); err != nil {
		t.Fatal(err)
	}
	if got := a.Split(venue.Shard{}, []byte(`{"channel":"push.contract","symbol":"BTC_USDT","data":{}}`)); len(got) != 0 {
		t.Fatal(got)
	}
}

func TestFilteringAndSamples(t *testing.T) {
	now := time.Unix(10000, 0)
	samples.Lock()
	old := samples.now
	samples.now = func() time.Time { return now }
	samples.last = make(map[string]time.Time)
	samples.Unlock()
	defer func() { samples.Lock(); samples.now = old; samples.last = make(map[string]time.Time); samples.Unlock() }()
	a := &Adapter{}
	frame := []byte(`{"symbol":"BTC_USDT","data":{"lastPrice":1,"holdVol":1.2300,"future":"x","bid1":2,"fairPrice":1e-8},"channel":"push.ticker","ts":123}`)
	sh := venue.Shard{Name: "test"}
	r := a.Split(sh, frame)
	want := `{"data":{"holdVol":1.2300,"future":"x","fairPrice":1e-8},"ts":123}`
	if len(r) != 2 || string(r[0].Payload) != want || string(r[1].Payload) != `{"sample":`+string(frame)+`}` || a.UnknownFields() != 1 {
		t.Fatalf("records %+v unknown %d", r, a.UnknownFields())
	}
	if r = a.Split(sh, frame); len(r) != 1 {
		t.Fatal(r)
	}
	now = now.Add(time.Hour)
	if r = a.Split(sh, frame); len(r) != 2 {
		t.Fatal(r)
	}
	if r = a.Split(venue.Shard{Name: "other"}, frame); len(r) != 2 {
		t.Fatal(r)
	}
}

func TestGapPayloadAndNotices(t *testing.T) {
	a := &Adapter{}
	for _, frame := range []string{`{"channel":"notice","data":null}`, `{"channel":"notice"}`} {
		if got := a.Split(venue.Shard{}, []byte(frame)); len(got) != 0 {
			t.Fatalf("notice %s: %+v", frame, got)
		}
	}
	for _, n := range []int{300, 4096, 5000} {
		frame := `{"channel":"push.unknown","data":"` + strings.Repeat("x", n) + `"}`
		got := a.Split(venue.Shard{}, []byte(frame))
		var p struct {
			Head  string `json:"head"`
			Bytes int    `json:"bytes"`
		}
		if len(got) != 1 || got[0].Channel != "gap" || json.Unmarshal(got[0].Payload, &p) != nil || p.Bytes != len(frame) || p.Head != frame[:min(len(frame), 4096)] {
			t.Fatalf("length %d: %+v", n, got)
		}
	}
}

func TestSubscribeResponses(t *testing.T) {
	a := &Adapter{}
	for _, frame := range []string{`{"channel":"rs.sub.deal","data":"success"}`, `{"channel":"rs.sub.contract","data":"success"}`} {
		if got := a.Split(venue.Shard{}, []byte(frame)); len(got) != 0 {
			t.Fatal(got)
		}
	}
	for _, frame := range []string{`{"channel":"rs.error","data":"Contract [X_USDT] not exists"}`, `{"channel":"rs.sub.deal","data":"failed"}`, `{"channel":"rs.sub.deal","data":{"success":false}}`, `{"channel":"rs.sub.deal","data":{"code":1001}}`, `{"channel":"rs.error","data":"` + strings.Repeat("x", 5000) + `"}`} {
		got := a.Split(venue.Shard{}, []byte(frame))
		var p struct {
			Event string `json:"event"`
			Frame string `json:"frame"`
		}
		if len(got) != 1 || got[0].Channel != "gap" || got[0].Symbol != "" || json.Unmarshal(got[0].Payload, &p) != nil || p.Event != "subscribe_error" || p.Frame != frame[:min(len(frame), 4096)] {
			t.Fatalf("response: %+v", got)
		}
	}
	if a.SubscribeAcks() != 2 || a.SubscribeErrors() != 5 {
		t.Fatalf("acks %d errors %d", a.SubscribeAcks(), a.SubscribeErrors())
	}
}

func TestConcurrentSplit(t *testing.T) {
	a := &Adapter{}
	a.Shards(context.Background(), []string{"BTC_USDT"})
	frames := venuetest.Fixture(t, "testdata/frames.jsonl")
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				for _, f := range frames {
					a.Split(venue.Shard{}, f)
				}
			}
		}()
	}
	wg.Wait()
}

func TestWebSocketProtocol(t *testing.T) {
	frames := venuetest.Fixture(t, "testdata/frames.jsonl")
	server := venuetest.NewServer(t, func(c *venuetest.Conn) {
		for range 5 {
			select {
			case <-c.Frames:
			case <-time.After(time.Second):
				return
			}
		}
		for _, f := range frames {
			if c.Send(websocket.MessageText, f) != nil {
				return
			}
		}
		select {
		case <-c.Frames:
		case <-time.After(time.Second):
		}
	})
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	a := &Adapter{}
	sh, _ := a.Shards(ctx, []string{"BTC_USDT"})
	for _, msg := range a.Subscribe(sh[0]) {
		if err := c.Write(ctx, websocket.MessageText, msg); err != nil {
			t.Fatal(err)
		}
	}
	for range frames {
		_, b, err := c.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		a.Split(sh[0], b)
	}
	_, f := a.Heartbeat(sh[0])
	_, p := f()
	if err := c.Write(ctx, websocket.MessageText, p); err != nil {
		t.Fatal(err)
	}
}
