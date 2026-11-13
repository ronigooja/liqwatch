package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ronigooja/liqwatch/internal/venue"
	"github.com/ronigooja/liqwatch/internal/venue/venuetest"
)

func fixtureAdapter(t *testing.T) (*Adapter, []venue.Shard) {
	t.Helper()
	data, err := os.ReadFile("testdata/instruments.json")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(data) }))
	t.Cleanup(server.Close)
	a := &Adapter{InstrumentsURL: server.URL}
	syms, err := a.Symbols(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(syms) != 9 {
		t.Fatalf("symbols: %v", syms)
	}
	shards, err := a.Shards(context.Background(), syms)
	if err != nil {
		t.Fatal(err)
	}
	return a, shards
}
func TestFixtures(t *testing.T) {
	a, shards := fixtureAdapter(t)
	seen := map[string]bool{}
	for _, path := range []string{"testdata/public.jsonl", "testdata/business.jsonl"} {
		for _, frame := range venuetest.Fixture(t, path) {
			var control struct {
				Event string `json:"event"`
			}
			_ = json.Unmarshal(frame, &control)
			for _, r := range a.Split(shards[0], frame) {
				if control.Event == "notice" && r.Channel == "gap" {
					continue
				}
				if r.Channel == "gap" {
					t.Fatalf("unexpected gap: %s", frame)
				}
				seen[r.Channel] = true
				if !json.Valid(r.Payload) {
					t.Fatal("invalid payload")
				}
				if r.Channel == "index" && !strings.HasSuffix(r.Symbol, "-USDT-SWAP") {
					t.Fatalf("index symbol: %s", r.Symbol)
				}
			}
		}
	}
	for _, c := range []string{"trade", "mark", "index", "funding", "oi", "liquidation"} {
		if !seen[c] {
			t.Errorf("missing %s", c)
		}
	}
	if a.Split(shards[0], []byte("pong")) != nil {
		t.Fatal("pong")
	}
	if r := a.Split(shards[0], []byte(`{"arg":{"channel":"new"},"data":[{}]}`)); len(r) != 1 || r[0].Channel != "gap" {
		t.Fatalf("unknown: %+v", r)
	}
	if _, ok := a.Classes()("liquidation"); ok {
		t.Fatal("liquidations droppable")
	}
}

func TestSubscriptionControls(t *testing.T) {
	a := &Adapter{}
	sh := venue.Shard{}
	frames := []string{
		`{"event":"error","code":"60018","msg":"bad subscription","arg":{"channel":"mark-price","instId":"BAD"}}`,
		`{"event":"channel-conn-count-error","channel":"orders","connCount":"30","connId":"abc"}`,
		`{"event":"notice","msg":"maintenance","code":"64010","connId":"abc"}`,
	}
	for _, frame := range frames {
		r := a.Split(sh, []byte(frame))
		if len(r) != 1 || r[0].Channel != "gap" || r[0].Symbol != "" {
			t.Fatalf("record: %+v", r)
		}
		var payload struct {
			Event string          `json:"event"`
			Frame json.RawMessage `json:"frame"`
		}
		if err := json.Unmarshal(r[0].Payload, &payload); err != nil || payload.Event != "subscribe_error" || string(payload.Frame) != frame {
			t.Fatalf("payload: %s", r[0].Payload)
		}
	}
	const workers, iterations = 20, 100
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if a.Split(sh, []byte(`{"event":"subscribe","arg":{"channel":"mark-price"}}`)) != nil {
					t.Error("ack recorded")
				}
				a.Split(sh, []byte(frames[0]))
				if a.Split(sh, []byte("pong")) != nil {
					t.Error("pong recorded")
				}
				_ = a.SubscribeAcks()
				_ = a.SubscribeErrors()
			}
		}()
	}
	wg.Wait()
	if a.SubscribeAcks() != workers*iterations || a.SubscribeErrors() != workers*iterations+uint64(len(frames)) {
		t.Fatalf("counts %d %d", a.SubscribeAcks(), a.SubscribeErrors())
	}
	if (&Adapter{}).SubscribeErrors() != 0 {
		t.Fatal("counters shared between adapters")
	}
}

func TestFilterAndSampling(t *testing.T) {
	a, _ := fixtureAdapter(t)
	now := time.Unix(10000, 0)
	old := sampleClock
	sampleClock = func() time.Time { return now }
	t.Cleanup(func() { sampleClock = old })
	sh := venue.Shard{Name: "filter-test"}
	frame := []byte(`{"arg":{"channel":"index-tickers","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","idxPx":"1.2300","open24h":"4","future":"\u0031","ts":"123"},{"instId":"BTC-USDT","idxPx":"2","ts":"124"}]}`)
	before := a.UnknownFields()
	r := a.Split(sh, frame)
	if len(r) != 3 || a.UnknownFields() != before+1 {
		t.Fatalf("records/count: %+v %d", r, a.UnknownFields()-before)
	}
	if string(r[0].Payload) != `{"arg":{"channel":"index-tickers","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","idxPx":"1.2300","future":"\u0031","ts":"123"}]}` {
		t.Fatalf("filtered: %s", r[0].Payload)
	}
	if string(r[1].Payload) != `{"sample":`+string(frame)+`}` {
		t.Fatalf("sample: %s", r[1].Payload)
	}
	if got := a.Split(sh, frame); len(got) != 2 {
		t.Fatalf("duplicate sample: %d", len(got))
	}
	now = now.Add(time.Hour)
	if got := a.Split(sh, frame); len(got) != 3 {
		t.Fatalf("missing hourly sample: %d", len(got))
	}
	if got := a.Split(venue.Shard{Name: "filter-test-other"}, frame); len(got) != 3 {
		t.Fatalf("missing shard sample: %d", len(got))
	}
}
func TestShardsSubscribeAndHeartbeat(t *testing.T) {
	a, shards := fixtureAdapter(t)
	if len(shards) != 2 || shards[0].URL != PublicURL || shards[1].URL != BusinessURL {
		t.Fatalf("shards: %+v", shards)
	}
	for _, sh := range shards {
		frames := a.Subscribe(sh)
		if len(frames) != 1 || len(frames[0]) >= 64*1024 {
			t.Fatal("invalid subscription")
		}
		var req struct {
			Op   string `json:"op"`
			Args []arg  `json:"args"`
		}
		if err := json.Unmarshal(frames[0], &req); err != nil || req.Op != "subscribe" || len(req.Args) != len(sh.Streams) {
			t.Fatal("bad request", err)
		}
		d, f := a.Heartbeat(sh)
		typ, p := f()
		if d >= 30*time.Second || typ != int(websocket.MessageText) || string(p) != "ping" {
			t.Fatal("heartbeat")
		}
	}
	server := venuetest.NewServer(t, func(c *venuetest.Conn) {
		frame := <-c.Frames
		if err := c.Send(websocket.MessageText, frame.Payload); err != nil {
			return
		}
	})
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	want := a.Subscribe(shards[0])[0]
	if err = ws.Write(ctx, websocket.MessageText, want); err != nil {
		t.Fatal(err)
	}
	_, got, err := ws.Read(ctx)
	if err != nil || string(got) != string(want) {
		t.Fatalf("server echo %s %v", got, err)
	}
}
func TestConcurrentSplit(t *testing.T) {
	a, shards := fixtureAdapter(t)
	frames := venuetest.Fixture(t, "testdata/public.jsonl")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				for _, f := range frames {
					a.Split(shards[0], f)
				}
			}
		}()
	}
	wg.Wait()
}
func TestDiscoveryErrors(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "blocked", 403) }))
	defer s.Close()
	a := &Adapter{InstrumentsURL: s.URL, HTTPClient: s.Client()}
	if _, err := a.Symbols(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	a.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"code":"1","msg":"bad"}`)), Header: make(http.Header)}, nil
	})}
	if _, err := a.Symbols(context.Background()); err == nil {
		t.Fatal("expected API error")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestShardLimit(t *testing.T) {
	a := &Adapter{indexes: map[string]string{}}
	var syms []string
	for i := 0; i < 525; i++ {
		s := fmt.Sprintf("X%d-USDT-SWAP", i)
		syms = append(syms, s)
		a.indexes[s] = fmt.Sprintf("X%d-USDT", i)
	}
	shards, err := a.Shards(context.Background(), syms)
	if err != nil {
		t.Fatal(err)
	}
	for _, sh := range shards {
		if len(sh.Streams) > maxArgs {
			t.Fatal("too many subscriptions")
		}
		frames := a.Subscribe(sh)
		if len(frames) != 1 || len(frames[0]) >= 64*1024 {
			t.Fatal("request limit")
		}
	}
}
