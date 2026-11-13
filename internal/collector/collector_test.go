package collector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

type nopSink struct{}

func (nopSink) Write(string, rawlog.Record) {}

func TestOnReconnectOnlyAfterUnplannedDrop(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		if conns.Add(1) == 1 {
			ws.Close(websocket.StatusGoingAway, "drop")
			return
		}
		defer ws.CloseNow()
		<-r.Context().Done()
	}))
	defer srv.Close()

	base := "ws" + strings.TrimPrefix(srv.URL, "http") + "/?streams="
	c := New(nopSink{}, fakeAdapter{}, []Shard{{Name: "trade00", URL: base + "btcusdt@aggTrade", Streams: []string{"btcusdt@aggTrade"}}})
	var mu sync.Mutex
	var got []string
	called := make(chan struct{}, 4)
	c.OnReconnect = func(sh Shard) {
		mu.Lock()
		got = append(got, sh.Name)
		mu.Unlock()
		called <- struct{}{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()

	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("OnReconnect not called after the first connection dropped")
	}
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != "trade00" {
		t.Fatalf("OnReconnect calls = %v, want exactly one for trade00", got)
	}
	if conns.Load() < 2 {
		t.Fatalf("server saw %d connections", conns.Load())
	}
}

func TestTokenFlowOnDisconnect(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		if conns.Add(1) == 1 {
			ws.Close(websocket.StatusGoingAway, "drop")
			return
		}
		defer ws.CloseNow()
		<-r.Context().Done()
	}))
	defer srv.Close()

	base := "ws" + strings.TrimPrefix(srv.URL, "http") + "/?streams="
	c := New(nopSink{}, fakeAdapter{}, []Shard{{Name: "trade00", URL: base + "btcusdt@aggTrade", Streams: []string{"btcusdt@aggTrade"}}})
	var disc atomic.Int32
	got := make(chan uint64, 2)
	c.OnDisconnect = func(Shard) uint64 { disc.Add(1); return 42 }
	c.OnReconnectToken = func(_ Shard, tok uint64) { got <- tok }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	select {
	case tok := <-got:
		if tok != 42 {
			t.Fatalf("token = %d, want the value returned by OnDisconnect", tok)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("OnReconnectToken not called")
	}
	cancel()
	<-done
	if disc.Load() != 1 {
		t.Fatalf("OnDisconnect called %d times", disc.Load())
	}
}

type fakeAdapter struct{}

func (fakeAdapter) Name() string                                                  { return "fake" }
func (fakeAdapter) Symbols(context.Context) ([]venue.Symbol, error)               { return nil, nil }
func (fakeAdapter) Shards(context.Context, []venue.Symbol) ([]venue.Shard, error) { return nil, nil }
func (fakeAdapter) Subscribe(venue.Shard) [][]byte                                { return nil }
func (fakeAdapter) Heartbeat(venue.Shard) (time.Duration, func() (int, []byte))   { return 0, nil }
func (fakeAdapter) Split(venue.Shard, []byte) []rawlog.Record                     { return nil }
func (fakeAdapter) Classes() func(string) (string, bool)                          { return nil }

type activeAdapter struct{ fakeAdapter }

func (activeAdapter) Subscribe(venue.Shard) [][]byte { return [][]byte{[]byte("one"), []byte("two")} }
func (activeAdapter) Heartbeat(venue.Shard) (time.Duration, func() (int, []byte)) {
	return 30 * time.Millisecond, func() (int, []byte) { return int(websocket.MessageText), []byte("ping") }
}
func (activeAdapter) Split(_ venue.Shard, msg []byte) []rawlog.Record {
	if string(msg) != "data" {
		return nil
	}
	return []rawlog.Record{{Channel: "trade", Symbol: "BTC", Payload: msg}}
}

type captureSink struct {
	mu      sync.Mutex
	records []rawlog.Record
}

func (s *captureSink) Write(_ string, r rawlog.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, r)
}
func TestSubscribeHeartbeatAndControlLiveness(t *testing.T) {
	type received struct {
		msg string
		at  time.Time
	}
	frames := make(chan received, 50)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		ctx := r.Context()
		go func() {
			ticker := time.NewTicker(25 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if ws.Write(ctx, websocket.MessageText, []byte("control")) != nil {
						return
					}
				}
			}
		}()
		for {
			_, msg, err := ws.Read(ctx)
			if err != nil {
				return
			}
			frames <- received{string(msg), time.Now()}
		}
	}))
	defer srv.Close()
	sink := &captureSink{}
	c := New(sink, activeAdapter{}, []Shard{{Name: "active", URL: "ws" + strings.TrimPrefix(srv.URL, "http"), Stall: 100 * time.Millisecond, SubscribeSpacing: 60 * time.Millisecond}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	var first, second received
	select {
	case first = <-frames:
	case <-time.After(time.Second):
		t.Fatal("missing first subscription")
	}
	select {
	case second = <-frames:
	case <-time.After(time.Second):
		t.Fatal("missing second subscription")
	}
	if first.msg != "one" || second.msg != "two" || second.at.Sub(first.at) < 50*time.Millisecond {
		t.Fatalf("subscriptions: %+v %+v", first, second)
	}
	select {
	case f := <-frames:
		if f.msg != "ping" {
			t.Fatalf("heartbeat: %+v", f)
		}
	case <-time.After(time.Second):
		t.Fatal("missing heartbeat")
	}
	time.Sleep(250 * time.Millisecond)
	if c.Stalls.Load() != 0 || c.Reconnects.Load() != 0 {
		t.Fatalf("control liveness: stalls=%d reconnects=%d", c.Stalls.Load(), c.Reconnects.Load())
	}
	cancel()
	<-done
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.records) != 0 {
		t.Fatalf("control frames recorded: %+v", sink.records)
	}
}
