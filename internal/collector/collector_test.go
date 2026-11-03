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

	"github.com/wicker/wicker/internal/rawlog"
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
	c := New(nopSink{}, []Shard{{Name: "depth", Base: base, Streams: []string{"btcusdt@depth@100ms"}}})
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
	if len(got) != 1 || got[0] != "depth" {
		t.Fatalf("OnReconnect calls = %v, want exactly one for depth", got)
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
	c := New(nopSink{}, []Shard{{Name: "trade00", Base: base, Streams: []string{"btcusdt@aggTrade"}}})
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
