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
