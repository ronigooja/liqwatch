// Package collector reads venue WebSocket streams and hands records to a rawlog.Sink.
// Design: docs/p0-core-design.md (Component 2).
package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

const (
	connMaxAge  = 23 * time.Hour // replace before the 24 h server limit
	overlap     = 10 * time.Second
	openSpacing = 500 * time.Millisecond
)

// Shard describes a venue connection.
type Shard = venue.Shard

type Collector struct {
	sink    rawlog.Sink
	shards  []Shard
	adapter venue.Adapter

	// OnReconnect is called after a shard reopens following an unplanned disconnect, once the
	// new connection is open. Planned 23 h replacements overlap and do not call it.
	OnReconnect func(Shard)
	// OnDisconnect is called right after an unplanned disconnect, before the shard reopens. The
	// returned token is passed to OnReconnectToken once the new connection is open.
	OnDisconnect func(Shard) uint64
	// OnReconnectToken replaces OnReconnect when set and receives the OnDisconnect token.
	OnReconnectToken func(Shard, uint64)
	// OpenSpacing is the delay between starting consecutive shards; default 500 ms.
	OpenSpacing time.Duration
	// OnOpen is called every time a connection of a shard opens (first, reconnect or replacement).
	OnOpen func(Shard)
	// OnClose is called when an opened connection closes.
	OnClose func(Shard)

	Reconnects atomic.Int64
	Stalls     atomic.Int64
	Messages   atomic.Int64
}

func New(sink rawlog.Sink, adapter venue.Adapter, shards []Shard) *Collector {
	return &Collector{sink: sink, adapter: adapter, shards: shards, OpenSpacing: openSpacing}
}

// Run starts all shards (gradually) and blocks until ctx is cancelled and all connections are closed.
func (c *Collector) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i, sh := range c.shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(i) * c.OpenSpacing):
			}
			c.runShard(ctx, sh)
		}()
	}
	wg.Wait()
}

func (c *Collector) runShard(ctx context.Context, sh Shard) {
	url := sh.URL
	maxAge := sh.MaxAge
	if maxAge <= 0 {
		maxAge = connMaxAge
	}
	backoff := time.Second
	var gen int
	var cur *conn
	var reconnected bool
	var token uint64
	for ctx.Err() == nil {
		gen++
		if gen > 1 {
			url = refreshURL(ctx, sh, url)
		}
		cur = c.start(ctx, sh, url, gen)
		started := time.Now()
		if reconnected && (c.OnReconnect != nil || c.OnReconnectToken != nil) {
			go func(k *conn, tok uint64) {
				select {
				case <-k.opened:
					if c.OnReconnect != nil {
						c.OnReconnect(sh)
					}
					if c.OnReconnectToken != nil {
						c.OnReconnectToken(sh, tok)
					}
				case <-k.done:
				case <-ctx.Done():
				}
			}(cur, token)
		}
		for {
			select {
			case <-ctx.Done():
				cur.stop()
				<-cur.done
				return
			case <-cur.done:
				c.Reconnects.Add(1)
				log.Printf("shard %s conn %s ended after %s: %v", sh.Name, cur.id, time.Since(started).Round(time.Second), cur.err())
				c.gap(sh, cur, "reconnect")
				goto retry
			case <-time.After(time.Until(started.Add(maxAge))):
				// planned replacement: new connection first, then retire the old one
				url = refreshURL(ctx, sh, url)
				next := c.start(ctx, sh, url, gen+1)
				select {
				case <-next.opened:
				case <-next.done:
				case <-ctx.Done():
				}
				time.Sleep(overlap)
				cur.stop()
				<-cur.done
				gen++
				cur, started = next, time.Now()
				log.Printf("shard %s replaced connection, now %s", sh.Name, cur.id)
			}
		}
	retry:
		reconnected = true
		token = 0
		if c.OnDisconnect != nil {
			token = c.OnDisconnect(sh)
		}
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func refreshURL(ctx context.Context, sh Shard, oldURL string) string {
	if sh.Refresh == nil {
		return oldURL
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	url, err := sh.Refresh(ctx)
	if err != nil || url == "" {
		log.Printf("shard %s URL refresh failed: %v", sh.Name, err)
		return oldURL
	}
	return url
}

type conn struct {
	id     string
	cancel context.CancelFunc
	opened chan struct{}
	done   chan struct{}
	mu     sync.Mutex
	e      error
}

func (k *conn) stop() { k.cancel() }
func (k *conn) err() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.e
}

func (c *Collector) start(parent context.Context, sh Shard, url string, gen int) *conn {
	ctx, cancel := context.WithCancel(parent)
	k := &conn{
		id:     fmt.Sprintf("%s-%s-%d", sh.Name, time.Now().UTC().Format("0102T150405"), gen),
		cancel: cancel, opened: make(chan struct{}), done: make(chan struct{}),
	}
	go func() {
		defer close(k.done)
		defer cancel()
		err := c.read(ctx, k, sh, url)
		if parent.Err() == nil && ctx.Err() != nil && err != nil && err.Error() == "stopped" {
			err = nil
		}
		k.mu.Lock()
		k.e = err
		k.mu.Unlock()
	}()
	return k
}

func (c *Collector) read(ctx context.Context, k *conn, sh Shard, url string) error {
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: sh.Header})
	if err != nil {
		return err
	}
	defer ws.CloseNow()
	ws.SetReadLimit(16 << 20)
	close(k.opened)
	if c.OnOpen != nil {
		c.OnOpen(sh)
	}
	if c.OnClose != nil {
		defer c.OnClose(sh)
	}
	log.Printf("shard %s conn %s open (%d streams)", sh.Name, k.id, len(sh.Streams))

	for i, msg := range c.adapter.Subscribe(sh) {
		if i > 0 && sh.SubscribeSpacing > 0 {
			t := time.NewTimer(sh.SubscribeSpacing)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
		}
		if err := ws.Write(ctx, websocket.MessageText, msg); err != nil {
			return err
		}
	}
	if every, frame := c.adapter.Heartbeat(sh); every > 0 && frame != nil {
		go func() {
			t := time.NewTicker(every)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					typ, payload := frame()
					if ws.Write(ctx, websocket.MessageType(typ), payload) != nil {
						k.cancel()
						return
					}
				}
			}
		}()
	}

	var last atomic.Int64
	last.Store(time.Now().UnixNano())
	if sh.Stall > 0 {
		wctx, wcancel := context.WithCancel(ctx)
		defer wcancel()
		go func() {
			t := time.NewTicker(sh.Stall / 4)
			defer t.Stop()
			for {
				select {
				case <-wctx.Done():
					return
				case <-t.C:
					if time.Since(time.Unix(0, last.Load())) > sh.Stall {
						c.Stalls.Add(1)
						log.Printf("shard %s conn %s stalled, replacing", sh.Name, k.id)
						k.cancel()
						return
					}
				}
			}
		}()
	}
	var cseq uint64
	for {
		_, data, err := ws.Read(ctx)
		rw := time.Now()
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("stopped")
			}
			return err
		}
		last.Store(rw.UnixNano())
		cseq++
		c.Messages.Add(1)
		for _, rec := range c.adapter.Split(sh, data) {
			rec.Venue, rec.Conn, rec.CSeq, rec.RecvWal, rec.RecvMon = c.adapter.Name(), k.id, cseq, rw.UnixNano(), rawlog.Mono()
			// The writer routes by venue, symbol and channel; only GroupGap changes its handling.
			c.sink.Write(rawlog.GroupTrade, rec)
		}
	}
}

func (c *Collector) gap(sh Shard, k *conn, cause string) {
	e := ""
	if err := k.err(); err != nil {
		e = err.Error()
	}
	p, _ := json.Marshal(map[string]any{"event": "conn_closed", "cause": cause, "err": e, "shard": sh.Name, "streams": sh.Streams})
	now := time.Now()
	c.sink.Write(rawlog.GroupGap, rawlog.Record{
		Venue: c.adapter.Name(), Conn: k.id, RecvWal: now.UnixNano(), RecvMon: rawlog.Mono(), Channel: "gap", Payload: p,
	})
}
