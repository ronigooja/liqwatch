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
	sink        rawlog.Sink
	shards      []Shard
	adapter     venue.Adapter
	throttle    *throttler
	shardCounts map[string]*shardCounters
	classifier  venue.FrameClassifier
	// subscribeWrite overrides subscription writes for deterministic connection tests.
	subscribeWrite func(context.Context, *websocket.Conn, []byte) error
	// clock overrides lifecycle timestamps in deterministic tests; not used per message.
	clock func() (time.Time, int64)

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
	c := &Collector{sink: sink, adapter: adapter, shards: shards, OpenSpacing: openSpacing}
	c.classifier, _ = adapter.(venue.FrameClassifier)
	c.shardCounts = newShardCounters(shards, c.classifier)
	c.ConfigureThrottle(true, nil)
	return c
}

// Run starts all shards (gradually) and blocks until ctx is cancelled and all connections are closed.
func (c *Collector) Run(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case now := <-t.C:
				c.throttle.flush(now, "", false)
			case <-done:
				return
			}
		}
	}()
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
	close(done)
	c.throttle.flush(time.Time{}, "", true)
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
	var previous *conn
	var slept time.Duration
	for ctx.Err() == nil {
		gen++
		if gen > 1 {
			url = refreshURL(ctx, sh, url)
		}
		cur = c.startRetry(ctx, sh, url, gen, previous, slept)
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
				previous = cur
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
		slept = backoff
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
	// Reader-owned lifecycle stats are published by closing done; runShard waits for done.
	// up is initialized before subscription workers start. previous/backoff/gen are immutable.
	up, closed, last          time.Time
	upMon, closedMon, lastMon int64
	msgs                      uint64
	previous                  *conn
	backoff                   time.Duration
	gen                       int
}

func (k *conn) stop() { k.cancel() }
func (k *conn) err() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.e
}

func (c *Collector) start(parent context.Context, sh Shard, url string, gen int) *conn {
	return c.startRetry(parent, sh, url, gen, nil, 0)
}

func (c *Collector) startRetry(parent context.Context, sh Shard, url string, gen int, previous *conn, backoff time.Duration) *conn {
	ctx, cancel := context.WithCancel(parent)
	k := &conn{
		id:     fmt.Sprintf("%s-%s-%d", sh.Name, time.Now().UTC().Format("0102T150405"), gen),
		cancel: cancel, opened: make(chan struct{}), done: make(chan struct{}),
		gen:      gen,
		previous: previous, backoff: backoff,
	}
	if previous != nil {
		// Retain only the boundary, not the preceding connection chain.
		k.previous = &conn{id: previous.id, closedMon: previous.closedMon}
	}
	go func() {
		defer close(k.done)
		defer cancel()
		err := c.read(ctx, k, sh, url)
		k.closed, k.closedMon = c.now()
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
	defer c.throttle.flush(time.Time{}, k.id, false)
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: sh.Header})
	if err != nil {
		return err
	}
	defer ws.CloseNow()
	k.up, k.upMon = c.now()
	ws.SetReadLimit(16 << 20)
	if lifecycle, ok := c.adapter.(venue.ConnLifecycle); ok {
		lifecycle.ConnOpened(sh, k.id)
		defer lifecycle.ConnClosed(sh, k.id)
	}
	close(k.opened)
	if c.OnOpen != nil {
		c.OnOpen(sh)
	}
	if c.OnClose != nil {
		defer c.OnClose(sh)
	}
	log.Printf("shard %s conn %s open (%d streams)", sh.Name, k.id, len(sh.Streams))

	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	var last atomic.Int64
	writeErr := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		for i, msg := range c.adapter.Subscribe(sh) {
			if i > 0 && sh.SubscribeSpacing > 0 {
				t := time.NewTimer(sh.SubscribeSpacing)
				select {
				case <-ctx.Done():
					t.Stop()
					return
				case <-t.C:
				}
			}
			write := c.subscribeWrite
			if write == nil {
				write = func(ctx context.Context, ws *websocket.Conn, msg []byte) error {
					return ws.Write(ctx, websocket.MessageText, msg)
				}
			}
			if err := write(ctx, ws, msg); err != nil {
				writeErr <- err
				cancel()
				return
			}
		}
		// Start the stall clock on the first frame or the last subscription, whichever comes first.
		last.CompareAndSwap(0, time.Now().UnixNano())
		c.openGap(sh, k)
	}()
	if every, frame := c.adapter.Heartbeat(sh); every > 0 && frame != nil {
		workers.Add(1)
		go func() {
			defer workers.Done()
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

	if sh.Stall > 0 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			t := time.NewTicker(sh.Stall / 4)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if seen := last.Load(); seen != 0 && time.Since(time.Unix(0, seen)) > sh.Stall {
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
	counts := c.shardCounts[sh.Name]
	var received time.Time
	defer func() {
		k.msgs = cseq
		if !received.IsZero() {
			k.last, k.lastMon = received, k.upMon+received.Sub(k.up).Nanoseconds()
		}
	}()
	for {
		_, data, err := ws.Read(ctx)
		rw := time.Now()
		if err != nil {
			select {
			case err := <-writeErr:
				return err
			default:
			}
			if ctx.Err() != nil {
				return fmt.Errorf("stopped")
			}
			return err
		}
		last.Store(rw.UnixNano())
		cseq++
		received = rw
		if cseq == 1 {
			c.writeGap(sh, k, rw, k.upMon+rw.Sub(k.up).Nanoseconds(), map[string]any{"event": "conn_first", "shard": sh.Name, "conn": k.id})
		}
		c.Messages.Add(1)
		if counts != nil {
			channel := "unclassified"
			if c.classifier != nil {
				channel = c.classifier.FrameChannel(data)
			}
			counts.channel(channel).frames.Add(1)
		}
		var records []rawlog.Record
		if splitter, ok := c.adapter.(venue.ConnSplitter); ok {
			records = splitter.SplitConn(sh, k.id, data)
		} else {
			records = c.adapter.Split(sh, data)
		}
		if counts != nil {
			// Most frames yield one record or a run of records on the same channel.
			for i := 0; i < len(records); {
				j := i + 1
				for j < len(records) && records[j].Channel == records[i].Channel {
					j++
				}
				counts.channel(records[i].Channel).records.Add(uint64(j - i))
				i = j
			}
		}
		for _, rec := range records {
			rec.Venue, rec.Conn, rec.CSeq, rec.RecvWal, rec.RecvMon = c.adapter.Name(), k.id, cseq, rw.UnixNano(), rawlog.Mono()
			// The writer routes by venue, symbol and channel; only GroupGap changes its handling.
			c.throttle.write(rec)
		}
	}
}

func (c *Collector) gap(sh Shard, k *conn, cause string) {
	e := ""
	if err := k.err(); err != nil {
		e = err.Error()
	}
	uptime := int64(0)
	if !k.up.IsZero() {
		uptime = (k.closedMon - k.upMon) / int64(time.Millisecond)
	}
	lastWall := int64(0)
	if !k.last.IsZero() {
		lastWall = k.last.UnixNano()
	}
	var previous any
	if k.previous != nil {
		previous = k.previous.id
	}
	c.writeGap(sh, k, k.closed, k.closedMon, map[string]any{"event": "conn_closed", "cause": cause, "err": e, "shard": sh.Name, "streams": sh.Streams,
		"conn": k.id, "prev": previous, "last_rw": lastWall, "last_rm": k.lastMon, "up_ms": uptime, "msgs": k.msgs})
}

func (c *Collector) now() (time.Time, int64) {
	if c.clock != nil {
		return c.clock()
	}
	return time.Now(), rawlog.Mono()
}

func (c *Collector) openGap(sh Shard, k *conn) {
	now, mono := c.now()
	var down any
	var previous any
	if k.previous != nil {
		down = (mono - k.previous.closedMon) / int64(time.Millisecond)
		previous = k.previous.id
	}
	c.writeGap(sh, k, now, mono, map[string]any{"event": "conn_opened", "shard": sh.Name, "conn": k.id,
		"prev": previous, "initial": k.gen == 1, "attempt": k.gen, "backoff_ms": k.backoff.Milliseconds(), "down_ms": down,
		"subscribed": len(sh.Streams), "subs_ok": true})
}

func (c *Collector) writeGap(sh Shard, k *conn, now time.Time, mono int64, payload map[string]any) {
	p, _ := json.Marshal(payload)
	c.sink.Write(rawlog.GroupGap, rawlog.Record{
		Venue: c.adapter.Name(), Conn: k.id, RecvWal: now.UnixNano(), RecvMon: mono, Channel: "gap", Payload: p,
	})
}
