// Package collector reads Binance futures combined streams and hands raw bytes to a rawlog.Sink.
// Design: docs/p0-core-design.md (Component 2).
package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/wicker/wicker/internal/rawlog"
)

const (
	WSPublic = "wss://fstream.binance.com/public/stream?streams="
	WSMarket = "wss://fstream.binance.com/market/stream?streams="

	connMaxAge  = 23 * time.Hour // replace before the 24 h server limit
	overlap     = 10 * time.Second
	openSpacing = 500 * time.Millisecond
)

// Shard is one combined-stream connection (replaced over time).
type Shard struct {
	Name    string
	Base    string
	Streams []string
	Stall   time.Duration // 0 disables stall detection
}

type Collector struct {
	sink   rawlog.Sink
	shards []Shard

	// OnReconnect is called after a shard reopens following an unplanned disconnect, once the
	// new connection is open. Planned 23 h replacements overlap and do not call it.
	OnReconnect func(Shard)
	// OpenSpacing is the delay between starting consecutive shards; default 500 ms.
	OpenSpacing time.Duration
	// OnOpen is called every time a connection of a shard opens (first, reconnect or replacement).
	OnOpen func(Shard)

	Reconnects atomic.Int64
	Stalls     atomic.Int64
	Messages   atomic.Int64
}

func New(sink rawlog.Sink, shards []Shard) *Collector {
	return &Collector{sink: sink, shards: shards, OpenSpacing: openSpacing}
}

// BuildShards lays out the tier A streams plus depth, per decision 0007.
func BuildShards(all, quotes, depth []string, depthSpeed string, perWS int) []Shard {
	if perWS <= 0 {
		perWS = 100
	}
	var out []Shard
	var agg []string
	for _, s := range all {
		agg = append(agg, strings.ToLower(s)+"@aggTrade")
	}
	for i := 0; i*perWS < len(agg); i++ {
		j := min((i+1)*perWS, len(agg))
		out = append(out, Shard{Name: fmt.Sprintf("trade%02d", i), Base: WSMarket, Streams: agg[i*perWS : j], Stall: 60 * time.Second})
	}
	// forceOrder is sparse; markPrice@1s on the same connection provides liveness.
	out = append(out, Shard{Name: "misc", Base: WSMarket, Streams: []string{"!markPrice@arr@1s", "!forceOrder@arr"}, Stall: 30 * time.Second})
	var q []string
	for _, s := range quotes {
		q = append(q, strings.ToLower(s)+"@bookTicker")
	}
	if len(q) > 0 {
		out = append(out, Shard{Name: "quote", Base: WSPublic, Streams: q, Stall: 30 * time.Second})
	}
	var d []string
	for _, s := range depth {
		d = append(d, strings.ToLower(s)+"@depth@"+depthSpeed)
	}
	if len(d) > 0 {
		out = append(out, Shard{Name: "depth", Base: WSPublic, Streams: d, Stall: 30 * time.Second})
	}
	return out
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
	url := sh.Base + strings.Join(sh.Streams, "/")
	backoff := time.Second
	var gen int
	var cur *conn
	var reconnected bool
	for ctx.Err() == nil {
		gen++
		cur = c.start(ctx, sh, url, gen)
		started := time.Now()
		if reconnected && c.OnReconnect != nil {
			go func(k *conn) {
				select {
				case <-k.opened:
					c.OnReconnect(sh)
				case <-k.done:
				case <-ctx.Done():
				}
			}(cur)
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
			case <-time.After(time.Until(started.Add(connMaxAge))):
				// planned replacement: new connection first, then retire the old one
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
	ws, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return err
	}
	defer ws.CloseNow()
	ws.SetReadLimit(16 << 20)
	close(k.opened)
	if c.OnOpen != nil {
		c.OnOpen(sh)
	}
	log.Printf("shard %s conn %s open (%d streams)", sh.Name, k.id, len(sh.Streams))

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
		ch, sym := classify(data)
		c.sink.Write(groupOf(ch), rawlog.Record{
			Conn: k.id, CSeq: cseq, RecvWal: rw.UnixNano(), RecvMon: rawlog.Mono(),
			Channel: ch, Symbol: sym, Payload: data,
		})
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
		Conn: k.id, RecvWal: now.UnixNano(), RecvMon: rawlog.Mono(), Channel: "gap", Payload: p,
	})
}

func groupOf(ch string) string {
	switch {
	case ch == "depth":
		return rawlog.GroupDepth
	case ch == "bookTicker":
		return rawlog.GroupQuote
	case strings.HasPrefix(ch, "forceOrder"):
		return rawlog.GroupLiq
	default:
		return rawlog.GroupTrade
	}
}

// classify extracts channel and symbol from a combined-stream message without full parsing.
func classify(data []byte) (ch, sym string) {
	i := bytes.Index(data, []byte(`"stream":"`))
	if i < 0 {
		return "unknown", ""
	}
	rest := data[i+10:]
	j := bytes.IndexByte(rest, '"')
	if j < 0 {
		return "unknown", ""
	}
	name := string(rest[:j])
	k := strings.IndexByte(name, '@')
	if k < 0 {
		return name, ""
	}
	sym = strings.ToUpper(name[:k])
	ch = name[k+1:]
	if m := strings.IndexByte(ch, '@'); m >= 0 {
		ch = ch[:m]
	}
	if sym != "" && sym[0] == '!' {
		return strings.TrimPrefix(name[:k], "!") + "_all", ""
	}
	return ch, sym
}
