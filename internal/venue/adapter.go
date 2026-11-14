// Package venue defines the interface between a venue and the collector.
package venue

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
)

// Symbol is the venue's unmodified symbol name.
type Symbol = string

// Throttler optionally declares minimum receive-time intervals by record channel.
type Throttler interface {
	Throttles() map[string]time.Duration
}

// Shard describes one WebSocket connection.
type Shard struct {
	Name             string
	URL              string
	Refresh          func(ctx context.Context) (url string, err error) // Optional URL renewal before subsequent connections.
	Header           http.Header                                       // Optional WebSocket dial headers; nil sends none.
	Streams          []string                                          // Informational; used in gap records and readiness.
	Stall            time.Duration
	MaxAge           time.Duration // Zero selects the collector default.
	MaxSubsPerConn   int           // Informational.
	SubscribeSpacing time.Duration
}

// Adapter provides venue-specific discovery, connection setup, and decoding.
//
// Call order: Symbols, then Shards, then Split for every received frame. Split may depend on state
// set by Shards (for example the symbol universe), so an adapter must not be used before Shards ran.
// Split is called from one goroutine per connection and several connections run at once: an adapter
// that keeps state must make it safe for concurrent reads and must not rewrite it while connections
// are open (calling Shards again, for instance to refresh a token, needs its own locking).
// A frame that proves the link is alive but carries no data (pong, subscribe acknowledgement) must
// return nil; the collector counts every received frame as liveness for stall detection.
// A data frame the adapter cannot classify should return a record on channel "gap" describing it
// instead of nil, so an unknown message type is not lost silently.
type Adapter interface {
	Name() string
	// Symbols returns in-scope trading USDT-margined perpetual symbols in venue form.
	Symbols(ctx context.Context) ([]Symbol, error)
	// Shards may use ctx for connection prerequisites such as REST tokens.
	Shards(ctx context.Context, syms []Symbol) ([]Shard, error)
	// Subscribe returns ordered text frames to send after opening a connection.
	Subscribe(sh Shard) [][]byte
	// Heartbeat returns zero interval for no active ping; otherwise frame supplies each ping.
	Heartbeat(sh Shard) (every time.Duration, frame func() (msgType int, payload []byte))
	// Split ignores control frames and preserves each data payload or its per-symbol envelope.
	// The collector supplies venue, connection, sequence and receive timestamps.
	Split(sh Shard, msg []byte) []rawlog.Record
	// Classes maps channels to rawlog drop classes.
	Classes() func(channel string) (class string, ok bool)
}

var registry = struct {
	sync.RWMutex
	factories map[string]func() Adapter
}{factories: make(map[string]func() Adapter)}

// Register installs a factory; duplicate names panic.
func Register(name string, f func() Adapter) {
	registry.Lock()
	defer registry.Unlock()
	if name == "" || f == nil || registry.factories[name] != nil {
		panic("venue: invalid or duplicate registration: " + name)
	}
	registry.factories[name] = f
}

// Get constructs a registered adapter, or returns nil if the name is unknown.
func Get(name string) Adapter {
	registry.RLock()
	f := registry.factories[name]
	registry.RUnlock()
	if f == nil {
		return nil
	}
	return f()
}
