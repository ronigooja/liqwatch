package collector

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

type throttleKey struct{ channel, symbol string }
type throttleBucket struct {
	start  int64
	record rawlog.Record
}
type throttler struct {
	mu        sync.Mutex
	intervals map[string]time.Duration
	buckets   map[throttleKey]throttleBucket
	counts    map[string]*atomic.Int64
	sink      rawlog.Sink
}

// ConfigureThrottle sets adapter defaults and optional channel overrides. Call before Run.
func (c *Collector) ConfigureThrottle(enabled bool, overrides map[string]time.Duration) {
	intervals := map[string]time.Duration{}
	if a, ok := c.adapter.(venue.Throttler); ok && enabled {
		for ch, d := range a.Throttles() {
			intervals[ch] = d
		}
		for ch, d := range overrides {
			if _, ok := intervals[ch]; ok {
				intervals[ch] = d
			}
		}
	}
	t := &throttler{intervals: intervals, buckets: make(map[throttleKey]throttleBucket), counts: make(map[string]*atomic.Int64), sink: c.sink}
	for ch := range intervals {
		t.counts[ch] = &atomic.Int64{}
	}
	c.throttle = t
}

// Thinned returns a snapshot of records replaced within buckets.
func (c *Collector) Thinned() map[string]int64 {
	r := make(map[string]int64)
	if c.throttle != nil {
		for ch, n := range c.throttle.counts {
			r[ch] = n.Load()
		}
	}
	return r
}

func (t *throttler) write(r rawlog.Record) {
	d := t.intervals[r.Channel]
	if d <= 0 || r.RecvWal == 0 {
		t.sink.Write(rawlog.GroupTrade, r)
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	k := throttleKey{r.Channel, r.Symbol}
	start := time.Unix(0, r.RecvWal).Truncate(d).UnixNano()
	if b, ok := t.buckets[k]; ok {
		if b.start == start {
			t.counts[r.Channel].Add(1)
			// Concurrent overlapping connections may deliver older receive times later.
			if r.RecvWal >= b.record.RecvWal {
				t.buckets[k] = throttleBucket{start, r}
			}
			return
		}
		t.sink.Write(rawlog.GroupTrade, b.record)
	}
	t.buckets[k] = throttleBucket{start, r}
}

func (t *throttler) flush(now time.Time, conn string, all bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, b := range t.buckets {
		if all || (conn != "" && b.record.Conn == conn) || (conn == "" && now.UnixNano() >= b.start+int64(t.intervals[k.channel])+int64(500*time.Millisecond)) {
			t.sink.Write(rawlog.GroupTrade, b.record)
			delete(t.buckets, k)
		}
	}
}
