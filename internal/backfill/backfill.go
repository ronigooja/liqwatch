// Package backfill fills aggTrade gaps after a WebSocket reconnect.
//
// Verified against the official Binance USDⓈ-M Futures documentation:
//   - aggTrades request weight is 20.
//   - limit has a maximum of 1000.
//   - fromId selects aggregate trades beginning with the specified aggregate
//     trade id (inclusive).
//
// Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Compressed-Aggregate-Trades-List
package backfill

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
)

const (
	endpoint       = "https://fapi.binance.com/fapi/v1/aggTrades"
	requestWeight  = 20
	window         = time.Minute
	maxPages       = 20
	initialBackoff = time.Second
	maxBackoff     = 2 * time.Minute
)

// Doer is the HTTP operation used by Backfiller. *http.Client implements it.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

type Tap struct {
	next       rawlog.Sink
	mu         sync.RWMutex
	last       map[string]int64
	generation uint64
	marks      map[uint64]map[string]bool
	starts     map[uint64]map[string]int64
}

// NewTap creates a concurrent aggTrade id tracker which forwards records first.
func NewTap(next rawlog.Sink) *Tap {
	return &Tap{next: next, last: make(map[string]int64), marks: make(map[uint64]map[string]bool), starts: make(map[uint64]map[string]int64)}
}

func (t *Tap) Write(group string, r rawlog.Record) {
	if t.next != nil {
		t.next.Write(group, r)
	}
	if r.Channel != "aggTrade" && r.Channel != "aggTrade_backfill" {
		return
	}
	forEachID(r.Payload, func(id int64) {
		t.mu.Lock()
		if id > t.last[r.Symbol] {
			t.last[r.Symbol] = id
		}
		if r.Channel == "aggTrade" {
			for _, seen := range t.marks {
				seen[r.Symbol] = true
			}
		}
		t.mu.Unlock()
	})
}

// Last returns the greatest aggregate trade id observed for symbol.
func (t *Tap) Last(symbol string) (int64, bool) {
	t.mu.RLock()
	v, ok := t.last[symbol]
	t.mu.RUnlock()
	return v, ok
}

// Symbols returns symbols for which an aggregate trade id has been observed.
func (t *Tap) Symbols() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	r := make([]string, 0, len(t.last))
	for s := range t.last {
		r = append(r, s)
	}
	return r
}

// Mark starts a reconnect generation. FirstAfter reports the first id observed
// after that mark, if any.
func (t *Tap) Mark() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.generation++
	t.marks[t.generation] = make(map[string]bool)
	t.starts[t.generation] = make(map[string]int64, len(t.last))
	for symbol, id := range t.last {
		t.starts[t.generation][symbol] = id
	}
	return t.generation
}

// FirstAfter reports the first id observed after marker for symbol.
func (t *Tap) FirstAfter(symbol string, marker uint64) (int64, bool) {
	t.mu.RLock()
	seen := t.marks[marker][symbol]
	id := t.last[symbol]
	t.mu.RUnlock()
	return id, seen
}

func (t *Tap) release(marker uint64) {
	if marker == 0 {
		return
	}
	t.mu.Lock()
	delete(t.marks, marker)
	delete(t.starts, marker)
	t.mu.Unlock()
}

func (t *Tap) lastAt(symbol string, marker uint64) (int64, bool) {
	t.mu.RLock()
	id, ok := t.starts[marker][symbol]
	t.mu.RUnlock()
	return id, ok
}

type request struct {
	symbols []string
	marker  uint64
}

type Backfiller struct {
	tap         *Tap
	sink        rawlog.Sink
	doer        Doer
	weightLimit int
	fraction    float64
	requests    chan request
	now         func() time.Time
	sleep       func(context.Context, time.Duration) error
	mu          sync.Mutex
	usage       []time.Time
	headerUsed  int
	headerAt    time.Time
	backoff     time.Duration
}

// New constructs a backfiller. A nil doer uses http.DefaultClient.
func New(tap *Tap, sink rawlog.Sink, doer Doer, weightPerMinLimit int, fraction float64) *Backfiller {
	if doer == nil {
		doer = http.DefaultClient
	}
	return &Backfiller{tap: tap, sink: sink, doer: doer, weightLimit: weightPerMinLimit, fraction: fraction, requests: make(chan request, 32), now: time.Now, sleep: sleepContext}
}

// RequestShard queues a reconnect backfill. Call it after the new shard is open.
func (b *Backfiller) RequestShard(symbols []string) {
	if b.tap == nil {
		return
	}
	b.requests <- request{symbols: append([]string(nil), symbols...), marker: b.tap.Mark()}
}

// RequestShardAfter queues a backfill using a marker returned by Tap.Mark.
// The marker must be created before reconnecting the WebSocket shard.
func (b *Backfiller) RequestShardAfter(symbols []string, marker uint64) {
	if b.tap == nil {
		return
	}
	b.requests <- request{symbols: append([]string(nil), symbols...), marker: marker}
}

// Run processes reconnect requests until ctx is cancelled.
func (b *Backfiller) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-b.requests:
			b.runRequest(ctx, r)
		}
	}
}

func (b *Backfiller) runRequest(ctx context.Context, r request) {
	for _, symbol := range r.symbols {
		last, ok := b.tap.lastAt(symbol, r.marker)
		if r.marker == 0 {
			last, ok = b.tap.Last(symbol)
		}
		if !ok {
			continue
		}
		from := last + 1
		capped := true
		for page := 0; page < maxPages; page++ {
			body, status, start, end, retry, err := b.fetch(ctx, symbol, from)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if e := b.sleep(ctx, retry); e != nil {
					return
				}
				continue
			}
			b.write(symbol, from, start, status, body, end)
			if status == http.StatusTooManyRequests || status == http.StatusTeapot {
				if err := b.sleep(ctx, retry); err != nil {
					return
				}
				continue
			}
			if status < 200 || status >= 300 {
				capped = false
				break
			}
			ids := aggregateIDs(body)
			if len(ids) == 0 {
				capped = false
				break
			}
			lastReturned := ids[len(ids)-1]
			from = lastReturned + 1
			if len(ids) < 1000 {
				capped = false
				break
			}
			if id, seen := b.tap.FirstAfter(symbol, r.marker); seen && id >= lastReturned {
				capped = false
				break
			}
		}
		if capped {
			log.Printf("aggTrade backfill page cap reached: %s", symbol)
		}
	}
	b.tap.release(r.marker)
}

func (b *Backfiller) fetch(ctx context.Context, symbol string, from int64) ([]byte, int, time.Time, time.Time, time.Duration, error) {
	if err := b.allow(ctx); err != nil {
		return nil, 0, b.now(), b.now(), 0, err
	}
	start := b.now()
	q := url.Values{"symbol": {symbol}, "fromId": {strconv.FormatInt(from, 10)}, "limit": {"1000"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+q.Encode(), nil)
	if err != nil {
		return nil, 0, start, b.now(), 0, err
	}
	resp, err := b.doer.Do(req)
	end := b.now()
	b.mu.Lock()
	b.usage = append(b.usage, start)
	b.mu.Unlock()
	if err != nil {
		return nil, 0, start, end, b.nextBackoff(), err
	}
	b.mu.Lock()
	if v, e := strconv.Atoi(resp.Header.Get("X-MBX-USED-WEIGHT-1M")); e == nil {
		b.headerUsed, b.headerAt = v, end
	}
	b.mu.Unlock()
	body, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil {
		return nil, resp.StatusCode, start, end, b.retry(resp), readErr
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusTeapot {
		return body, resp.StatusCode, start, end, b.retry(resp), nil
	}
	b.backoff = 0
	return body, resp.StatusCode, start, end, 0, nil
}

func (b *Backfiller) write(symbol string, from int64, start time.Time, status int, body []byte, end time.Time) {
	if b.sink == nil {
		return
	}
	p := struct {
		ReqStartNS int64           `json:"req_start_ns"`
		ReqEndNS   int64           `json:"req_end_ns"`
		Status     int             `json:"status"`
		FromID     int64           `json:"from_id"`
		Body       json.RawMessage `json:"body"`
	}{Status: status, FromID: from, Body: body, ReqEndNS: end.UnixNano()}
	p.ReqStartNS = start.UnixNano()
	encoded, _ := json.Marshal(p)
	b.sink.Write(rawlog.GroupTrade, rawlog.Record{RecvWal: end.UnixNano(), RecvMon: rawlog.Mono(), Channel: "aggTrade_backfill", Symbol: symbol, Payload: encoded})
}

func (b *Backfiller) allow(ctx context.Context) error {
	budget := int(float64(b.weightLimit) * b.fraction)
	if budget < requestWeight {
		budget = requestWeight
	}
	for {
		now := b.now()
		b.mu.Lock()
		i := 0
		for i < len(b.usage) && now.Sub(b.usage[i]) >= window {
			i++
		}
		b.usage = b.usage[i:]
		used := len(b.usage) * requestWeight
		if b.headerAt.IsZero() || now.Sub(b.headerAt) >= window || b.headerUsed <= int(float64(b.weightLimit)*.70) {
			if used+requestWeight <= budget {
				b.mu.Unlock()
				return nil
			}
		} else {
			used = b.headerUsed
			if used+requestWeight <= budget {
				b.mu.Unlock()
				return nil
			}
		}
		wait := window
		if len(b.usage) > 0 {
			wait = b.usage[0].Add(window).Sub(now)
		}
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		b.mu.Unlock()
		if err := b.sleep(ctx, wait); err != nil {
			return err
		}
	}
}

func (b *Backfiller) retry(resp *http.Response) time.Duration {
	if d := retryAfter(resp.Header.Get("Retry-After")); d > 0 {
		return d
	}
	return b.nextBackoff()
}
func (b *Backfiller) nextBackoff() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.backoff == 0 {
		b.backoff = initialBackoff
	} else {
		b.backoff *= 2
		if b.backoff > maxBackoff {
			b.backoff = maxBackoff
		}
	}
	return b.backoff
}
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func aggregateIDs(body []byte) []int64 {
	var v []struct {
		A int64 `json:"a"`
	}
	if json.Unmarshal(body, &v) != nil {
		return nil
	}
	r := make([]int64, len(v))
	for i := range v {
		r[i] = v[i].A
	}
	return r
}

func forEachID(p []byte, fn func(int64)) {
	for i := 0; i+3 < len(p); i++ {
		if p[i] != '"' || p[i+1] != 'a' || p[i+2] != '"' {
			continue
		}
		j := i + 3
		for j < len(p) && (p[j] == ' ' || p[j] == ':') {
			j++
		}
		k := j
		for k < len(p) && p[k] >= '0' && p[k] <= '9' {
			k++
		}
		if k > j {
			if n, err := strconv.ParseInt(string(p[j:k]), 10, 64); err == nil {
				fn(n)
			}
		}
	}
}
func retryAfter(v string) time.Duration {
	if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && time.Until(t) > 0 {
		return time.Until(t)
	}
	return 0
}
