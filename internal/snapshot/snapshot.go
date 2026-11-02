// Package snapshot schedules REST order-book snapshots.
package snapshot

import (
	"container/heap"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/wicker/wicker/internal/config"
	"github.com/wicker/wicker/internal/rawlog"
)

const (
	endpoint       = "https://fapi.binance.com/fapi/v1/depth"
	requestWeight  = 20
	window         = time.Minute
	maxBackoff     = 2 * time.Minute
	initialBackoff = time.Second
)

// Doer is the HTTP operation used by Scheduler. *http.Client implements it.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// Statistics are cumulative counters, except WeightUsed which is total request
// weight accounted by this scheduler.
type Statistics struct {
	Requests    uint64
	WeightUsed  int
	RateLimited uint64
}

// Scheduler schedules initial, verification, and explicitly-triggered snapshots.
type Scheduler struct {
	cfg         config.Snapshot
	symbols     []string
	sink        rawlog.Sink
	doer        Doer
	weightLimit int
	budget      int
	trigger     chan string
	statsMu     sync.RWMutex
	stats       Statistics
	mu          sync.Mutex
	backoff     time.Duration
	headerUsed  int
	headerAt    time.Time
}

type requestEvent struct {
	symbol string
	due    time.Time
	seq    uint64
	index  int
}

type eventHeap []*requestEvent

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].seq < h[j].seq
	}
	return h[i].due.Before(h[j].due)
}
func (h eventHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *eventHeap) Push(x any) { *h = append(*h, x.(*requestEvent)) }
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// New constructs a scheduler. A nil doer uses http.DefaultClient.
func New(cfg config.Snapshot, symbols []string, sink rawlog.Sink, weightPerMinLimit int, httpDoer Doer) *Scheduler {
	if httpDoer == nil {
		httpDoer = http.DefaultClient
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 1000
	}
	if cfg.VerifyEveryMin <= 0 {
		cfg.VerifyEveryMin = 5
	}
	budget := int(float64(weightPerMinLimit) * cfg.WeightFraction)
	if cfg.WeightFraction <= 0 {
		budget = 0
	}
	copySymbols := append([]string(nil), symbols...)
	return &Scheduler{
		cfg:         cfg,
		symbols:     copySymbols,
		sink:        sink,
		doer:        httpDoer,
		weightLimit: weightPerMinLimit,
		budget:      budget,
		trigger:     make(chan string, len(copySymbols)*2+1),
	}
}

// Stats returns a snapshot of the scheduler counters.
func (s *Scheduler) Stats() Statistics {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	return s.stats
}

// Trigger requests one snapshot. Duplicate queued triggers are coalesced by Run.
func (s *Scheduler) Trigger(symbol string) {
	select {
	case s.trigger <- symbol:
	default:
	}
}

// Run schedules snapshots until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	if len(s.symbols) == 0 {
		<-ctx.Done()
		return
	}
	start := time.Now()
	var events eventHeap
	heap.Init(&events)
	queued := make(map[string]bool)
	eventsBySymbol := make(map[string]*requestEvent)
	var seq uint64
	add := func(symbol string, due time.Time) {
		if queued[symbol] {
			if current := eventsBySymbol[symbol]; due.Before(current.due) {
				current.due = due
				heap.Fix(&events, current.index)
			}
			return
		}
		queued[symbol] = true
		seq++
		e := &requestEvent{symbol: symbol, due: due, seq: seq}
		heap.Push(&events, e)
		e.index = events.Len() - 1
		eventsBySymbol[symbol] = e
	}
	interval := staggerInterval(len(s.symbols))
	for i, symbol := range s.symbols {
		add(symbol, start.Add(time.Duration(i)*interval))
	}

	var used []usage
	var backoffUntil time.Time
	for {
		var timer *time.Timer
		var timerC <-chan time.Time
		if events.Len() > 0 {
			d := time.Until(events[0].due)
			if d < 0 {
				d = 0
			}
			timer = time.NewTimer(d)
			timerC = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case symbol := <-s.trigger:
			if timer != nil {
				timer.Stop()
			}
			if contains(s.symbols, symbol) {
				add(symbol, time.Now())
			}
		case <-timerC:
			e := heap.Pop(&events).(*requestEvent)
			delete(eventsBySymbol, e.symbol)
			queued[e.symbol] = false
			now := time.Now()
			if now.Before(backoffUntil) {
				add(e.symbol, backoffUntil)
				continue
			}
			used = prune(used, now)
			if s.budget <= 0 || s.weightLimit <= 0 || s.localUsed(used, now)+requestWeight > s.budget {
				due := now.Add(time.Until(windowStart(now).Add(window)))
				if due.Before(now.Add(10 * time.Millisecond)) {
					due = now.Add(time.Second)
				}
				add(e.symbol, due)
				continue
			}

			u, rateLimited := s.fetch(ctx, e.symbol)
			used = append(used, u)
			if rateLimited {
				backoffUntil = time.Now().Add(u.retryAfter)
				if u.retryAfter == 0 {
					backoffUntil = time.Now().Add(s.nextBackoff())
				}
				add(e.symbol, backoffUntil)
				continue
			}
			add(e.symbol, time.Now().Add(s.verifyDelay(e.symbol)))
		}
	}
}

type usage struct {
	at         time.Time
	weight     int
	retryAfter time.Duration
}

func (s *Scheduler) fetch(ctx context.Context, symbol string) (usage, bool) {
	start := time.Now()
	u := usage{at: start, weight: requestWeight}
	values := url.Values{}
	values.Set("symbol", symbol)
	values.Set("limit", "1000")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+values.Encode(), nil)
	if err != nil {
		return u, false
	}
	s.statsMu.Lock()
	s.stats.Requests++
	s.stats.WeightUsed += requestWeight
	s.statsMu.Unlock()
	resp, err := s.doer.Do(req)
	if err != nil {
		return u, false
	}
	body, readErr := io.ReadAll(resp.Body)
	end := time.Now()
	resp.Body.Close()
	if v, err := strconv.Atoi(resp.Header.Get("X-MBX-USED-WEIGHT-1M")); err == nil {
		s.mu.Lock()
		s.headerUsed, s.headerAt = v, end
		s.mu.Unlock()
		s.statsMu.Lock()
		if v > s.stats.WeightUsed {
			s.stats.WeightUsed = v
		}
		s.statsMu.Unlock()
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusTeapot {
		s.statsMu.Lock()
		s.stats.RateLimited++
		s.statsMu.Unlock()
		log.Printf("snapshot REST rate limited: status=%d", resp.StatusCode)
	}
	if readErr == nil && s.sink != nil {
		payload := struct {
			ReqStartNS int64           `json:"req_start_ns"`
			ReqEndNS   int64           `json:"req_end_ns"`
			Status     int             `json:"status"`
			Body       json.RawMessage `json:"body"`
		}{unixNano(start), unixNano(end), resp.StatusCode, json.RawMessage(body)}
		encoded, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			payload.Body, _ = json.Marshal(string(body))
			encoded, _ = json.Marshal(payload)
		}
		s.sink.Write(rawlog.GroupDepth, rawlog.Record{
			RecvWal: unixNano(end), RecvMon: end.Sub(processStart).Nanoseconds(),
			Channel: "snapshot", Symbol: symbol, Payload: encoded,
		})
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusTeapot {
		u.retryAfter = retryAfter(resp.Header.Get("Retry-After"))
		return u, true
	}
	return u, readErr != nil
}

var processStart = time.Now()

func unixNano(t time.Time) int64 { return t.UnixNano() }

func (s *Scheduler) nextBackoff() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.backoff == 0 {
		s.backoff = initialBackoff
	} else {
		s.backoff *= 2
		if s.backoff > maxBackoff {
			s.backoff = maxBackoff
		}
	}
	return s.backoff
}

func (s *Scheduler) localUsed(values []usage, now time.Time) int {
	local := sum(values)
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.Sub(s.headerAt) < window && s.headerUsed > local {
		return s.headerUsed
	}
	return local
}

func (s *Scheduler) verifyDelay(symbol string) time.Duration {
	base := time.Duration(s.cfg.VerifyEveryMin) * time.Minute
	if base <= 0 {
		base = 5 * time.Minute
	}
	// A stable, small per-symbol offset prevents periodic bursts without a PRNG.
	var n uint32
	for i := 0; i < len(symbol); i++ {
		n = n*33 + uint32(symbol[i])
	}
	return base + time.Duration(int64(n%21)-10)*base/100
}

func staggerInterval(n int) time.Duration {
	if n <= 1 {
		return 0
	}
	interval := window / time.Duration(n)
	if interval > time.Second {
		return time.Second
	}
	return interval
}

func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

func prune(values []usage, now time.Time) []usage {
	cutoff := now.Add(-window)
	i := 0
	for i < len(values) && values[i].at.Before(cutoff) {
		i++
	}
	return values[i:]
}

func sum(values []usage) int {
	total := 0
	for _, v := range values {
		total += v.weight
	}
	return total
}

func windowStart(t time.Time) time.Time {
	return t.Truncate(window)
}

func retryAfter(value string) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if d := time.Until(when); d > 0 {
			return d
		}
	}
	return 0
}
