package poller

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"
)

type Symbol = string

type Channel struct {
	Venue, Name string
	PerSymbol   bool
	Period      time.Duration
	Daily       bool
	Weight      float64
	RPS         float64
	Fetch       func(context.Context, Symbol) ([]Record, error)
	// Skip, when set and true for a symbol, leaves the job untouched this cycle: no request, no budget used.
	Skip func(Symbol) bool
}

type Clock interface {
	Now() time.Time
	Sleep(context.Context, time.Duration) error
}
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }
func (RealClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type RetryError struct {
	Err   error
	After time.Duration
}

func (e *RetryError) Error() string { return e.Err.Error() }

type PermanentError struct {
	Err     error
	Payload []byte
}

func (e *PermanentError) Error() string { return e.Err.Error() }

type jobState struct {
	LegacyHistory bool  `json:"legacy_history,omitempty"`
	LastSuccess   int64 `json:"last_success"`
	RowTimestamp  int64 `json:"row_timestamp,omitempty"`
	NoDataAt      int64 `json:"no_data_at,omitempty"`
}
type state struct {
	RecordingStartMS int64               `json:"recording_start_ms,omitempty"`
	Jobs             map[string]jobState `json:"jobs"`
	Symbols          []string            `json:"symbols,omitempty"`
}
type bucket struct {
	tokens  float64
	updated time.Time
}
type failure struct {
	attempts int
	since    time.Time
	next     time.Time
}

type Scheduler struct {
	Channels             []Channel
	Sink                 Sink
	Clock                Clock
	StatePath            string
	WeightPerMinute      float64
	Universe             func() []string
	state                state
	buckets              map[string]*bucket
	blocked, lastRequest map[string]time.Time
	failures             map[string]*failure
	stopped              map[string]bool
	seen                 map[string]map[int64]map[[32]byte]bool
	Duplicates           atomic.Uint64
	Revisions            atomic.Uint64
	// OnSweep is called after a RunOnce sweep or when Run reaches an idle pass.
	OnSweep func()
}

func NewScheduler(channels []Channel, sink Sink, clock Clock, path string, weight float64) (*Scheduler, error) {
	if clock == nil {
		clock = RealClock{}
	}
	if weight <= 0 {
		weight = 240
	}
	s := &Scheduler{Channels: channels, Sink: sink, Clock: clock, StatePath: path, WeightPerMinute: weight, state: state{Jobs: map[string]jobState{}}, buckets: map[string]*bucket{}, blocked: map[string]time.Time{}, lastRequest: map[string]time.Time{}, failures: map[string]*failure{}, stopped: map[string]bool{}, seen: map[string]map[int64]map[[32]byte]bool{}}
	if path != "" {
		b, err := os.ReadFile(path)
		if err == nil {
			if err = json.Unmarshal(b, &s.state); err != nil {
				s.state = state{Jobs: map[string]jobState{}}
			}
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}
	if s.state.Jobs == nil {
		s.state.Jobs = map[string]jobState{}
	}
	if s.state.RecordingStartMS == 0 {
		// An absent floor identifies legacy state. Preserve its cursor-bearing jobs;
		// cursorless and newly discovered jobs begin recording at migration time.
		for key, st := range s.state.Jobs {
			if st.RowTimestamp > 0 {
				st.LegacyHistory = true
				s.state.Jobs[key] = st
			}
		}
		s.state.RecordingStartMS = clock.Now().UnixMilli()
		if err := s.save(); err != nil {
			return nil, fmt.Errorf("save recording start: %w", err)
		}
	}
	return s, nil
}

// RecordingFloor returns the inclusive live history boundary. Legacy cursor-bearing
// jobs have no new boundary, so migration cannot discard their recovery scope.
func (s *Scheduler) RecordingFloor(venue, channel, symbol string) int64 {
	if s.state.Jobs[venue+"/"+channel+"/"+symbol].LegacyHistory {
		return 0
	}
	return s.state.RecordingStartMS
}

// StatePath returns the state file of a venue inside the raw directory. Each venue process has its own file
// so concurrent per-venue pollers never overwrite each other; binance keeps the original poll-state.json.
func StatePath(rawDir, venue string) string {
	if venue == "binance" {
		return filepath.Join(rawDir, "poll-state.json")
	}
	return filepath.Join(rawDir, "poll-state-"+venue+".json")
}
func jobKey(c Channel, sym string) string { return c.Venue + "/" + c.Name + "/" + sym }
func (s *Scheduler) RowTimestamp(venue, channel, symbol string) int64 {
	return s.state.Jobs[venue+"/"+channel+"/"+symbol].RowTimestamp
}

// NoDataSkip reports whether a symbol without cursor had an empty first request less than 24h ago.
func (s *Scheduler) NoDataSkip(venue, channel, symbol string) bool {
	st := s.state.Jobs[venue+"/"+channel+"/"+symbol]
	return st.RowTimestamp == 0 && st.NoDataAt > 0 && s.Clock.Now().Sub(time.Unix(0, st.NoDataAt)) < noDataRetry
}

// NoDataAt returns the unix-nanosecond time of the last empty first request, 0 if none.
func (s *Scheduler) NoDataAt(venue, channel, symbol string) int64 {
	return s.state.Jobs[venue+"/"+channel+"/"+symbol].NoDataAt
}

// MarkNoData records an empty first request; the state file persists it with the next successful job.
func (s *Scheduler) MarkNoData(venue, channel, symbol string, at time.Time) {
	key := venue + "/" + channel + "/" + symbol
	st := s.state.Jobs[key]
	st.NoDataAt = at.UnixNano()
	s.state.Jobs[key] = st
}
func (s *Scheduler) Symbols() []string           { return append([]string(nil), s.state.Symbols...) }
func (s *Scheduler) SetSymbols(symbols []string) { s.state.Symbols = append([]string(nil), symbols...) }
func (s *Scheduler) save() error {
	if s.StatePath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.StatePath), 0755); err != nil {
		return err
	}
	b, err := json.Marshal(s.state)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(s.StatePath), ".poll-state-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return err
	}
	return os.Rename(name, s.StatePath)
}
func due(c Channel, last int64, now time.Time) bool {
	if last == 0 {
		return true
	}
	t := time.Unix(0, last).UTC()
	now = now.UTC()
	if c.Daily {
		boundary := time.Date(now.Year(), now.Month(), now.Day(), 0, 5, 0, 0, time.UTC)
		if now.Before(boundary) {
			boundary = boundary.Add(-24 * time.Hour)
		}
		return t.Before(boundary)
	}
	if c.Period <= 0 {
		return false
	}
	return now.Truncate(c.Period).After(t.Truncate(c.Period))
}
func (s *Scheduler) available(c Channel, now time.Time) time.Duration {
	endpoint := c.Venue + "/" + c.Name
	wait := s.blocked[endpoint].Sub(now)
	if c.RPS > 0 {
		if d := s.lastRequest[endpoint].Add(time.Duration(float64(time.Second) / c.RPS)).Sub(now); d > wait {
			wait = d
		}
	}
	b := s.buckets[c.Venue]
	if b == nil {
		b = &bucket{tokens: s.WeightPerMinute, updated: now}
		s.buckets[c.Venue] = b
	}
	if now.After(b.updated) {
		b.tokens += now.Sub(b.updated).Minutes() * s.WeightPerMinute
		if b.tokens > s.WeightPerMinute {
			b.tokens = s.WeightPerMinute
		}
		b.updated = now
	}
	w := c.Weight
	if w <= 0 {
		w = 1
	}
	if w > s.WeightPerMinute {
		return 24 * time.Hour
	}
	if b.tokens < w {
		d := time.Duration((w - b.tokens) / s.WeightPerMinute * float64(time.Minute))
		if d > wait {
			wait = d
		}
	}
	if wait < 0 {
		return 0
	}
	return wait
}

// dedup reconstructs recent output before accepting a history row.
func (s *Scheduler) dedup(r *Record) bool {
	ts, ok := r.Meta["last_row_timestamp"].(int64)
	if !ok {
		return false
	}
	key := r.Venue + "/" + r.Channel + "/" + r.Symbol
	m, ok := s.seen[key]
	if !ok {
		m = map[int64]map[[32]byte]bool{}
		s.seen[key] = m
		if sink, yes := s.Sink.(*FileSink); yes {
			for _, day := range []time.Time{s.Clock.Now().Add(-24 * time.Hour), s.Clock.Now()} {
				path := filepath.Join(sink.Dir, r.Venue, r.Symbol, day.UTC().Format("20060102"), r.Channel+".p.jsonl")
				f, e := os.Open(path)
				if e != nil {
					continue
				}
				sc := bufio.NewScanner(f)
				sc.Buffer(make([]byte, 4096), 16<<20)
				for sc.Scan() {
					var v struct {
						Payload json.RawMessage `json:"p"`
						Meta    struct {
							TS int64 `json:"last_row_timestamp"`
						} `json:"meta"`
					}
					if json.Unmarshal(sc.Bytes(), &v) != nil || v.Meta.TS <= 0 {
						continue
					}
					addHash(m, v.Meta.TS, v.Payload)
				}
				f.Close()
			}
		}
	}
	var compact bytes.Buffer
	if json.Compact(&compact, r.Payload) != nil {
		return false
	}
	h := sha256.Sum256(compact.Bytes())
	if m[ts][h] {
		s.Duplicates.Add(1)
		return true
	}
	if len(m[ts]) > 0 {
		r.Meta["revised"] = true
		s.Revisions.Add(1)
	}
	addHash(m, ts, r.Payload)
	if len(m) > 10000 {
		var oldest int64
		for t := range m {
			if oldest == 0 || t < oldest {
				oldest = t
			}
		}
		delete(m, oldest)
	}
	return false
}
func addHash(m map[int64]map[[32]byte]bool, ts int64, raw []byte) {
	var b bytes.Buffer
	if json.Compact(&b, raw) != nil {
		return
	}
	h := sha256.Sum256(b.Bytes())
	if m[ts] == nil {
		m[ts] = map[[32]byte]bool{}
	}
	m[ts][h] = true
}
func (s *Scheduler) waitRequest(ctx context.Context, venue, name string) error {
	for _, c := range s.Channels {
		if c.Venue == venue && c.Name == name {
			for {
				now := s.Clock.Now()
				d := s.available(c, now)
				if d <= 0 {
					w := c.Weight
					if w <= 0 {
						w = 1
					}
					s.buckets[venue].tokens -= w
					s.lastRequest[venue+"/"+name] = now
					return nil
				}
				if err := s.Clock.Sleep(ctx, d); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
func (s *Scheduler) gap(c Channel, sym, event, msg string, f *failure) {
	now := s.Clock.Now()
	p, _ := json.Marshal(map[string]any{"event": event, "channel": c.Name, "symbol": sym, "err": msg, "attempt": f.attempts, "since_ns": f.since.UnixNano()})
	s.Sink.Write(Record{Venue: c.Venue, Symbol: sym, Channel: "gap", RecvWall: now.UnixNano(), RecvMono: time.Since(monoOrigin).Nanoseconds(), Payload: p})
}

// Step runs at most one due request and returns a suggested delay until another step.
func (s *Scheduler) Step(ctx context.Context) (time.Duration, error) {
	now := s.Clock.Now()
	wait := time.Second
	symbols := s.Symbols()
	if s.Universe != nil {
		symbols = s.Universe()
	}
	sort.Strings(symbols)
	for _, c := range s.Channels {
		if channelDropped(c.Venue, c.Name) {
			continue
		}
		syms := []string{""}
		if c.PerSymbol {
			syms = symbols
		}
		for _, sym := range syms {
			key := jobKey(c, sym)
			if s.stopped[key] || !due(c, s.state.Jobs[key].LastSuccess, now) || (c.Skip != nil && c.Skip(sym)) {
				continue
			}
			if f := s.failures[key]; f != nil && now.Before(f.next) {
				if d := f.next.Sub(now); d < wait {
					wait = d
				}
				continue
			}
			if d := s.available(c, now); d > 0 {
				if d < wait {
					wait = d
				}
				continue
			}
			w := c.Weight
			if w <= 0 {
				w = 1
			}
			s.buckets[c.Venue].tokens -= w
			endpoint := c.Venue + "/" + c.Name
			s.lastRequest[endpoint] = now
			records, err := c.Fetch(ctx, sym)
			if err != nil {
				f := s.failures[key]
				if f == nil {
					f = &failure{since: now}
					s.failures[key] = f
				}
				f.attempts++
				s.gap(c, sym, "poll_failed", err.Error(), f)
				var permanent *PermanentError
				if errors.As(err, &permanent) {
					p := permanent.Payload
					if !json.Valid(p) {
						p, _ = json.Marshal(string(p))
					}
					s.Sink.Write(Record{Venue: c.Venue, Symbol: sym, Channel: "brackets_error", RecvWall: now.UnixNano(), RecvMono: time.Since(monoOrigin).Nanoseconds(), Payload: p})
					s.stopped[key] = true
					return 0, nil
				}
				d := time.Second << min(f.attempts-1, 8)
				if d > 300*time.Second {
					d = 300 * time.Second
				}
				var retry *RetryError
				failedAt := s.Clock.Now()
				if errors.As(err, &retry) {
					if retry.After > d {
						d = retry.After
					}
					s.blocked[endpoint] = failedAt.Add(retry.After)
				}
				f.next = failedAt.Add(d)
				return 0, nil
			}
			for _, r := range records {
				payload, record, err := ApplyPolicy(r.Venue, r.Channel, r.Payload)
				if err != nil {
					return 0, fmt.Errorf("filter %s/%s: %w", r.Venue, r.Channel, err)
				}
				if !record {
					continue
				}
				r.Payload = payload
				// A policy change can cause one extra revision per row on the first fetch.
				if s.dedup(&r) {
					continue
				}
				s.Sink.Write(r)
			}
			if f := s.failures[key]; f != nil {
				s.gap(c, sym, "poll_recovered", "", f)
				delete(s.failures, key)
			}
			st := s.state.Jobs[key]
			st.LastSuccess = s.Clock.Now().UnixNano()
			for _, r := range records {
				if ts, ok := r.Meta["last_row_timestamp"].(int64); ok && ts > st.RowTimestamp {
					st.RowTimestamp = ts
				}
			}
			s.state.Jobs[key] = st
			if x, ok := s.Sink.(interface{ Flush() error }); ok {
				if err := x.Flush(); err != nil {
					return 0, err
				}
			}
			if err := s.save(); err != nil {
				return 0, fmt.Errorf("save poll state: %w", err)
			}
			return 0, nil
		}
	}
	return wait, nil
}

func (s *Scheduler) Run(ctx context.Context) error {
	for {
		d, err := s.Step(ctx)
		if err != nil {
			return err
		}
		if d > 0 {
			if s.OnSweep != nil {
				s.OnSweep()
			}
			if err = s.Clock.Sleep(ctx, d); err != nil {
				return err
			}
		}
	}
}

// RunOnce attempts each job due at the start of the sweep at most once.
func (s *Scheduler) RunOnce(ctx context.Context) error {
	if s.OnSweep != nil {
		defer s.OnSweep()
	}
	seen := map[string]bool{}
	for {
		original := s.Channels
		var channels []Channel
		for _, c := range original {
			if channelDropped(c.Venue, c.Name) {
				continue
			}
			cc := c
			if !c.PerSymbol && seen[jobKey(c, "")] {
				continue
			}
			fetch := c.Fetch
			cc.Fetch = func(ctx context.Context, sym string) ([]Record, error) {
				seen[jobKey(cc, sym)] = true
				return fetch(ctx, sym)
			}
			channels = append(channels, cc)
		}
		s.Channels = channels
		oldUniverse := s.Universe
		s.Universe = func() []string {
			var syms []string
			if oldUniverse != nil {
				syms = oldUniverse()
			} else {
				syms = s.Symbols()
			}
			return syms
		}
		// Temporarily suppress already attempted per-symbol jobs.
		added := []string{}
		for key := range seen {
			if !s.stopped[key] {
				s.stopped[key] = true
				added = append(added, key)
			}
		}
		d, err := s.Step(ctx)
		s.Channels = original
		s.Universe = oldUniverse
		for _, key := range added {
			delete(s.stopped, key)
		}
		if err != nil {
			return err
		}
		if d > 0 {
			pending := false
			for _, c := range original {
				if channelDropped(c.Venue, c.Name) {
					continue
				}
				syms := []string{""}
				if c.PerSymbol {
					if oldUniverse != nil {
						syms = oldUniverse()
					} else {
						syms = s.Symbols()
					}
				}
				for _, sym := range syms {
					if !seen[jobKey(c, sym)] && due(c, s.state.Jobs[jobKey(c, sym)].LastSuccess, s.Clock.Now()) && !(c.Skip != nil && c.Skip(sym)) {
						pending = true
					}
				}
			}
			if !pending {
				return nil
			}
			if err := s.Clock.Sleep(ctx, d); err != nil {
				return err
			}
		}
	}
}
