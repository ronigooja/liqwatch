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
	History     bool // Replayable history may defer its first fetch to its phase; snapshots may not.
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
	next time.Time
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
	capacityReported     map[string]string
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
		b = &bucket{}
		s.buckets[c.Venue] = b
	}
	if d := b.next.Sub(now); d > wait {
		wait = d
	}
	if wait < 0 {
		return 0
	}
	return wait
}

// charge paces weighted work without banked idle/startup credit. A request reserves
// its cost AFTER its start, so even costs above a minute's budget remain runnable.
// A rolling interval can include one boundary request; no token-bucket burst is allowed.
func (s *Scheduler) charge(c Channel, now time.Time) {
	w := c.Weight
	if w <= 0 {
		w = 1
	}
	d := time.Duration(float64(time.Minute) * w / s.WeightPerMinute)
	if d < time.Nanosecond {
		d = time.Nanosecond
	}
	s.buckets[c.Venue].next = now.Add(d)
	s.lastRequest[c.Venue+"/"+c.Name] = now
}

type scheduledJob struct {
	c        Channel
	sym, key string
	deadline time.Time
	order    int
}

func cadence(c Channel) time.Duration {
	if c.Daily {
		return 24 * time.Hour
	}
	return c.Period
}

// schedule assigns evenly spaced, collision-free ranks to canonical keys within
// each venue/cadence. For an unchanged universe/config, phases survive restart
// without new state. Daily windows still open at 00:05 UTC. The next lattice slot
// is always <= one period after success; missed slots coalesce, never catch up.
// These are eligibility deadlines, NOT HTTP-success bounds under overload/failure.
func (s *Scheduler) schedule(symbols []string) []scheduledJob {
	groups := map[string][]scheduledJob{}
	for order, c := range s.Channels {
		if channelDropped(c.Venue, c.Name) {
			continue
		}
		syms := []string{""}
		if c.PerSymbol {
			syms = symbols
		}
		for _, sym := range syms {
			group := fmt.Sprintf("%s/%d/%t", c.Venue, cadence(c), c.Daily)
			groups[group] = append(groups[group], scheduledJob{c: c, sym: sym, key: jobKey(c, sym), order: order})
		}
	}
	var jobs []scheduledJob
	for _, group := range groups {
		sort.Slice(group, func(i, j int) bool { return group[i].key < group[j].key })
		for i, j := range group {
			p := cadence(j.c)
			last := s.state.Jobs[j.key].LastSuccess
			j.deadline = time.UnixMilli(s.state.RecordingStartMS)
			if p > 0 && (last != 0 || j.c.History) {
				offset := time.Duration(float64(p) * float64(i) / float64(len(group)))
				if j.c.Daily {
					offset += 5 * time.Minute
				}
				after := j.deadline
				if last != 0 {
					after = time.Unix(0, last)
				}
				j.deadline = after.Add(-offset).Truncate(p).Add(offset)
				if j.deadline.Before(after) || last != 0 {
					j.deadline = j.deadline.Add(p)
				}
			} else if last != 0 {
				continue // A nonpositive period is one-shot, as in legacy state.
			}
			if f := s.failures[j.key]; f != nil {
				j.deadline = f.next // Retrying an old slot must not jump ahead of regular work.
			}
			jobs = append(jobs, j)
		}
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].deadline.Equal(jobs[j].deadline) {
			// Preserve factory order for equally urgent bootstrap dependencies
			// (notably exchangeInfo, which discovers the Binance universe).
			if jobs[i].order != jobs[j].order {
				return jobs[i].order < jobs[j].order
			}
			return jobs[i].key < jobs[j].key
		}
		return jobs[i].deadline.Before(jobs[j].deadline)
	})
	return jobs
}

// reportCapacity flags configurations impossible even with one HTTP request per
// job. Pagination, latency and failures can exceed capacity below this lower bound.
func (s *Scheduler) reportCapacity(jobs []scheduledJob, now time.Time) {
	venueLoad, endpointLoad := map[string]float64{}, map[string]float64{}
	for _, j := range jobs {
		p := cadence(j.c)
		if p <= 0 {
			continue
		}
		w := j.c.Weight
		if w <= 0 {
			w = 1
		}
		venueLoad[j.c.Venue] += w * float64(time.Minute) / float64(p)
		endpointLoad[j.c.Venue+"/"+j.c.Name] += float64(time.Second) / float64(p)
	}
	if s.capacityReported == nil {
		s.capacityReported = map[string]string{}
	}
	for _, j := range jobs {
		endpoint := j.c.Venue + "/" + j.c.Name
		msg := ""
		if load := venueLoad[j.c.Venue]; load > s.WeightPerMinute {
			msg = fmt.Sprintf("minimum venue weight/min %.6g exceeds configured %.6g", load, s.WeightPerMinute)
		}
		if load := endpointLoad[endpoint]; j.c.RPS > 0 && load > j.c.RPS {
			msg += fmt.Sprintf(" minimum endpoint requests/sec %.6g exceeds configured %.6g", load, j.c.RPS)
		}
		if msg != "" && s.capacityReported[endpoint] != msg {
			p, _ := json.Marshal(map[string]any{"event": "schedule_over_capacity", "channel": j.c.Name, "err": msg})
			s.Sink.Write(Record{Venue: j.c.Venue, Channel: "gap", RecvWall: now.UnixNano(), RecvMono: time.Since(monoOrigin).Nanoseconds(), Payload: p})
		}
		s.capacityReported[endpoint] = msg
	}
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
					s.charge(c, now)
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
	jobs := s.schedule(symbols)
	s.reportCapacity(jobs, now)
	for _, j := range jobs {
		c, sym, key := j.c, j.sym, j.key
		if s.stopped[key] || (c.Skip != nil && c.Skip(sym)) {
			continue
		}
		if now.Before(j.deadline) {
			if d := j.deadline.Sub(now); d < wait {
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
		s.charge(c, now)
		endpoint := c.Venue + "/" + c.Name
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
	start := s.Clock.Now()
	for {
		original := s.Channels
		var channels []Channel
		for _, c := range original {
			if channelDropped(c.Venue, c.Name) {
				continue
			}
			cc := c
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
			syms := s.Symbols()
			if oldUniverse != nil {
				syms = oldUniverse()
			}
			for _, j := range s.schedule(syms) {
				if !seen[j.key] && !s.stopped[j.key] && !j.deadline.After(start) && !(j.c.Skip != nil && j.c.Skip(j.sym)) {
					pending = true
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
