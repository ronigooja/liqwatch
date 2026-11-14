// Package poller polls exchange REST endpoints into the raw JSONL archive.
//
// # Adding a venue
//
// A venue is one new file internal/poller/<venue>.go (plus its test). It edits no shared file.
//
//	func init() { RegisterVenue("bybit", bybitChannels, WithBaseURL("https://api.bybit.com")) }
//
//	func bybitChannels(e *Env) []Channel {
//		return []Channel{
//			e.HistoryChannel("openInterest", HistorySpec{
//				Step: 5 * time.Minute, Retention: 30 * 24 * time.Hour,
//				Mode: PageBackward, PageSize: 200,
//				Fetch: func(ctx context.Context, symbol string, start, end int64) ([]Row, error) {
//					resp, err := e.Get(ctx, "/v5/market/open-interest", url.Values{"symbol": {symbol}, ...})
//					... // parse resp.Body, return one Row{TS: exchange ms, Raw: row JSON} per row
//				}}),
//			e.SnapshotChannel("instruments", SnapshotSpec{Mode: SnapshotSplit, Fetch: ..., Split: ...}),
//		}
//	}
//
// What the venue implements:
//
//   - History channel: one Fetch(ctx, symbol, start, end) returning the rows of [start,end] (ms, inclusive).
//     It does a single HTTP request and no paging, retries, dedup or gap logic. Each Row carries the exchange
//     timestamp (ms) and the raw row JSON. Row.Meta is optional extra envelope metadata.
//   - Snapshot channel (daily, no dedup): Fetch(ctx, symbol) returning the raw response. See SnapshotMode.
//   - Anything else: build a Channel with Env.Channel and return Records yourself.
//
// What the framework provides through Env: the symbol universe (usable symbols of this venue in
// config/venue-map.toml, filtered by -syms; the map path is [poller] map_path), an HTTP client that sets
// User-Agent "Mozilla/5.0" on every request (Env.Doer, Env.Get), the base URL (config override
// [poller.venues.<name>] base_url, else the default given to RegisterVenue), per-venue rate limits
// (rps, weight_per_minute, channel_rps, weights), per-venue periods, overlap and initial lookback, the
// sink (Env.Sink), the scheduler (Env.Scheduler, bound after construction) and the state file
// (poll-state.json for binance, poll-state-<venue>.json otherwise; job keys are venue/channel/symbol).
// Env.Get maps HTTP 429/418 to RetryError and rejects non-2xx and non-JSON bodies.
//
// What History does for a history channel and symbol:
//
//   - Start = persisted cursor (newest row timestamp seen) minus overlap, or now minus initial lookback
//     (default 2h) without a cursor. Retention clamps the start; a cursor older than retention writes a
//     retention_gap record {"event":"retention_gap","start_ms","end_ms"}.
//   - Paging, chosen by Mode:
//     PageWindows walks forward in windows of Window*Step ms: Fetch(start, start+Window*Step-1), then the
//     next window. Use it when the venue returns the newest rows of a range and caps rows per request, so
//     each window must hold at most the cap (Binance: 400 windows of 5m against a 500 row cap).
//     PageSize is a request limit, not a guarantee that a page is full: source gaps can make a capped
//     page short. PageForward advances from newest timestamp + 1; PageBackward advances to earliest - 1.
//     With Step > 0, a short page is final only within two steps of the range boundary; otherwise another
//     request confirms coverage. Empty pages and non-advancing pages stop the walk. PageBackward with
//     Step 0 confirms every short page until start; PageForward with Step 0 retains short-page termination
//     for sparse funding series. Use PageForward for oldest-first venues, PageBackward for newest-first.
//     Every request after the first of a walk waits for the venue's rate limit.
//   - Rows are sorted by time, exact duplicates inside the walk dropped, and each record carries
//     Meta["last_row_timestamp"]. The scheduler then drops rows already in the output (rebuilt from the
//     tail of today's and yesterday's file after a restart) and marks a changed row with the same
//     timestamp Meta["revised"]=true.
//   - For Step > 0, a bucket is returned only after its end plus Grace (default one Step):
//     ts + Step + Grace <= now. Recent buckets can change between reads and otherwise generate
//     repeated revisions; overlap re-reads them after they finalize. Step 0 and snapshots are unaffected.
//   - Step > 0: two consecutive rows more than Step apart write a source_gap record {"kind":"source_gap",
//     "start_ms","end_ms"} on channel "gap". Step 0 disables this (sparse series such as funding).
//   - no_data: for a symbol without cursor, a walk with zero rows writes one {"event":"no_data"} gap
//     record and the symbol is retried at most once per 24h (persisted in the state file). Set
//     SkipNoData for sparse series where an empty first window is normal.
//
// Dedup applies to any record whose Meta has last_row_timestamp (int64); snapshot records have none.
package poller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// PageMode selects how History walks a time range.
type PageMode int

const (
	// PageWindows walks forward in fixed windows of Window*Step.
	PageWindows PageMode = iota
	// PageForward walks forward from start; venue returns oldest rows first.
	PageForward
	// PageBackward walks backward from now; venue returns the newest end of the range.
	PageBackward
)

const noDataRetry = 24 * time.Hour

// Row is one exchange row: its timestamp in milliseconds and the raw JSON.
type Row struct {
	TS   int64
	Raw  []byte
	Meta map[string]any
}

// History walks one channel and symbol and returns the records to write.
type History struct {
	Venue, Channel, Symbol string
	Clock                  Clock
	// Sched is the cursor source (persisted RowTimestamp), rate limiter, no_data state and sink for
	// retention gaps. nil means no cursor, no waiting and no state.
	Sched     *Scheduler
	Overlap   time.Duration // default 2h
	Lookback  time.Duration // initial lookback, default 2h, clamped to MaxLookback (else Retention) when set
	Retention time.Duration // 0: unlimited; older starts are clamped and a retention_gap is written
	// MaxLookback clamps Lookback without implying a retention gap (0: use Retention).
	MaxLookback time.Duration
	Step        time.Duration // row granularity; 0 disables source_gap
	Grace       time.Duration // finalization delay; zero defaults to Step
	Mode        PageMode
	Window      int // PageWindows: rows per window
	PageSize    int // PageForward and PageBackward: request limit, not guaranteed page length
	// SkipNoData disables the no_data rule.
	SkipNoData bool
	Fetch      func(ctx context.Context, start, end int64) ([]Row, error)
}

type historyItem struct {
	ts  int64
	rec Record
}

func (h *History) gap(now time.Time, payload map[string]any) Record {
	p, _ := json.Marshal(payload)
	return Record{Venue: h.Venue, Symbol: h.Symbol, Channel: "gap", RecvWall: now.UnixNano(), Payload: p}
}

// Run performs one walk.
func (h *History) Run(ctx context.Context) ([]Record, error) {
	if h.Fetch == nil {
		return nil, errors.New("history: no Fetch")
	}
	clock := h.Clock
	if clock == nil {
		clock = RealClock{}
	}
	now := clock.Now()
	nowMS := now.UnixMilli()
	overlap, lookback := h.Overlap, h.Lookback
	if overlap <= 0 {
		overlap = 2 * time.Hour
	}
	if lookback <= 0 {
		lookback = 2 * time.Hour
	}
	maxLookback := h.MaxLookback
	if maxLookback <= 0 {
		maxLookback = h.Retention
	}
	if maxLookback > 0 && lookback > maxLookback {
		lookback = maxLookback
	}
	start := now.Add(-lookback).UnixMilli()
	var cursor, noData int64
	if h.Sched != nil {
		cursor = h.Sched.RowTimestamp(h.Venue, h.Channel, h.Symbol)
		noData = h.Sched.NoDataAt(h.Venue, h.Channel, h.Symbol)
	}
	useNoData := h.Sched != nil && h.Symbol != "" && !h.SkipNoData
	if useNoData && h.Sched.NoDataSkip(h.Venue, h.Channel, h.Symbol) {
		return nil, nil
	}
	if cursor > 0 {
		start = cursor - overlap.Milliseconds()
	}
	if h.Retention > 0 {
		oldest := now.Add(-h.Retention).UnixMilli()
		if start < oldest {
			if cursor > 0 && cursor < oldest && h.Sched != nil && h.Sched.Sink != nil {
				h.Sched.Sink.Write(h.gap(now, map[string]any{"event": "retention_gap", "channel": h.Channel, "symbol": h.Symbol, "start_ms": cursor, "end_ms": oldest}))
			}
			start = oldest
		}
	}
	var items []historyItem
	seen := map[int64]map[[32]byte]bool{}
	first := true
	request := func(from, to int64) ([]Row, error) {
		if !first && h.Sched != nil {
			if err := h.Sched.waitRequest(ctx, h.Venue, h.Channel); err != nil {
				return nil, err
			}
		}
		first = false
		rows, err := h.Fetch(ctx, from, to)
		if err != nil {
			return nil, err
		}
		recv := clock.Now()
		for _, r := range rows {
			if r.TS <= 0 {
				return nil, fmt.Errorf("missing row timestamp")
			}
			if !json.Valid(r.Raw) {
				return nil, fmt.Errorf("invalid row JSON")
			}
			sum := sha256.Sum256(r.Raw)
			if seen[r.TS][sum] {
				continue
			}
			if seen[r.TS] == nil {
				seen[r.TS] = map[[32]byte]bool{}
			}
			seen[r.TS][sum] = true
			meta := make(map[string]any, len(r.Meta)+1)
			for k, v := range r.Meta {
				meta[k] = v
			}
			meta["last_row_timestamp"] = r.TS
			items = append(items, historyItem{r.TS, Record{Venue: h.Venue, Symbol: h.Symbol, Channel: h.Channel, RecvWall: recv.UnixNano(), RecvMono: time.Since(monoOrigin).Nanoseconds(), Meta: meta, Payload: r.Raw}})
		}
		return rows, nil
	}
	switch h.Mode {
	case PageWindows:
		width := int64(h.Window) * h.Step.Milliseconds()
		if width <= 0 {
			return nil, errors.New("history: PageWindows needs Window and Step")
		}
		for start <= nowMS {
			end := min(nowMS, start+width-1)
			if _, err := request(start, end); err != nil {
				return nil, err
			}
			start = end + 1
		}
	case PageForward:
		for start <= nowMS {
			rows, err := request(start, nowMS)
			if err != nil {
				return nil, err
			}
			if len(rows) == 0 || h.PageSize <= 0 || (h.Step <= 0 && len(rows) < h.PageSize) {
				break
			}
			newest := int64(0)
			for _, r := range rows {
				newest = max(newest, r.TS)
			}
			if newest < start {
				if h.Step <= 0 {
					return nil, fmt.Errorf("pagination did not advance")
				}
				break
			}
			if h.Step > 0 && len(rows) < h.PageSize && nowMS-newest <= 2*h.Step.Milliseconds() {
				break
			}
			start = newest + 1
		}
	case PageBackward:
		end := nowMS
		for end >= start {
			rows, err := request(start, end)
			if err != nil {
				return nil, err
			}
			if len(rows) == 0 || h.PageSize <= 0 {
				break
			}
			earliest := rows[0].TS
			for _, r := range rows {
				earliest = min(earliest, r.TS)
			}
			if earliest <= start {
				break
			}
			if earliest-1 >= end {
				break
			}
			if h.Step > 0 && len(rows) < h.PageSize && earliest-start <= 2*h.Step.Milliseconds() {
				break
			}
			end = earliest - 1
		}
	default:
		return nil, fmt.Errorf("history: unknown page mode %d", h.Mode)
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].ts < items[j].ts })
	if h.Mode == PageBackward {
		kept := items[:0]
		for _, it := range items {
			if it.ts >= start {
				kept = append(kept, it)
			}
		}
		items = kept
	}
	hadRows := len(items) > 0
	if h.Step > 0 {
		grace := h.Grace
		if grace <= 0 {
			grace = h.Step
		}
		cutoff := now.Add(-h.Step - grace).UnixMilli()
		kept := items[:0]
		for _, it := range items {
			if it.ts <= cutoff {
				kept = append(kept, it)
			}
		}
		items = kept
	}
	out := make([]Record, 0, len(items))
	for _, it := range items {
		out = append(out, it.rec)
	}
	if h.Step > 0 {
		step := h.Step.Milliseconds()
		for i := 1; i < len(items); i++ {
			prev, next := items[i-1].ts, items[i].ts
			if next-prev <= step {
				continue
			}
			out = append(out, h.gap(now, map[string]any{"event": "source_gap", "kind": "source_gap", "channel": h.Channel, "symbol": h.Symbol, "start_ms": prev + step, "end_ms": next}))
		}
	}
	if useNoData && cursor == 0 && !hadRows {
		if noData == 0 {
			rec := h.gap(now, map[string]any{"event": "no_data", "channel": h.Channel, "symbol": h.Symbol})
			rec.RecvMono = time.Since(monoOrigin).Nanoseconds()
			out = append(out, rec)
		}
		h.Sched.MarkNoData(h.Venue, h.Channel, h.Symbol, now)
	}
	return out, nil
}
