package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

const step5 = 5 * time.Minute

// fakeExchange serves a 5m series on /series. newest=true returns the newest `limit` rows of the requested
// range (Bybit/OKX/Gate style); newest=false returns the oldest rows first (Binance fundingRate style).
// Rows exist in [from, to] ms except timestamps in holes. value changes the payload of a timestamp.
type fakeExchange struct {
	from, to    int64
	newest      bool
	limit       int
	holes       map[int64]bool
	value       func(ts int64) int
	calls       atomic.Int64
	server      *httptest.Server
	windowLimit bool // limit timestamp slots rather than existing rows
}

func newFakeExchange(from, to int64, newest bool, limit int) *fakeExchange {
	f := &fakeExchange{from: from, to: to, newest: newest, limit: limit, holes: map[int64]bool{}, value: func(int64) int { return 1 }}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		start, _ := strconv.ParseInt(r.URL.Query().Get("start"), 10, 64)
		end, _ := strconv.ParseInt(r.URL.Query().Get("end"), 10, 64)
		if f.windowLimit {
			if f.newest {
				start = max(start, end-int64(f.limit-1)*step5.Milliseconds())
			} else {
				end = min(end, start+int64(f.limit)*step5.Milliseconds()-1)
			}
		}
		var rows []map[string]int64
		for ts := f.from; ts <= f.to; ts += step5.Milliseconds() {
			if ts >= start && ts <= end && !f.holes[ts] {
				rows = append(rows, map[string]int64{"ts": ts, "v": int64(f.value(ts))})
			}
		}
		if len(rows) > f.limit {
			if f.newest {
				rows = rows[len(rows)-f.limit:]
			} else {
				rows = rows[:f.limit]
			}
		}
		if f.newest { // newest first, as those venues return
			for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
				rows[i], rows[j] = rows[j], rows[i]
			}
		}
		json.NewEncoder(w).Encode(rows)
	}))
	return f
}

func TestHistoryShortPagesCoverRange(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	for _, mode := range []PageMode{PageBackward, PageForward} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			start := now.Add(-2500 * step5).UnixMilli()
			f := newFakeExchange(start, now.UnixMilli(), mode == PageBackward, 2000)
			defer f.server.Close()
			f.windowLimit = true
			for _, n := range []int64{600, 700, 800} {
				f.holes[start+n*step5.Milliseconds()] = true
			}
			h := genericHistory(f, &fakeClock{now}, nil, mode)
			h.Lookback = 2500 * step5
			var first int
			fetch := h.Fetch
			h.Fetch = func(ctx context.Context, a, b int64) ([]Row, error) {
				rows, err := fetch(ctx, a, b)
				if f.calls.Load() == 1 {
					first = len(rows)
				}
				return rows, err
			}
			recs, err := h.Run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if first != 1997 {
				t.Fatalf("first page=%d, want 1997", first)
			}
			got := map[int64]bool{}
			for _, r := range recs {
				if r.Channel == "oi" {
					got[r.Meta["last_row_timestamp"].(int64)] = true
				}
			}
			for ts := start; ts <= now.UnixMilli(); ts += step5.Milliseconds() {
				if got[ts] == f.holes[ts] {
					t.Fatalf("timestamp %d: present=%v hole=%v", ts, got[ts], f.holes[ts])
				}
			}
			if f.calls.Load() != 2 {
				t.Fatalf("calls=%d, want 2", f.calls.Load())
			}
		})
	}
}

func TestHistoryPagingTermination(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	for _, mode := range []PageMode{PageBackward, PageForward} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			for _, scenario := range []string{"exhausted", "empty", "repeated"} {
				t.Run(scenario, func(t *testing.T) {
					from, to := now.Add(-time.Hour).UnixMilli(), now.UnixMilli()
					if mode == PageForward {
						to = now.Add(-time.Hour).UnixMilli()
						from = now.Add(-2 * time.Hour).UnixMilli()
					}
					if scenario == "empty" {
						from, to = now.Add(time.Hour).UnixMilli(), now.Add(2*time.Hour).UnixMilli()
					}
					f := newFakeExchange(from, to, mode == PageBackward, 100)
					defer f.server.Close()
					h := genericHistory(f, &fakeClock{now}, nil, mode)
					h.Lookback = 3 * time.Hour
					if scenario == "repeated" {
						fetch := h.Fetch
						h.Fetch = func(ctx context.Context, _, _ int64) ([]Row, error) { return fetch(ctx, from, to) }
					}
					_, err := h.Run(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					want := int64(2)
					if scenario == "empty" {
						want = 1
					}
					if f.calls.Load() != want {
						t.Fatalf("calls=%d, want %d", f.calls.Load(), want)
					}
				})
			}
		})
	}
}

func (f *fakeExchange) fetch(ctx context.Context, start, end int64) ([]Row, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/series?start=%d&end=%d", f.server.URL, start, end), nil)
	resp, err := f.server.Client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var raws []json.RawMessage
	if err = json.Unmarshal(body, &raws); err != nil {
		return nil, err
	}
	var rows []Row
	for _, raw := range raws {
		rows = append(rows, Row{TS: tsOf(raw), Raw: raw})
	}
	return rows, nil
}

func tsOf(raw []byte) int64 {
	var x struct {
		TS int64 `json:"ts"`
	}
	json.Unmarshal(raw, &x)
	return x.TS
}

func genericHistory(f *fakeExchange, clock Clock, s *Scheduler, mode PageMode) *History {
	h := &History{Venue: "fake", Channel: "oi", Symbol: "BTCUSDT", Clock: clock, Sched: s, Lookback: 72 * time.Hour, Step: step5, Mode: mode, Fetch: f.fetch}
	switch mode {
	case PageWindows:
		h.Window = f.limit
	default:
		h.PageSize = f.limit
	}
	return h
}

func checkSeries(t *testing.T, recs []Record, wantFirst, wantLast int64, wantCount int) {
	t.Helper()
	var rows []Record
	for _, r := range recs {
		if r.Channel == "oi" {
			rows = append(rows, r)
		}
	}
	if len(rows) != wantCount {
		t.Fatalf("rows=%d want %d", len(rows), wantCount)
	}
	for i, r := range rows {
		ts := r.Meta["last_row_timestamp"].(int64)
		if i > 0 && ts != rows[i-1].Meta["last_row_timestamp"].(int64)+step5.Milliseconds() {
			t.Fatalf("row %d ts=%d not contiguous or not sorted", i, ts)
		}
		if i == 0 && ts != wantFirst || i == len(rows)-1 && ts != wantLast {
			t.Fatalf("row %d ts=%d (first %d last %d)", i, ts, wantFirst, wantLast)
		}
		if tsOf(r.Payload) != ts {
			t.Fatal("payload does not match timestamp")
		}
	}
}

func TestGenericPagingModes(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	nowMS := now.UnixMilli()
	start := now.Add(-72 * time.Hour).UnixMilli()
	for _, tc := range []struct {
		name   string
		mode   PageMode
		newest bool
		limit  int
		calls  int64
	}{
		{"windows with newest-end server", PageWindows, true, 100, 11}, // 72h = 864 steps in windows of 100 steps -> 9 + tail
		{"forward with oldest-first server", PageForward, false, 200, 5},
		{"backward with newest-end server", PageBackward, true, 200, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeExchange(nowMS-80*time.Hour.Milliseconds(), nowMS, tc.newest, tc.limit)
			defer f.server.Close()
			clock := &fakeClock{now}
			s, _ := NewScheduler(nil, &memorySink{}, clock, "", 240)
			recs, err := genericHistory(f, clock, s, tc.mode).Run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			first := (start + 299999) / 300000 * 300000
			checkSeries(t, recs, first, nowMS/300000*300000, int((nowMS/300000*300000-first)/300000)+1)
			if got := f.calls.Load(); got < tc.calls-2 || got > tc.calls+2 {
				t.Fatalf("calls=%d expected about %d", got, tc.calls)
			}
			for _, r := range recs {
				if r.Channel == "gap" {
					t.Fatalf("unexpected gap %s", r.Payload)
				}
			}
		})
	}
}

func TestGenericBackwardStopsAtStartAndNeverRepeats(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	nowMS := now.UnixMilli()
	f := newFakeExchange(nowMS-24*time.Hour.Milliseconds(), nowMS, true, 100)
	defer f.server.Close()
	clock := &fakeClock{now}
	var requested [][2]int64
	h := genericHistory(f, clock, nil, PageBackward)
	h.Lookback = 10 * time.Hour
	inner := h.Fetch
	h.Fetch = func(ctx context.Context, a, b int64) ([]Row, error) {
		requested = append(requested, [2]int64{a, b})
		return inner(ctx, a, b)
	}
	recs, err := h.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 121 { // 10h = 120 steps, inclusive start boundary
		t.Fatalf("rows=%d", len(recs))
	}
	for i := 1; i < len(requested); i++ {
		if requested[i][1] >= requested[i-1][1] {
			t.Fatalf("end did not move back: %v", requested)
		}
		if requested[i][0] != requested[0][0] {
			t.Fatalf("start changed: %v", requested)
		}
	}
	if len(requested) != 2 {
		t.Fatalf("requests=%v", requested)
	}
}

func TestGenericSourceGapsBothDirections(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	nowMS := now.UnixMilli()
	for _, mode := range []PageMode{PageWindows, PageForward, PageBackward} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			f := newFakeExchange(nowMS-3*time.Hour.Milliseconds(), nowMS, mode != PageForward, 20)
			defer f.server.Close()
			holeA := nowMS - 90*60*1000
			f.holes[holeA], f.holes[holeA+300000] = true, true
			clock := &fakeClock{now}
			h := genericHistory(f, clock, nil, mode)
			h.Lookback = 2 * time.Hour
			recs, err := h.Run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			var gaps []Record
			for _, r := range recs {
				if r.Channel == "gap" {
					gaps = append(gaps, r)
				}
			}
			if len(gaps) != 1 || len(recs) != 24+1-2+1 {
				t.Fatalf("gaps=%d recs=%d", len(gaps), len(recs))
			}
			var g struct {
				Event, Kind, Channel, Symbol string
				Start                        int64 `json:"start_ms"`
				End                          int64 `json:"end_ms"`
			}
			json.Unmarshal(gaps[0].Payload, &g)
			if g.Kind != "source_gap" || g.Event != "source_gap" || g.Channel != "oi" || g.Symbol != "BTCUSDT" || g.Start != holeA || g.End != holeA+600000 {
				t.Fatalf("gap=%+v", g)
			}
		})
	}
}

func fakeJob(f *fakeExchange, clock Clock, mode PageMode, sink Sink, state string) (*Scheduler, *History) {
	var h *History
	var sched *Scheduler
	ch := Channel{Venue: "fake", Name: "oi", PerSymbol: true, Period: step5, Weight: 1, Skip: func(sym string) bool { return sched.NoDataSkip("fake", "oi", sym) }, Fetch: func(ctx context.Context, sym string) ([]Record, error) {
		return h.Run(ctx)
	}}
	s, _ := NewScheduler([]Channel{ch}, sink, clock, state, 240)
	sched = s
	s.Universe = func() []string { return []string{"BTCUSDT"} }
	h = genericHistory(f, clock, s, mode)
	h.Lookback = 2 * time.Hour
	return s, h
}

func TestGenericOverlapDedupAndRevision(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	for _, mode := range []PageMode{PageWindows, PageForward, PageBackward} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			f := newFakeExchange(now.Add(-4*time.Hour).UnixMilli(), now.Add(2*time.Hour).UnixMilli(), mode != PageForward, 50)
			defer f.server.Close()
			clock := &fakeClock{now}
			sink := &memorySink{}
			s, _ := fakeJob(f, clock, mode, sink, "")
			if _, err := s.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			first := len(sink.records)
			if first != 25 {
				t.Fatalf("first run wrote %d", first)
			}
			cursor := s.RowTimestamp("fake", "oi", "BTCUSDT")
			if cursor != now.UnixMilli() {
				t.Fatalf("cursor %d", cursor)
			}
			clock.t = clock.t.Add(5 * time.Minute)
			f.value = func(ts int64) int {
				if ts == now.UnixMilli() {
					return 2
				}
				return 1
			}
			if _, err := s.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			// overlap of 2h (default) re-reads 24 old rows (all duplicates) plus now(revised) and now+5m (new)
			if s.Duplicates.Load() != 24 || s.Revisions.Load() != 1 {
				t.Fatalf("dups=%d revs=%d", s.Duplicates.Load(), s.Revisions.Load())
			}
			if len(sink.records) != first+2 {
				t.Fatalf("wrote %d", len(sink.records)-first)
			}
			rev := sink.records[first]
			if rev.Meta["revised"] != true || rev.Meta["last_row_timestamp"].(int64) != now.UnixMilli() {
				t.Fatalf("revision not first appended: %+v", rev.Meta)
			}
			if sink.records[first+1].Meta["revised"] != nil {
				t.Fatal("new row flagged revised")
			}
		})
	}
}

func TestGenericNoDataOncePerDay(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 2, 0, 0, time.UTC)
	f := newFakeExchange(now.Add(48*time.Hour).UnixMilli(), now.Add(49*time.Hour).UnixMilli(), true, 50) // nothing in range
	defer f.server.Close()
	clock := &fakeClock{now}
	sink := &memorySink{}
	state := filepath.Join(t.TempDir(), "state.json")
	s, _ := fakeJob(f, clock, PageBackward, sink, state)
	step := func() {
		t.Helper()
		if _, err := s.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	step()
	if f.calls.Load() != 1 || len(sink.records) != 1 {
		t.Fatalf("calls=%d records=%d", f.calls.Load(), len(sink.records))
	}
	var g map[string]any
	json.Unmarshal(sink.records[0].Payload, &g)
	if sink.records[0].Channel != "gap" || sink.records[0].Venue != "fake" || sink.records[0].Symbol != "BTCUSDT" || g["event"] != "no_data" || g["channel"] != "oi" || g["symbol"] != "BTCUSDT" {
		t.Fatalf("record=%+v payload=%s", sink.records[0], sink.records[0].Payload)
	}
	for i := 0; i < 30; i++ { // every 5 minutes for 2.5h, including after a restart
		clock.t = clock.t.Add(5 * time.Minute)
		if i == 10 {
			s, _ = fakeJob(f, clock, PageBackward, sink, state)
		}
		step()
	}
	if f.calls.Load() != 1 || len(sink.records) != 1 {
		t.Fatalf("requested again within a day: calls=%d records=%d", f.calls.Load(), len(sink.records))
	}
	clock.t = clock.t.Add(24 * time.Hour)
	step()
	if f.calls.Load() != 2 {
		t.Fatalf("no daily retry: calls=%d", f.calls.Load())
	}
	if len(sink.records) != 1 {
		t.Fatalf("no_data written again: %d", len(sink.records))
	}
	clock.t = clock.t.Add(5 * time.Minute)
	step()
	if f.calls.Load() != 2 {
		t.Fatal("retried twice in one day")
	}
	// data appears: normal operation resumes at the next retry
	clock.t = clock.t.Add(24 * time.Hour)
	f.from, f.to = clock.t.Add(-time.Hour).UnixMilli()/300000*300000, clock.t.UnixMilli()
	step()
	if s.RowTimestamp("fake", "oi", "BTCUSDT") == 0 {
		t.Fatal("cursor not set after data appeared")
	}
}

func TestGenericNoDataNotForSymbolWithCursorOrSkip(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 2, 0, 0, time.UTC)
	f := newFakeExchange(now.Add(48*time.Hour).UnixMilli(), now.Add(49*time.Hour).UnixMilli(), true, 50)
	defer f.server.Close()
	clock := &fakeClock{now}
	s, h := fakeJob(f, clock, PageBackward, &memorySink{}, "")
	s.state.Jobs["fake/oi/BTCUSDT"] = jobState{RowTimestamp: now.Add(-time.Hour).UnixMilli()}
	recs, err := h.Run(context.Background())
	if err != nil || len(recs) != 0 {
		t.Fatalf("cursor symbol got %v %v", recs, err)
	}
	h.SkipNoData = true
	s.state.Jobs["fake/oi/BTCUSDT"] = jobState{}
	if recs, err = h.Run(context.Background()); err != nil || len(recs) != 0 {
		t.Fatalf("SkipNoData got %v %v", recs, err)
	}
}

func TestGenericRestartReconstruction(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 2, 0, 0, time.UTC) // yesterday's file is also read
	for _, mode := range []PageMode{PageWindows, PageBackward} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			f := newFakeExchange(now.Add(-4*time.Hour).UnixMilli(), now.Add(time.Hour).UnixMilli(), true, 50)
			defer f.server.Close()
			clock := &fakeClock{now}
			dir := t.TempDir()
			state := filepath.Join(dir, "state.json")
			sink := NewFileSink(dir, time.Hour)
			s, _ := fakeJob(f, clock, mode, sink, state)
			if _, err := s.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			sink.Close()
			clock.t = clock.t.Add(5 * time.Minute)
			f.value = func(ts int64) int {
				if ts == now.UnixMilli() {
					return 9
				}
				return 1
			}
			sink = NewFileSink(dir, time.Hour)
			defer sink.Close()
			s, _ = fakeJob(f, clock, mode, sink, state)
			if _, err := s.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			if s.Duplicates.Load() == 0 {
				t.Fatal("restart wrote the overlap again")
			}
			if s.Revisions.Load() != 1 {
				t.Fatalf("revisions=%d", s.Revisions.Load())
			}
			sink.Flush()
			b, err := os.ReadFile(filepath.Join(dir, "fake", "BTCUSDT", now.Format("20060102"), "oi.p.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			seen := map[string]int{}
			for _, line := range splitLines(b) {
				var e struct {
					P json.RawMessage `json:"p"`
				}
				json.Unmarshal(line, &e)
				seen[string(e.P)]++
			}
			for p, n := range seen {
				if n != 1 {
					t.Fatalf("payload %s written %d times", p, n)
				}
			}
		})
	}
}

func splitLines(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, c := range b {
		if c == '\n' {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	return out
}

func TestStateNamespacedPerVenue(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	clock := &fakeClock{now}
	path := filepath.Join(t.TempDir(), "state.json")
	s, _ := NewScheduler(nil, &memorySink{}, clock, path, 240)
	s.state.Jobs["a/oi/BTCUSDT"] = jobState{RowTimestamp: 111}
	s.state.Jobs["b/oi/BTCUSDT"] = jobState{RowTimestamp: 222, NoDataAt: 333}
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	s, _ = NewScheduler(nil, &memorySink{}, clock, path, 240)
	if s.RowTimestamp("a", "oi", "BTCUSDT") != 111 || s.RowTimestamp("b", "oi", "BTCUSDT") != 222 || s.NoDataAt("b", "oi", "BTCUSDT") != 333 || s.NoDataAt("a", "oi", "BTCUSDT") != 0 {
		t.Fatalf("state=%+v", s.state)
	}
	if StatePath("/raw", "binance") != "/raw/poll-state.json" || StatePath("/raw", "bybit") != "/raw/poll-state-bybit.json" {
		t.Fatal(StatePath("/raw", "binance"), StatePath("/raw", "bybit"))
	}
}

func TestSnapshotFetch(t *testing.T) {
	clock := &fakeClock{time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)}
	bulk := SnapshotFetch("fake", "tiers", clock, SnapshotSpec{Mode: SnapshotBulk, Fetch: func(context.Context, string) ([]byte, error) { return []byte(`{"a":1}`), nil }})
	recs, err := bulk(context.Background(), "")
	if err != nil || len(recs) != 1 || recs[0].Symbol != "" || recs[0].Channel != "tiers" || recs[0].Meta["last_row_timestamp"] != nil {
		t.Fatalf("%+v %v", recs, err)
	}
	split := SnapshotFetch("fake", "tiers", clock, SnapshotSpec{Mode: SnapshotSplit,
		Fetch: func(context.Context, string) ([]byte, error) { return []byte(`[1,2]`), nil },
		Split: func([]byte) ([]SnapshotRow, error) {
			return []SnapshotRow{{"A", []byte(`1`)}, {"B", []byte(`2`)}}, nil
		}})
	recs, err = split(context.Background(), "")
	if err != nil || len(recs) != 2 || recs[1].Symbol != "B" || string(recs[1].Payload) != "2" {
		t.Fatalf("%+v %v", recs, err)
	}
	s, _ := NewScheduler([]Channel{{Venue: "fake", Name: "tiers", Daily: true, Fetch: bulk}}, &memorySink{}, clock, "", 240)
	s.Step(context.Background())
	if len(s.Sink.(*memorySink).records) != 1 {
		t.Fatal("no record")
	}
	clock.t = clock.t.Add(25 * time.Hour)
	s.Step(context.Background())
	if len(s.Sink.(*memorySink).records) != 2 {
		t.Fatal("snapshot was deduplicated")
	}
}
