package poller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRunOnceExcludesPhasesAndRetriesAfterSweepStart(t *testing.T) {
	clock := &fakeClock{time.Date(2026, 11, 1, 23, 59, 30, 0, time.UTC)}
	var calls []string
	channels := []Channel{}
	for _, spec := range []struct {
		venue, name string
		history     bool
	}{{"a", "snapshot1", false}, {"a", "snapshot2", false}, {"b", "history", true}, {"c", "retry", false}} {
		v := spec
		channels = append(channels, Channel{Venue: v.venue, Name: v.name, Period: time.Hour, History: v.history, Fetch: func(context.Context, string) ([]Record, error) { calls = append(calls, v.name); return nil, nil }})
	}
	s, _ := NewScheduler(channels, &memorySink{}, clock, "", 0.6) // one request per 100 seconds
	s.failures["c/retry/"] = &failure{next: clock.Now().Add(10 * time.Second)}
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(calls) != "[snapshot1 snapshot2]" {
		t.Fatalf("sweep fetched newly eligible jobs: %v", calls)
	}
	if want := time.Date(2026, 11, 2, 0, 1, 10, 0, time.UTC); !clock.Now().Equal(want) {
		t.Fatalf("finish=%s want %s", clock.Now(), want)
	}
	// A retry already due at the next sweep's start remains eligible.
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	// snapshot1's midnight regular slot is also due at the second sweep start.
	if fmt.Sprint(calls) != "[snapshot1 snapshot2 retry history snapshot1]" {
		t.Fatalf("next sweep: %v", calls)
	}
}

func TestCapacityExactBoundaryDedupAndMembership(t *testing.T) {
	clock := &fakeClock{time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	sink := &memorySink{}
	c := Channel{Venue: "v", Name: "capacity", PerSymbol: true, Period: 20 * time.Second, RPS: 1}
	s, _ := NewScheduler([]Channel{c}, sink, clock, "", 60)
	syms := []string{}
	for i := 0; i < 20; i++ {
		syms = append(syms, fmt.Sprintf("S%02d", i))
	}
	check := func() { s.reportCapacity(s.schedule(syms), clock.Now()) }
	check()
	if len(sink.records) != 0 {
		t.Fatalf("exact capacity falsely warned: %v", sink.records)
	}
	syms = append(syms, "S20")
	check()
	check()
	if len(sink.records) != 1 {
		t.Fatalf("real overload/dedup warnings=%d", len(sink.records))
	}
	syms = syms[:20]
	check()
	if len(sink.records) != 1 {
		t.Fatal("return to capacity warned")
	}
	syms = append(syms, "S20")
	check()
	if len(sink.records) != 2 {
		t.Fatal("recurring overload not reported")
	}
}

func TestBybitActualPaginationUsesSharedWeightedPacing(t *testing.T) {
	clock := &fakeClock{time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	start := clock.Now()
	var times []time.Time
	e := &Env{Venue: "bybit", Clock: clock, BaseURL: "https://unused.invalid", RPS: 100}
	e.Doer = cooldownDoer(func(r *http.Request) (*http.Response, error) {
		times = append(times, clock.Now())
		cursor := "next"
		if len(times) == 2 {
			if r.URL.Query().Get("cursor") != "next" {
				t.Fatal("missing cursor")
			}
			cursor = ""
		}
		body := fmt.Sprintf(`{"retCode":0,"retMsg":"OK","result":{"list":[],"nextPageCursor":%q}}`, cursor)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	c := bybitSnapshot(e, "riskLimit", "/v5/market/risk-limit", false)
	c.Weight = 2
	s, _ := NewScheduler([]Channel{c}, &memorySink{}, clock, "", 6)
	e.Scheduler = s
	if _, err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(times) != 2 || !times[0].Equal(start) || !times[1].Equal(start.Add(20*time.Second)) {
		t.Fatalf("actual page starts %v", times)
	}
	if d := s.available(c, clock.Now()); d != 20*time.Second {
		t.Fatalf("second page bypassed budget: wait %s", d)
	}
}

// All expectations below are fixed clock/grid arithmetic, not production phase calculations.
func TestUniformHistoryPhasesRestartAndFloor(t *testing.T) {
	start := time.Date(2026, 11, 1, 23, 37, 0, 0, time.UTC)
	clock := &fakeClock{start}
	var calls []time.Time
	channels := []Channel{}
	for _, name := range []string{"a", "b"} {
		channels = append(channels, Channel{Venue: "v", Name: name, History: true, PerSymbol: true, Period: time.Hour,
			Fetch: func(context.Context, string) ([]Record, error) { calls = append(calls, clock.Now()); return nil, nil }})
	}
	path := t.TempDir() + "/state.json"
	s, err := NewScheduler(channels, &memorySink{}, clock, path, 240)
	if err != nil {
		t.Fatal(err)
	}
	s.SetSymbols([]string{"C", "A", "B"})
	ctx := context.Background()
	for clock.Now().Before(start.Add(2 * time.Hour)) {
		d, err := s.Step(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if d > 0 {
			clock.t = clock.t.Add(d)
		}
		if len(calls) == 6 {
			// Restart mid-cycle, with reversed config order and persisted universe.
			channels[0], channels[1] = channels[1], channels[0]
			s, err = NewScheduler(channels, &memorySink{}, clock, path, 240)
			if err != nil {
				t.Fatal(err)
			}
			if s.RecordingFloor("v", "a", "A") != start.UnixMilli() {
				t.Fatal("floor moved")
			}
		}
	}
	if len(calls) != 12 {
		t.Fatalf("calls=%d want 12", len(calls))
	}
	first := time.Date(2026, 11, 1, 23, 40, 0, 0, time.UTC)
	for i, at := range calls {
		if want := first.Add(time.Duration(i) * 10 * time.Minute); !at.Equal(want) {
			t.Fatalf("call %d=%s want %s", i, at, want)
		}
	}
}

func TestImmediateSnapshotsThenPhasedWithinPeriod(t *testing.T) {
	clock := &fakeClock{time.Date(2026, 11, 1, 23, 59, 37, 0, time.UTC)}
	start := clock.Now()
	times := map[string][]time.Time{}
	c := Channel{Venue: "v", Name: "snapshot", PerSymbol: true, Period: time.Minute,
		Fetch: func(_ context.Context, sym string) ([]Record, error) {
			times[sym] = append(times[sym], clock.Now())
			return nil, nil
		}}
	s, _ := NewScheduler([]Channel{c}, &memorySink{}, clock, "", 240)
	s.SetSymbols([]string{"C", "B", "A"})
	for clock.Now().Before(start.Add(3 * time.Minute)) {
		d, err := s.Step(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if d > 0 {
			clock.t = clock.t.Add(d)
		}
	}
	for _, sym := range []string{"A", "B", "C"} {
		got := times[sym]
		if len(got) < 3 || got[0].Sub(start) > time.Second {
			t.Fatalf("startup %s: %v", sym, got)
		}
		for i := 1; i < len(got); i++ {
			if gap := got[i].Sub(got[i-1]); gap > time.Minute {
				t.Fatalf("%s gap %s", sym, gap)
			}
		}
	}
	if !times["A"][1].Equal(time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)) || !times["B"][1].Equal(time.Date(2026, 11, 2, 0, 0, 20, 0, time.UTC)) || !times["C"][1].Equal(time.Date(2026, 11, 1, 23, 59, 40, 0, time.UTC)) {
		t.Fatalf("phases: %v", times)
	}
}

func TestWeightedPacingNoIdleCreditAndPagination(t *testing.T) {
	clock := &fakeClock{time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	var starts []time.Time
	var s *Scheduler
	c := Channel{Venue: "v", Name: "pages", Weight: 2, Fetch: func(ctx context.Context, _ string) ([]Record, error) {
		for i := 0; i < 4; i++ {
			if i > 0 {
				if err := s.waitRequest(ctx, "v", "pages"); err != nil {
					return nil, err
				}
			}
			starts = append(starts, clock.Now())
		}
		return nil, nil
	}}
	s, _ = NewScheduler([]Channel{c}, &memorySink{}, clock, "", 6)
	if _, err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i, at := range starts {
		if want := starts[0].Add(time.Duration(i) * 20 * time.Second); !at.Equal(want) {
			t.Fatalf("page %d: %s want %s", i, at, want)
		}
	}
	clock.t = clock.t.Add(24 * time.Hour)
	if err := s.waitRequest(context.Background(), "v", "pages"); err != nil {
		t.Fatal(err)
	}
	if d := s.available(c, clock.Now()); d != 20*time.Second {
		t.Fatalf("banked idle credit: %s", d)
	}
	for i := 0; i < 20; i++ {
		clock.t = clock.t.Add(time.Second)
		if i < 19 && s.available(c, clock.Now()) <= 0 {
			t.Fatal("early refill")
		}
	}
}

func TestRetryDeadlineFairnessAndMissedSlotsCoalesce(t *testing.T) {
	t.Run("backoff-boundary", func(t *testing.T) {
		clock := &fakeClock{time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
		start := clock.Now()
		calls := 0
		c := Channel{Venue: "v", Name: "backoff", Period: time.Minute, Fetch: func(context.Context, string) ([]Record, error) {
			calls++
			return nil, errors.New("network")
		}}
		s, _ := NewScheduler([]Channel{c}, &memorySink{}, clock, "", 240)
		s.Step(context.Background())
		clock.t = start.Add(time.Second - time.Nanosecond)
		s.Step(context.Background())
		if calls != 1 {
			t.Fatal("early backoff retry")
		}
		clock.t = start.Add(time.Second)
		s.Step(context.Background())
		if calls != 2 {
			t.Fatal("missed inclusive backoff boundary")
		}
	})
	clock := &fakeClock{time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	var calls []string
	channels := []Channel{}
	for _, name := range []string{"a", "b", "c"} {
		n := name
		channels = append(channels, Channel{Venue: "v", Name: n, Period: time.Minute, Fetch: func(context.Context, string) ([]Record, error) {
			calls = append(calls, n)
			if n == "a" {
				return nil, &RetryError{errors.New("429"), 30 * time.Second}
			}
			return nil, nil
		}})
	}
	s, _ := NewScheduler(channels, &memorySink{}, clock, "", 60)
	for i := 0; i < 3; i++ {
		if _, err := s.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		clock.t = clock.t.Add(time.Second)
	}
	if fmt.Sprint(calls) != "[a b c]" {
		t.Fatalf("regular starvation %v", calls)
	}
	clock.t = time.Date(2026, 11, 1, 0, 0, 28, 0, time.UTC)
	s.Step(context.Background())
	// b's regular :20 phase is due; the failed endpoint a must remain blocked.
	if len(calls) != 4 || calls[3] != "b" {
		t.Fatal("early retry")
	}
	clock.t = clock.t.Add(time.Second)
	s.Step(context.Background())
	if len(calls) != 4 {
		t.Fatal("retry crossed cooldown boundary")
	}
	clock.t = clock.t.Add(time.Second)
	s.Step(context.Background())
	if len(calls) != 5 || calls[4] != "a" {
		t.Fatal("retry boundary")
	}
	clock.t = clock.t.Add(10 * time.Hour)
	for i := 0; i < 3; i++ {
		s.Step(context.Background())
		clock.t = clock.t.Add(time.Second)
	}
	n := len(calls)
	s.Step(context.Background())
	if len(calls) != n {
		t.Fatal("missed periods caught up in burst")
	}
}

func TestDailyEligibilityBoundaryAndNonpositiveCadence(t *testing.T) {
	clock := &fakeClock{time.Date(2026, 11, 1, 0, 4, 0, 0, time.UTC)}
	s, _ := NewScheduler([]Channel{{Venue: "v", Name: "daily", Daily: true, History: true, PerSymbol: true}}, &memorySink{}, clock, "", 240)
	s.SetSymbols([]string{"B", "A"})
	jobs := s.schedule(s.Symbols())
	for i, j := range jobs {
		want := time.Date(2026, 11, 1, 0, 5, 0, 0, time.UTC).Add(time.Duration(i) * 12 * time.Hour)
		if !j.deadline.Equal(want) {
			t.Fatalf("daily %d %s want %s", i, j.deadline, want)
		}
	}
	clock.t = time.Date(2026, 11, 1, 0, 5, 0, 0, time.UTC)
	s.state.RecordingStartMS = clock.Now().UnixMilli()
	if got := s.schedule(s.Symbols())[0].deadline; !got.Equal(clock.Now()) {
		t.Fatalf("inclusive first boundary: %s", got)
	}
	s.state.Jobs["v/daily/A"] = jobState{LastSuccess: clock.Now().UnixNano()}
	for _, j := range s.schedule(s.Symbols()) {
		if j.sym == "A" && !j.deadline.Equal(clock.Now().Add(24*time.Hour)) {
			t.Fatal("success repeated same boundary")
		}
	}
	for _, p := range []time.Duration{0, -time.Second} {
		calls := 0
		c := Channel{Venue: "v", Name: "once", Period: p, Fetch: func(context.Context, string) ([]Record, error) { calls++; return nil, nil }}
		x, _ := NewScheduler([]Channel{c}, &memorySink{}, clock, "", 240)
		x.Step(context.Background())
		clock.t = clock.t.Add(time.Hour)
		x.Step(context.Background())
		if calls != 1 {
			t.Fatalf("nonpositive period %s calls %d", p, calls)
		}
	}
}

func TestCapacityIsReportedWithoutChangingPeriod(t *testing.T) {
	clock := &fakeClock{time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	sink := &memorySink{}
	c := Channel{Venue: "v", Name: "too-many", PerSymbol: true, Period: time.Second, RPS: 1, Weight: 2, Fetch: func(context.Context, string) ([]Record, error) { return nil, nil }}
	s, _ := NewScheduler([]Channel{c}, sink, clock, "", 60)
	s.SetSymbols([]string{"A", "B"})
	s.Step(context.Background())
	if len(sink.records) != 1 || !strings.Contains(string(sink.records[0].Payload), "schedule_over_capacity") {
		t.Fatalf("missing capacity warning: %v", sink.records)
	}
	if s.Channels[0].Period != time.Second {
		t.Fatal("period changed")
	}
}

func TestPhasedHistoryFetchRetainsEarliestRecordingData(t *testing.T) {
	clock := &fakeClock{time.Date(2026, 11, 1, 0, 7, 0, 0, time.UTC)}
	floor := clock.Now().UnixMilli()
	var s *Scheduler
	var from int64
	c := Channel{Venue: "v", Name: "history", History: true, PerSymbol: true, Period: time.Hour,
		Fetch: func(ctx context.Context, sym string) ([]Record, error) {
			h := &History{Venue: "v", Channel: "history", Symbol: sym, Clock: clock, Sched: s,
				Step: time.Minute, Mode: PageForward, Fetch: func(_ context.Context, start, end int64) ([]Row, error) {
					from = start
					return []Row{{TS: floor, Raw: []byte(`{"value":1}`)}}, nil
				}}
			return h.Run(ctx)
		}}
	sink := &memorySink{}
	s, _ = NewScheduler([]Channel{c}, sink, clock, "", 240)
	s.SetSymbols([]string{"A", "B"})
	clock.t = time.Date(2026, 11, 1, 0, 30, 0, 0, time.UTC)
	if _, err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if from != floor || s.RowTimestamp("v", "history", "B") != floor {
		t.Fatalf("delayed first history lost floor: request %d cursor %d want %d", from, s.RowTimestamp("v", "history", "B"), floor)
	}
}
