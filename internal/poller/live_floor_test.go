package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLiveFloorStartRestartMidnightAndFinalization(t *testing.T) {
	for _, mode := range []PageMode{PageWindows, PageForward, PageBackward} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			start := time.Date(2026, 11, 15, 23, 57, 0, 0, time.UTC)
			clock := &fakeClock{start}
			path := filepath.Join(t.TempDir(), "state.json")
			s, err := NewScheduler(nil, &memorySink{}, clock, path, 240)
			if err != nil {
				t.Fatal(err)
			}
			// The floor must be on disk before any fetch, even if every fetch fails.
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var persisted struct {
				Start int64 `json:"recording_start_ms"`
			}
			if err := json.Unmarshal(b, &persisted); err != nil {
				t.Fatal(err)
			}
			if persisted.Start != 1794787020000 {
				t.Fatalf("persisted floor = %d", persisted.Start)
			}
			var from int64
			h := &History{Venue: "v", Channel: "h", Symbol: "BTC", Clock: clock, Sched: s,
				Lookback: 72 * time.Hour, Overlap: 2 * time.Hour, Step: 5 * time.Minute, Mode: mode, Window: 400,
				Fetch: func(_ context.Context, a, b int64) ([]Row, error) {
					from = a
					// Deliberately ignore query bounds to exercise local filtering.
					return []Row{{TS: 1794786900000, Raw: []byte(`{"v":1}`)}, // 23:55: before recording
						{TS: 1794787020000, Raw: []byte(`{"v":2}`)},      // inclusive exact floor
						{TS: 1794787200000, Raw: []byte(`{"v":3}`)}}, nil // 00:00 next day
				}}
			recs, err := h.Run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(recs) != 0 || from != 1794787020000 {
				t.Fatalf("initial records=%v from=%d", recs, from)
			}
			if s.NoDataSkip("v", "h", "BTC") {
				t.Fatal("live initial range suppressed")
			}
			clock.t = time.Date(2026, 11, 16, 0, 10, 0, 0, time.UTC)
			s, err = NewScheduler(nil, &memorySink{}, clock, path, 240)
			if err != nil {
				t.Fatal(err)
			}
			h.Sched = s
			recs, err = h.Run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(recs) != 2 || from != 1794787020000 {
				t.Fatalf("restart records=%v from=%d", recs, from)
			}
			if recs[0].Meta["last_row_timestamp"] != int64(1794787020000) || recs[1].Meta["last_row_timestamp"] != int64(1794787200000) {
				t.Fatalf("wrong rows: %v", recs)
			}
			// Cursor overlap must not widen recording scope; repeat/revise still dedups.
			if s.dedup(&recs[0]) || !s.dedup(&recs[0]) {
				t.Fatal("duplicate handling")
			}
			recs[0].Payload = []byte(`{"v":4}`)
			if s.dedup(&recs[0]) || recs[0].Meta["revised"] != true {
				t.Fatal("revision handling")
			}
			s.state.Jobs["v/h/BTC"] = jobState{RowTimestamp: 1794787200000}
			if err := s.save(); err != nil {
				t.Fatal(err)
			}
			s, err = NewScheduler(nil, &memorySink{}, clock, path, 240)
			if err != nil {
				t.Fatal(err)
			}
			h.Sched = s
			if _, err = h.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			if from != 1794787020000 {
				t.Fatalf("cursor escaped floor: %d", from)
			}
		})
	}
}

func TestLiveFloorLegacyMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte(`{"jobs":{"v/h/old":{"row_timestamp":1794783600000,"last_success":123},"v/h/empty":{"no_data_at":456}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{time.Date(2026, 11, 15, 23, 57, 0, 0, time.UTC)}
	s, err := NewScheduler(nil, &memorySink{}, clock, path, 240)
	if err != nil {
		t.Fatal(err)
	}
	if s.RecordingFloor("v", "h", "old") != 0 || s.RowTimestamp("v", "h", "old") != 1794783600000 {
		t.Fatal("legacy cursor constrained")
	}
	if s.state.Jobs["v/h/old"].LastSuccess != 123 || s.state.Jobs["v/h/empty"].NoDataAt != 456 {
		t.Fatal("migration erased legacy state")
	}
	if s.RecordingFloor("v", "h", "empty") != 1794787020000 || s.RecordingFloor("v", "h", "new") != 1794787020000 {
		t.Fatal("cursorless migration scope")
	}
	clock.t = clock.t.Add(24 * time.Hour)
	s, err = NewScheduler(nil, &memorySink{}, clock, path, 240)
	if err != nil {
		t.Fatal(err)
	}
	if s.RecordingFloor("v", "h", "old") != 0 || s.RecordingFloor("v", "h", "empty") != 1794787020000 {
		t.Fatal("migration changed on restart")
	}
	var from int64
	h := History{Venue: "v", Channel: "h", Symbol: "old", Clock: clock, Sched: s, Overlap: time.Hour, Mode: PageBackward,
		Fetch: func(_ context.Context, a, b int64) ([]Row, error) { from = a; return nil, nil }}
	if _, err := h.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if from != 1794780000000 {
		t.Fatalf("legacy overlap start = %d", from)
	}
}

func TestLiveFloorEmptyRangeVersusUnavailable(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(fmt.Sprint(unavailable), func(t *testing.T) {
			clock := &fakeClock{time.Date(2026, 11, 15, 23, 57, 0, 0, time.UTC)}
			s, err := NewScheduler(nil, &memorySink{}, clock, "", 240)
			if err != nil {
				t.Fatal(err)
			}
			h := History{Venue: "v", Channel: "h", Symbol: "BTC", Clock: clock, Sched: s, Mode: PageBackward,
				Fetch: func(context.Context, int64, int64) ([]Row, error) {
					if unavailable {
						return nil, ErrHistoryUnavailable
					}
					return nil, nil
				}}
			recs, err := h.Run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if s.NoDataSkip("v", "h", "BTC") != unavailable {
				t.Fatalf("cooldown=%v", s.NoDataSkip("v", "h", "BTC"))
			}
			if unavailable && len(recs) != 1 {
				t.Fatal("missing upstream absence record")
			}
			if !unavailable && len(recs) != 0 {
				t.Fatal("empty live range is not upstream absence")
			}
		})
	}
}

func TestInitialLookbackZeroOverridesNonzero(t *testing.T) {
	if got := initialLookback("0s", "72h"); got != 0 {
		t.Fatalf("zero fell back: %v", got)
	}
}
