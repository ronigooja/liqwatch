package poller

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestDedupRevisionReturnsToEarlierValue(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	sink := &memorySink{}
	s, _ := NewScheduler(nil, sink, &fakeClock{now}, "", 240)
	record := func(payload string) Record {
		return Record{Venue: "v", Channel: "h", Symbol: "S", RecvWall: 7, RecvMono: 8, Meta: map[string]any{"last_row_timestamp": int64(123)}, Payload: []byte(payload)}
	}
	for _, p := range []string{`{"v":"A"}`, `{"v":"B"}`, `{"v":"A"}`, `{"v":"B"}`} {
		r := record(p)
		if !s.dedup(&r) {
			sink.Write(r)
		}
	}
	a, b := record(`{"v":"A"}`), record(`{"v":"B"}`)
	b.Meta["revised"] = true
	if want := []Record{a, b}; !reflect.DeepEqual(sink.records, want) {
		t.Fatalf("records=%#v want=%#v", sink.records, want)
	}
	if s.Duplicates.Load() != 2 || s.Revisions.Load() != 1 {
		t.Fatalf("duplicates=%d revisions=%d", s.Duplicates.Load(), s.Revisions.Load())
	}
}

func TestDedupRestartCursorWindow(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{now}
	dir := t.TempDir()
	cursor := now.Add(-time.Hour).UnixMilli()
	edge := cursor - (2 * time.Hour).Milliseconds()
	rec := func(ts int64) Record {
		return Record{Venue: "v", Symbol: "S", Channel: "h", RecvWall: now.UnixNano(), RecvMono: 7, Meta: map[string]any{"last_row_timestamp": ts}, Payload: []byte(fmt.Sprintf(`{"ts":%d}`, ts))}
	}
	disk := NewFileSink(dir, time.Hour)
	for _, ts := range []int64{edge - 1, edge, cursor, now.UnixMilli()} {
		disk.Write(rec(ts))
	}
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	disk = NewFileSink(dir, time.Hour)
	defer disk.Close()
	s, _ := NewScheduler(nil, disk, clock, "", 240)
	key := "v/h/S"
	s.state.Jobs[key] = jobState{RowTimestamp: cursor}
	s.overlaps[key] = 2 * time.Hour
	var emitted []Record
	for _, ts := range []int64{edge, cursor, now.UnixMilli(), edge - 1} {
		r := rec(ts)
		if !s.dedup(&r) {
			emitted = append(emitted, r)
		}
	}
	if want := []Record{rec(edge - 1)}; !reflect.DeepEqual(emitted, want) {
		t.Fatalf("records=%#v want=%#v", emitted, want)
	}
	if s.Duplicates.Load() != 3 || s.Revisions.Load() != 0 {
		t.Fatalf("duplicates=%d revisions=%d", s.Duplicates.Load(), s.Revisions.Load())
	}
}

func TestDedupBoundAcrossPolls(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{now}
	sink := &memorySink{}
	var s *Scheduler
	n := 0
	c := Channel{Venue: "v", Name: "h", History: true, Period: time.Minute, Fetch: func(context.Context, string) ([]Record, error) {
		n++
		s.overlaps["v/h/"] = 2 * time.Hour
		ts := now.UnixMilli() + int64(n)*60000
		return []Record{{Venue: "v", Channel: "h", RecvWall: 7, RecvMono: 8, Meta: map[string]any{"last_row_timestamp": ts}, Payload: []byte(fmt.Sprintf(`{"ts":%d}`, ts))}}, nil
	}}
	s, _ = NewScheduler([]Channel{c}, sink, clock, "", 240)
	var want []Record
	for i := 1; i <= 300; i++ {
		clock.t = now.Add(time.Duration(i) * time.Minute)
		if _, err := s.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		ts := now.UnixMilli() + int64(i)*60000
		want = append(want, Record{Venue: "v", Channel: "h", RecvWall: 7, RecvMono: 8, Meta: map[string]any{"last_row_timestamp": ts}, Payload: []byte(fmt.Sprintf(`{"ts":%d}`, ts))})
		if got := len(s.seen["v/h/"]); got > 121 {
			t.Fatalf("poll %d size=%d", i, got)
		}
	}
	if !reflect.DeepEqual(sink.records, want) {
		t.Fatalf("records differ: got=%d want=%d", len(sink.records), len(want))
	}
	if got := len(s.seen["v/h/"]); got != 121 {
		t.Fatalf("size=%d", got)
	}
}
