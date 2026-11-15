package poller

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// withoutPolicies disables policies for a non-parallel test and its subtests.
func withoutPolicies(t *testing.T) {
	t.Helper()
	previous := policiesDisabled
	policiesDisabled = true
	t.Cleanup(func() { policiesDisabled = previous })
}

// assertVenueGolden compares all testdata/<venue>/*.json snapshots with the
// filtered golden. GENERATE_VENUE_GOLDEN=<venue> creates or replaces it.
func assertVenueGolden(t *testing.T, venue string) {
	t.Helper()
	path := filepath.Join("testdata", venue, "golden-filtered.txt")
	generate := os.Getenv("GENERATE_VENUE_GOLDEN") == venue
	var want []byte
	if !generate {
		var err error
		want, err = os.ReadFile(path)
		if os.IsNotExist(err) {
			t.Skipf("filtered golden absent: %s", path)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	got := fixtureGolden(t, venue)
	if generate {
		if err := os.WriteFile(path, got, 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("filtered golden differs: %s", path)
	}
}

// venueDedupFixture contains corresponding history rows with identical timestamps:
// Dropped changes only a dropped field and Kept changes only a retained field.
type venueDedupFixture struct {
	Original, Dropped, Kept []Record
}

// assertVenueDedup exercises a registered policy across original, identical,
// dropped-field and kept-field fetches. Each slice must contain one history row.
func assertVenueDedup(t *testing.T, venue, channel string, fixture venueDedupFixture) {
	t.Helper()
	policies.RLock()
	_, registered := policies.m[policyKey{venue, channel}]
	policies.RUnlock()
	if !registered || policiesDisabled {
		t.Fatal("venue policy must be registered and enabled")
	}
	if len(fixture.Original) != 1 || len(fixture.Dropped) != 1 || len(fixture.Kept) != 1 {
		t.Fatal("each fixture variant must contain one row")
	}
	base, dropped, kept := fixture.Original[0], fixture.Dropped[0], fixture.Kept[0]
	baseTS, validTS := base.Meta["last_row_timestamp"].(int64)
	if !validTS || baseTS <= 0 || dropped.Meta["last_row_timestamp"] != baseTS || kept.Meta["last_row_timestamp"] != baseTS {
		t.Fatal("fixture rows must have the same positive int64 timestamp")
	}
	for _, r := range []Record{base, dropped, kept} {
		if r.Venue != venue || r.Channel != channel || r.Symbol != base.Symbol {
			t.Fatal("fixture rows must identify the same venue, channel and symbol")
		}
	}
	filtered := make([][]byte, 3)
	for i, r := range []Record{base, dropped, kept} {
		var record bool
		var err error
		filtered[i], record, err = ApplyPolicy(venue, channel, r.Payload)
		if err != nil || !record {
			t.Fatalf("invalid fixture policy output: record=%v err=%v", record, err)
		}
	}
	if bytes.Equal(base.Payload, dropped.Payload) || !bytes.Equal(filtered[0], filtered[1]) || bytes.Equal(filtered[0], filtered[2]) {
		t.Fatal("fixture must change a dropped field, then a kept field")
	}
	clock := &fakeClock{time.UnixMilli(baseTS).Add(time.Hour)}
	sink := &memorySink{}
	fetches := [][]Record{fixture.Original, fixture.Original, fixture.Dropped, fixture.Kept}
	n := 0
	c := Channel{Venue: venue, Name: channel, Period: time.Hour, Fetch: func(context.Context, string) ([]Record, error) {
		rows := fetches[n]
		n++
		// Scheduler adds revision metadata, so do not mutate the fixture.
		copyRows := make([]Record, len(rows))
		for i, r := range rows {
			copyRows[i] = r
			copyRows[i].Meta = make(map[string]any, len(r.Meta))
			for k, v := range r.Meta {
				copyRows[i].Meta[k] = v
			}
		}
		return copyRows, nil
	}}
	s, err := NewScheduler([]Channel{c}, sink, clock, "", 240)
	if err != nil {
		t.Fatal(err)
	}
	for range fetches {
		if _, err := s.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		clock.t = clock.t.Add(25 * time.Hour)
	}
	if n != 4 || len(sink.records) != 2 || sink.records[0].Meta["revised"] != nil || sink.records[1].Meta["revised"] != true || s.Duplicates.Load() != 2 || s.Revisions.Load() != 1 {
		t.Fatalf("dedup: fetches=%d records=%+v duplicates=%d revisions=%d", n, sink.records, s.Duplicates.Load(), s.Revisions.Load())
	}
}

func TestPolicyHelpers(t *testing.T) {
	const venue, channel = "policy-helper-test", "history"
	if err := RegisterPolicy(venue, channel, Policy{Keep: []string{"ts", "kept"}, Drop: []string{"dropped"}, TimestampKey: "ts"}); err != nil {
		t.Fatal(err)
	}
	row := func(payload string) []Record {
		return []Record{{Venue: venue, Channel: channel, Meta: map[string]any{"last_row_timestamp": int64(42)}, Payload: []byte(payload)}}
	}
	assertVenueDedup(t, venue, channel, venueDedupFixture{
		Original: row(`{"ts":42,"kept":1,"dropped":1}`),
		Dropped:  row(`{"ts":42,"kept":1,"dropped":2}`),
		Kept:     row(`{"ts":42,"kept":2,"dropped":1}`),
	})
	t.Run("disabled", func(t *testing.T) {
		withoutPolicies(t)
		raw := []byte(`{"ts":42,"kept":1,"dropped":1}`)
		got, record, err := ApplyPolicy(venue, channel, raw)
		if err != nil || !record || !bytes.Equal(got, raw) {
			t.Fatal("policies not disabled")
		}
	})
	if got, record, err := ApplyPolicy(venue, channel, []byte(`{"ts":42,"dropped":1}`)); err != nil || !record || string(got) != `{"ts":42}` || policiesDisabled {
		t.Fatal("policies not restored")
	}
	// The missing golden must skip before attempting to load nonexistent fixtures.
	ok := t.Run("missing golden", func(t *testing.T) { assertVenueGolden(t, "policy-no-such-venue") })
	if !ok {
		t.Fatal("missing golden must skip")
	}
}
