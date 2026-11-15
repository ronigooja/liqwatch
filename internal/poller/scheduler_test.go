package poller

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time                                 { return c.t }
func (c *fakeClock) Sleep(_ context.Context, d time.Duration) error { c.t = c.t.Add(d); return nil }

type memorySink struct{ records []Record }

func (s *memorySink) Write(r Record) { s.records = append(s.records, r) }
func TestSchedulerRetryAndResume(t *testing.T) {
	c := &fakeClock{time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)}
	sink := &memorySink{}
	calls := 0
	ch := Channel{Venue: "v", Name: "a", Period: time.Minute, Fetch: func(context.Context, string) ([]Record, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("failure")
		}
		return nil, nil
	}}
	path := t.TempDir() + "/state.json"
	s, _ := NewScheduler([]Channel{ch}, sink, c, path, 240)
	s.Step(context.Background())
	if len(sink.records) != 1 {
		t.Fatal("missing gap")
	}
	c.t = c.t.Add(time.Second)
	s.Step(context.Background())
	if len(sink.records) != 2 {
		t.Fatal("missing recovery")
	}
	s, _ = NewScheduler([]Channel{ch}, sink, c, path, 240)
	s.Step(context.Background())
	if calls != 2 {
		t.Fatal("checkpoint burst")
	}
}
func TestBudgetAndEndpointBlocking(t *testing.T) {
	c := &fakeClock{time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)}
	sink := &memorySink{}
	a, b := 0, 0
	channels := []Channel{{Venue: "v", Name: "a", Period: time.Minute, Fetch: func(context.Context, string) ([]Record, error) {
		a++
		return nil, &RetryError{errors.New("429"), time.Minute}
	}}, {Venue: "v", Name: "b", Period: time.Minute, Fetch: func(context.Context, string) ([]Record, error) { b++; return nil, nil }}}
	s, _ := NewScheduler(channels, sink, c, "", 2)
	s.Step(context.Background())
	s.Step(context.Background())
	if b != 0 {
		t.Fatal("startup budget burst")
	}
	c.t = c.t.Add(30 * time.Second)
	s.Step(context.Background())
	if a != 1 || b != 1 {
		t.Fatalf("a=%d b=%d", a, b)
	}
	c.t = c.t.Add(time.Second)
	channels[1].Name = "c"
	s.Channels = channels
	s.Step(context.Background())
	if b != 1 {
		t.Fatal("budget exceeded")
	}
}
