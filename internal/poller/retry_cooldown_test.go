package poller

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type cooldownDoer func(*http.Request) (*http.Response, error)

func (f cooldownDoer) Do(r *http.Request) (*http.Response, error) { return f(r) }

func TestSchedulerRetryAfterStartsAtSlowResponse(t *testing.T) {
	for _, dateHeader := range []bool{false, true} {
		t.Run(map[bool]string{false: "seconds", true: "date"}[dateHeader], func(t *testing.T) {
			start := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
			clock := &fakeClock{start}
			calls := 0
			e := &Env{Venue: "slow-retry", Clock: clock, BaseURL: "https://unused.invalid", RPS: 1}
			e.Doer = cooldownDoer(func(r *http.Request) (*http.Response, error) {
				calls++
				clock.t = clock.t.Add(10 * time.Second)
				h := http.Header{"Retry-After": {"30"}}
				if dateHeader {
					h.Set("Retry-After", start.Add(40*time.Second).Format(http.TimeFormat))
				}
				return &http.Response{StatusCode: http.StatusTooManyRequests, Header: h, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
			})
			ch := e.Channel("rate-limited", ChannelSpec{PerSymbol: true, Period: time.Hour, Fetch: func(ctx context.Context, symbol string) ([]Record, error) {
				_, err := e.Get(ctx, "/limited", nil)
				return nil, err
			}})
			s, err := NewScheduler([]Channel{ch}, &memorySink{}, clock, "", 240)
			if err != nil {
				t.Fatal(err)
			}
			s.SetSymbols([]string{"A", "B"})
			if _, err := s.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			deadline := start.Add(40 * time.Second)
			if got := s.blocked["slow-retry/rate-limited"]; !got.Equal(deadline) {
				t.Fatalf("endpoint cooldown = %s, want %s", got, deadline)
			}
			if got := s.failures[jobKey(ch, "A")].next; !got.Equal(deadline) {
				t.Fatalf("retry = %s, want %s", got, deadline)
			}
			clock.t = deadline.Add(-time.Nanosecond)
			if _, err := s.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatal("retry or another symbol requested before response-relative cooldown")
			}
			clock.t = deadline
			if _, err := s.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			if calls != 2 {
				t.Fatal("retry not requested at cooldown deadline")
			}
		})
	}
}
