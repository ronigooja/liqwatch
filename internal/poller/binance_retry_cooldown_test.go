package poller

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestBinanceRetryAfterStartsAtSlowResponse(t *testing.T) {
	for _, header := range []string{"seconds", "date"} {
		t.Run(header, func(t *testing.T) {
			start := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
			clock := &fakeClock{start}
			calls := 0
			doer := cooldownDoer(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != http.MethodGet || r.URL.Path != "/fapi/v1/exchangeInfo" {
					t.Fatalf("unexpected Binance request: %s %s", r.Method, r.URL.Path)
				}
				clock.t = clock.t.Add(10 * time.Second)
				value := "30"
				if header == "date" {
					value = start.Add(40 * time.Second).Format(http.TimeFormat)
				}
				return &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": {value}}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
			})
			env, registered, err := NewVenue("binance", PollConfig{}, Deps{Doer: doer, Clock: clock})
			if err != nil {
				t.Fatal(err)
			}
			var channels []Channel
			for _, c := range registered {
				if c.Name == "exchangeInfo" {
					channels = append(channels, c)
				}
			}
			if len(channels) != 1 || channelDropped("binance", "exchangeInfo") {
				t.Fatal("expected actual supported exchangeInfo channel")
			}
			s, err := NewScheduler(channels, &memorySink{}, clock, "", 240)
			if err != nil {
				t.Fatal(err)
			}
			env.Bind(s)
			if _, err := s.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			deadline := start.Add(40 * time.Second)
			if got := s.blocked["binance/exchangeInfo"]; !got.Equal(deadline) {
				t.Fatalf("endpoint cooldown = %s, want %s", got, deadline)
			}
			if got := s.failures["binance/exchangeInfo/"].next; !got.Equal(deadline) {
				t.Fatalf("retry deadline = %s, want %s", got, deadline)
			}
			clock.t = deadline.Add(-time.Nanosecond)
			if _, err := s.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatal("requested before cooldown expired")
			}
			clock.t = deadline
			if _, err := s.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			if calls != 2 {
				t.Fatal("did not retry at exact response-relative deadline")
			}
		})
	}
}
