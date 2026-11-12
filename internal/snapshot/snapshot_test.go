package snapshot

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/config"
)

type fakeDoer struct {
	mu    sync.Mutex
	calls []time.Time
	code  int
	head  http.Header
}

func (f *fakeDoer) Do(*http.Request) (*http.Response, error) {
	f.mu.Lock()
	f.calls = append(f.calls, time.Now())
	f.mu.Unlock()
	h := f.head
	if h == nil {
		h = make(http.Header)
	}
	return &http.Response{StatusCode: f.code, Header: h, Body: io.NopCloser(strings.NewReader(`{"lastUpdateId":1,"bids":[],"asks":[]}`))}, nil
}

func (f *fakeDoer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func cfgForTests() config.Snapshot {
	return config.Snapshot{Limit: 1000, VerifyEveryMin: 60, WeightFraction: 1}
}

func TestBudgetEnforcement(t *testing.T) {
	doer := &fakeDoer{code: http.StatusOK}
	s := New(cfgForTests(), []string{"BTCUSDT", "ETHUSDT"}, nil, 20, doer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(1200 * time.Millisecond)
	if got := doer.count(); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
}

func TestRateLimitBackoff(t *testing.T) {
	doer := &fakeDoer{code: http.StatusTooManyRequests, head: http.Header{"Retry-After": []string{"1"}}}
	s := New(cfgForTests(), []string{"BTCUSDT"}, nil, 20, doer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(1100 * time.Millisecond)
	if got := doer.count(); got != 1 {
		t.Fatalf("requests = %d, want 1 after Retry-After", got)
	}
	if got := s.Stats().RateLimited; got != 1 {
		t.Fatalf("rate limited = %d, want 1", got)
	}
}

func TestInitialStagger(t *testing.T) {
	doer := &fakeDoer{code: http.StatusOK}
	s := New(cfgForTests(), []string{"BTCUSDT", "ETHUSDT"}, nil, 40, doer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	time.Sleep(1200 * time.Millisecond)
	doer.mu.Lock()
	defer doer.mu.Unlock()
	if len(doer.calls) != 2 || doer.calls[1].Sub(doer.calls[0]) < 900*time.Millisecond {
		t.Fatalf("calls = %v, want two calls about one second apart", doer.calls)
	}
}

func TestTriggerDedupe(t *testing.T) {
	doer := &fakeDoer{code: http.StatusOK}
	s := New(cfgForTests(), []string{"BTCUSDT"}, nil, 40, doer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	deadline := time.Now().Add(time.Second)
	for doer.count() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	s.Trigger("BTCUSDT")
	s.Trigger("BTCUSDT")
	time.Sleep(100 * time.Millisecond)
	if got := doer.count(); got != 2 {
		t.Fatalf("requests = %d, want one initial and one deduplicated trigger", got)
	}
}
