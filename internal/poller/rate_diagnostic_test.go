package poller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestRateDiagnosticsHTTPPaths(t *testing.T) {
	for _, binance := range []bool{false, true} {
		for _, retry := range []struct {
			name, raw, kind string
			parsed, delay   time.Duration
		}{
			{"seconds", "7.5", "seconds", 7500 * time.Millisecond, 7500 * time.Millisecond},
			{"date", "Sun, 01 Nov 2026 12:00:40 GMT", "date", 30 * time.Second, 30 * time.Second},
			{"invalid", "invalid", "default", time.Minute, time.Minute},
			{"missing", "", "default", time.Minute, time.Minute},
			{"minimum", "0", "seconds", 0, time.Second},
		} {
			t.Run(map[bool]string{false: "env", true: "binance"}[binance]+"/"+retry.name, func(t *testing.T) {
				start := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
				clock := &fakeClock{start}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Retry-After", retry.raw)
					w.Header().Set("Set-Cookie", "hidden-cookie")
					w.WriteHeader(429)
					io.WriteString(w, `{"code":-1003,"msg":"too many requests","token":"body-secret","nested":{"api_key":"key-secret"},"echo":"query-secret auth-secret cookie-secret","url":"https://user:password@example.com/limited?private=other-secret"}`)
				}))
				defer server.Close()
				doer := cooldownDoer(func(r *http.Request) (*http.Response, error) {
					clock.t = start.Add(10 * time.Second)
					r.Header.Set("Authorization", "Bearer auth-secret")
					r.Header.Set("Cookie", "sid=cookie-secret")
					return server.Client().Do(r)
				})
				var err error
				if binance {
					b := &Binance{Config: PollConfig{BracketsURL: server.URL + "/limited?private=query-secret"}, Doer: doer, Clock: clock}
					_, err = b.requestPage(context.Background(), "brackets", "", 0, 0)
				} else {
					e := &Env{BaseURL: server.URL, Doer: doer, Clock: clock}
					_, err = e.Get(context.Background(), "/limited", url.Values{"private": {"query-secret"}})
				}
				var re *RetryError
				if !errors.As(err, &re) || re.After != retry.delay {
					t.Fatalf("retry = %#v, err = %v", re, err)
				}
				var d *RateDiagnostic
				if !errors.As(re.Err, &d) {
					t.Fatalf("missing diagnostic: %v", err)
				}
				if d.Method != map[bool]string{false: "GET", true: "POST"}[binance] || d.Path != "/limited" || d.Status != 429 || !d.RequestStarted.Equal(start) || !d.ResponseReceived.Equal(start.Add(10*time.Second)) || d.RetryAfterRaw != retry.raw || d.RetryAfterKind != retry.kind || d.ParsedDelay != retry.parsed || d.Cooldown != retry.delay || !d.CooldownUntil.Equal(clock.t.Add(retry.delay)) {
					t.Fatalf("incorrect diagnostic: %v", d)
				}
				for _, secret := range []string{"body-secret", "key-secret", "query-secret", "auth-secret", "cookie-secret", "hidden-cookie", "other-secret", "private=", "https://", "Authorization", "Cookie"} {
					if strings.Contains(err.Error(), secret) {
						t.Fatalf("leaked %q: %v", secret, err)
					}
				}
				if !strings.Contains(d.Body, "too many requests") || !strings.Contains(d.Body, "-1003") {
					t.Fatalf("lost useful body: %v", d)
				}
				if retry.name == "seconds" && !binance {
					t.Logf("sanitized sample: %s", err)
				}
			})
		}
	}
}

type countingRateBody struct{ n int }

func (b *countingRateBody) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	b.n += len(p)
	return len(p), nil
}
func (*countingRateBody) Close() error { return nil }

func TestRateDiagnosticBoundedAndPersisted(t *testing.T) {
	clock := &fakeClock{time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
	body := &countingRateBody{}
	e := &Env{BaseURL: "https://unused.invalid", Clock: clock, RPS: 1, Venue: "limited", Doer: cooldownDoer(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 418, Header: http.Header{"Retry-After": {"authorization=header-secret"}}, Body: body}, nil
	})}
	ch := e.Channel("limited", ChannelSpec{Fetch: func(ctx context.Context, _ string) ([]Record, error) {
		_, err := e.Get(ctx, "/limited?token=query-secret", nil)
		return nil, err
	}})
	sink := &memorySink{}
	s, err := NewScheduler([]Channel{ch}, sink, clock, "", 240)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if body.n != rateBodyLimit+1 {
		t.Fatalf("body read %d bytes", body.n)
	}
	if len(sink.records) != 1 || sink.records[0].Channel != "gap" {
		t.Fatalf("missing gap: %+v", sink.records)
	}
	var gap struct {
		Err string `json:"err"`
	}
	if err := json.Unmarshal(sink.records[0].Payload, &gap); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gap.Err, `"body_truncated":true`) || !strings.Contains(gap.Err, `"status":418`) || !strings.Contains(gap.Err, `"cooldown_ns":60000000000`) || strings.Contains(gap.Err, "header-secret") || strings.Contains(gap.Err, "query-secret") {
		t.Fatalf("bad persisted diagnostic: %s", gap.Err)
	}
}

type failingRateBody struct{}

func (failingRateBody) Read([]byte) (int, error) {
	return 0, errors.New("read error containing secret-data")
}
func (failingRateBody) Close() error { return nil }

func TestRateDiagnosticBodySafetyBothPaths(t *testing.T) {
	for _, binance := range []bool{false, true} {
		for _, mode := range []string{"oversized", "read-error", "plain-text", "json"} {
			t.Run(map[bool]string{false: "env", true: "binance"}[binance]+"/"+mode, func(t *testing.T) {
				start := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
				clock := &fakeClock{start}
				large := &countingRateBody{}
				var body io.ReadCloser
				switch mode {
				case "oversized":
					body = large
				case "read-error":
					body = failingRateBody{}
				case "plain-text":
					body = io.NopCloser(strings.NewReader("credential=secret-data Authorization: Bearer secret-data"))
				default:
					body = io.NopCloser(strings.NewReader(`{"items":[{"cookie":"secret-data"},"token=secret-data"],"echo":123456789,"msg":"/path?echo=secret-data"}`))
				}
				doer := cooldownDoer(func(r *http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: 418, Header: http.Header{"Retry-After": {"token=secret-data"}}, Body: body}, nil
				})
				var err error
				if binance {
					b := &Binance{Config: PollConfig{BracketsURL: "https://unused.invalid/limited?private=123456789"}, Doer: doer, Clock: clock}
					_, err = b.requestPage(context.Background(), "brackets", "", 0, 0)
				} else {
					e := &Env{BaseURL: "https://unused.invalid", Doer: doer, Clock: clock}
					_, err = e.Get(context.Background(), "/limited?private=123456789", nil)
				}
				var retry *RetryError
				if !errors.As(err, &retry) || retry.After != time.Minute {
					t.Fatalf("lost retry behavior: %v", err)
				}
				d := retry.Err.(*RateDiagnostic)
				if d.BodyReadError != (mode == "read-error") || d.BodyTruncated != (mode == "oversized") {
					t.Fatalf("wrong capture metadata: %v", d)
				}
				if mode == "oversized" && large.n != rateBodyLimit+1 {
					t.Fatalf("unbounded read: %d", large.n)
				}
				if strings.Contains(err.Error(), "secret-data") || strings.Contains(err.Error(), "123456789") {
					t.Fatalf("sensitive response leaked: %v", err)
				}
			})
		}
	}
}
