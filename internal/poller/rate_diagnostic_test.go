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
				if d.Method != map[bool]string{false: "GET", true: "POST"}[binance] || d.Path != "/limited" || d.Status != 429 || !d.RequestStarted.Equal(start) || !d.ResponseReceived.Equal(start.Add(10*time.Second)) || d.RetryAfterRaw != retry.raw || d.RetryAfterKind != retry.kind || d.ParsedDelay != retry.parsed || d.ResponseRetryDelay != retry.delay || !d.RetryAfterReferenceUntil.Equal(clock.t.Add(retry.delay)) {
					t.Fatalf("incorrect diagnostic: %v", d)
				}
				for _, secret := range []string{"body-secret", "key-secret", "query-secret", "auth-secret", "cookie-secret", "hidden-cookie", "other-secret", "private=", "https://", "Authorization", "Cookie"} {
					if strings.Contains(err.Error(), secret) {
						t.Fatalf("leaked %q: %v", secret, err)
					}
				}
				if !binance && (!strings.Contains(d.Body, "too many requests") || !strings.Contains(d.Body, "-1003")) {
					t.Fatalf("lost useful body: %v", d)
				}
				if binance && d.BodyOmittedReason != "request_body_present" {
					t.Fatalf("POST response captured: %v", d)
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
	if !strings.Contains(gap.Err, `"body_truncated":true`) || !strings.Contains(gap.Err, `"status":418`) || !strings.Contains(gap.Err, `"response_retry_delay_ns":60000000000`) || strings.Contains(gap.Err, "header-secret") || strings.Contains(gap.Err, "query-secret") {
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
				if d.BodyReadError != (!binance && mode == "read-error") || d.BodyTruncated != (!binance && mode == "oversized") {
					t.Fatalf("wrong capture metadata: %v", d)
				}
				if mode == "oversized" && ((!binance && large.n != rateBodyLimit+1) || (binance && large.n != 0)) {
					t.Fatalf("unbounded read: %d", large.n)
				}
				if strings.Contains(err.Error(), "secret-data") || strings.Contains(err.Error(), "123456789") {
					t.Fatalf("sensitive response leaked: %v", err)
				}
			})
		}
	}
}

func TestRateDiagnosticOmitsRequestBodyEchoFromGap(t *testing.T) {
	clock := &fakeClock{time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(429)
		io.WriteString(w, `{"echo":"opaque-value"}`)
	}))
	defer server.Close()
	e := &Env{BaseURL: server.URL, Doer: server.Client(), Clock: clock, RPS: 1, Venue: "limited"}
	ch := e.Channel("limited", ChannelSpec{Fetch: func(ctx context.Context, _ string) ([]Record, error) {
		_, err := e.Request(ctx, "POST", "/limited", nil, []byte(`{"password":"opaque-value"}`), nil)
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
	if len(sink.records) != 1 {
		t.Fatalf("missing gap: %+v", sink.records)
	}
	var gap struct {
		Err string `json:"err"`
	}
	if err := json.Unmarshal(sink.records[0].Payload, &gap); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(gap.Err, "opaque-value") || !strings.Contains(gap.Err, `"body_omitted_reason":"request_body_present"`) {
		t.Fatalf("unsafe persisted POST diagnostic: %s", gap.Err)
	}
	t.Logf("sanitized POST sample: %s", gap.Err)
}

func TestRateDiagnosticReferenceIsNotScheduledDeadline(t *testing.T) {
	clock := &fakeClock{time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
	e := &Env{BaseURL: "https://unused.invalid", Clock: clock, RPS: 1, Venue: "limited", Doer: cooldownDoer(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"1"}}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})}
	ch := e.Channel("limited", ChannelSpec{Fetch: func(ctx context.Context, _ string) ([]Record, error) {
		_, err := e.Get(ctx, "/limited", nil)
		return nil, err
	}})
	sink := &memorySink{}
	s, err := NewScheduler([]Channel{ch}, sink, clock, "", 240)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := s.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			clock.t = clock.t.Add(time.Second)
		}
	}
	if len(sink.records) != 2 {
		t.Fatalf("wanted repeated failures: %+v", sink.records)
	}
	var gap struct {
		Err string `json:"err"`
	}
	if err := json.Unmarshal(sink.records[1].Payload, &gap); err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(gap.Err, "HTTP rate limit: ")), &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"cooldown_ns", "cooldown_from", "cooldown_until", "scheduled_deadline"} {
		if _, ok := fields[key]; ok {
			t.Fatalf("false actual cooldown field %s: %s", key, gap.Err)
		}
	}
	until, err := time.Parse(time.RFC3339Nano, fields["retry_after_reference_until"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if fields["response_retry_delay_ns"] != float64(time.Second) || !until.Equal(clock.t.Add(time.Second)) || !s.failures[jobKey(ch, "")].next.Equal(clock.t.Add(2*time.Second)) {
		t.Fatalf("reference/schedule mismatch: %s, actual %s", gap.Err, s.failures[jobKey(ch, "")].next)
	}
}

func TestRateDiagnosticRedactsKnownPathValues(t *testing.T) {
	req, err := http.NewRequest("GET", "https://unused.invalid/opaque-value?x=opaque-value", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := diagnosticSanitizer(req)(req.URL.Path); strings.Contains(got, "opaque-value") {
		t.Fatalf("known credential remains in path: %s", got)
	}
}

func TestRateDiagnosticRequestBodyPresence(t *testing.T) {
	for _, mode := range []string{"unknown", "get-body", "nil", "no-body"} {
		t.Run(mode, func(t *testing.T) {
			req, err := http.NewRequest("POST", "https://unused.invalid/limited", nil)
			if err != nil {
				t.Fatal(err)
			}
			requestBody := &countingRateBody{}
			switch mode {
			case "unknown", "get-body":
				req.Body = requestBody
				req.ContentLength = -1
			case "no-body":
				req.Body = http.NoBody
			}
			req.GetBody = func() (io.ReadCloser, error) { t.Fatal("GetBody must not be called"); return nil, nil }
			if mode == "unknown" {
				req.GetBody = nil
			}
			responseBody := &countingRateBody{}
			clock := &fakeClock{time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
			err = rateLimitError(req, &http.Response{StatusCode: 429, Header: http.Header{}, Body: responseBody}, clock.t, clock.t, clock)
			d := err.(*RetryError).Err.(*RateDiagnostic)
			omit := mode == "unknown" || mode == "get-body"
			if (d.BodyOmittedReason == "request_body_present") != omit || requestBody.n != 0 || (omit && responseBody.n != 0) || (!omit && responseBody.n != rateBodyLimit+1) {
				t.Fatalf("unexpected capture: %v, request bytes %d response bytes %d", d, requestBody.n, responseBody.n)
			}
		})
	}
}
