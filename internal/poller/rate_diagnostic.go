package poller

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const rateBodyLimit = 4096

// RateDiagnostic is emitted only for HTTP 429/418. Error serializes it so the
// scheduler's existing gap error sink persists the same sanitized diagnostics.
// Paths must be static, non-sensitive endpoint paths: redaction removes known
// query/userinfo/sensitive-header values and sensitive keywords, but cannot
// identify arbitrary opaque credentials supplied only inside a path.
type RateDiagnostic struct {
	Method            string        `json:"method"`
	Path              string        `json:"path"`
	RequestStarted    time.Time     `json:"request_started"`
	ResponseReceived  time.Time     `json:"response_received"`
	Status            int           `json:"status"`
	Body              string        `json:"body"`
	BodyTruncated     bool          `json:"body_truncated"`
	BodyReadError     bool          `json:"body_read_error"`
	BodyOmittedReason string        `json:"body_omitted_reason,omitempty"`
	RetryAfterRaw     string        `json:"retry_after_raw"`
	RetryAfterKind    string        `json:"retry_after_kind"`
	ParsedDelay       time.Duration `json:"parsed_delay_ns"`
	// Response-derived references only; scheduler backoff and sink latency can
	// move the actual scheduled retry later. These are not scheduler deadlines.
	ResponseRetryDelay       time.Duration `json:"response_retry_delay_ns"`
	RetryAfterReferenceFrom  time.Time     `json:"retry_after_reference_from"`
	RetryAfterReferenceUntil time.Time     `json:"retry_after_reference_until"`
}

func (d *RateDiagnostic) Error() string {
	b, _ := json.Marshal(d)
	return "HTTP rate limit: " + string(b)
}

var sensitiveDiagnostic = regexp.MustCompile(`(?i)secret|token|auth|cookie|(?:api|access|private)[-_ ]?key|password|credential|signature|session`)
var diagnosticURL = regexp.MustCompile(`https?://[^\s"<>]+`)

// diagnosticSanitizer never serializes request headers, URLs or query strings.
// Known request values are also removed when a server echoes them in JSON.
func diagnosticSanitizer(req *http.Request) func(string) string {
	secrets := []string{}
	add := func(s string) {
		if s != "" {
			secrets = append(secrets, s, url.QueryEscape(s))
		}
	}
	for _, vs := range req.URL.Query() {
		for _, v := range vs {
			add(v)
		}
	}
	if req.URL.User != nil {
		add(req.URL.User.Username())
		p, _ := req.URL.User.Password()
		add(p)
	}
	for k, vs := range req.Header {
		if sensitiveDiagnostic.MatchString(k) {
			for _, v := range vs {
				add(v)
				for _, part := range strings.FieldsFunc(v, func(r rune) bool { return r == ' ' || r == ';' || r == '=' }) {
					add(part)
				}
			}
		}
	}
	return func(s string) string {
		if sensitiveDiagnostic.MatchString(s) {
			return "[redacted]"
		}
		s = diagnosticURL.ReplaceAllStringFunc(s, func(raw string) string {
			u, err := url.Parse(raw)
			if err != nil {
				return "[URL omitted]"
			}
			return u.Path
		})
		for _, secret := range secrets {
			s = strings.ReplaceAll(s, secret, "[redacted]")
		}
		if i := strings.IndexByte(s, '?'); i >= 0 {
			s = s[:i] + "[query omitted]"
		}
		return s
	}
}

func sanitizeDiagnosticJSON(v any, clean func(string) string) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, v := range x {
			if sensitiveDiagnostic.MatchString(k) {
				out["[sensitive field]"] = "[redacted]"
			} else {
				out[clean(k)] = sanitizeDiagnosticJSON(v, clean)
			}
		}
		return out
	case []any:
		for i := range x {
			x[i] = sanitizeDiagnosticJSON(x[i], clean)
		}
	case string:
		return clean(x)
	case json.Number:
		if clean(string(x)) != string(x) {
			return "[redacted]"
		}
	}
	return v
}

func rateLimitError(req *http.Request, resp *http.Response, started, received time.Time, clock Clock) error {
	clean := diagnosticSanitizer(req)
	d := &RateDiagnostic{Method: clean(req.Method), Path: clean(req.URL.Path), RequestStarted: started, ResponseReceived: received, Status: resp.StatusCode}
	var raw []byte
	var err error
	if req.Body != nil && req.Body != http.NoBody {
		// Do not inspect, consume or replay request bodies (including GetBody).
		// An unrecognized response field may echo any opaque request-body value.
		d.BodyOmittedReason = "request_body_present"
	} else {
		raw, err = io.ReadAll(io.LimitReader(resp.Body, rateBodyLimit+1))
	}
	d.BodyReadError = err != nil
	d.BodyTruncated = len(raw) > rateBodyLimit
	d.Body = "[non-JSON, incomplete or oversized response omitted]"
	if d.BodyOmittedReason != "" {
		d.Body = "[response omitted: request body present]"
	} else if !d.BodyReadError && !d.BodyTruncated {
		var v any
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if json.Valid(raw) && decoder.Decode(&v) == nil {
			b, _ := json.Marshal(sanitizeDiagnosticJSON(v, clean))
			if len(b) <= rateBodyLimit {
				d.Body = string(b)
			} else {
				d.BodyTruncated = true
			}
		}
	}
	value := resp.Header.Get("Retry-After")
	d.RetryAfterRaw = "[oversized value omitted]"
	if len(value) <= rateBodyLimit {
		d.RetryAfterRaw = clean(value)
	}
	d.RetryAfterReferenceFrom = clock.Now()
	d.ParsedDelay, d.RetryAfterKind = 60*time.Second, "default"
	if n, err := strconv.ParseFloat(value, 64); err == nil {
		d.ParsedDelay, d.RetryAfterKind = time.Duration(n*float64(time.Second)), "seconds"
	} else if t, err := http.ParseTime(value); err == nil {
		d.ParsedDelay, d.RetryAfterKind = t.Sub(d.RetryAfterReferenceFrom), "date"
	}
	d.ResponseRetryDelay = d.ParsedDelay
	if d.ResponseRetryDelay < time.Second {
		d.ResponseRetryDelay = time.Second
	}
	d.RetryAfterReferenceUntil = d.RetryAfterReferenceFrom.Add(d.ResponseRetryDelay)
	return &RetryError{d, d.ResponseRetryDelay}
}
