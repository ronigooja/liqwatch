package backfill

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/wicker/wicker/internal/rawlog"
)

type testSink struct {
	mu      sync.Mutex
	records []rawlog.Record
}

func (s *testSink) Write(_ string, r rawlog.Record) {
	s.mu.Lock()
	s.records = append(s.records, r)
	s.mu.Unlock()
}

type response struct {
	status int
	body   string
	retry  string
}
type testDoer struct {
	mu        sync.Mutex
	responses []response
	requests  []*http.Request
}

func (d *testDoer) Do(r *http.Request) (*http.Response, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.requests = append(d.requests, r)
	x := d.responses[0]
	d.responses = d.responses[1:]
	h := make(http.Header)
	if x.retry != "" {
		h.Set("Retry-After", x.retry)
	}
	return &http.Response{StatusCode: x.status, Header: h, Body: io.NopCloser(bytes.NewBufferString(x.body))}, nil
}
func trade(id int64) rawlog.Record {
	return rawlog.Record{Channel: "aggTrade", Symbol: "ENAUSDT", Payload: []byte(`{"data":{"a":` + strconv.FormatInt(id, 10) + `}}`)}
}

func TestTapExtractsAndForwards(t *testing.T) {
	s := &testSink{}
	tap := NewTap(s)
	tap.Write(rawlog.GroupTrade, trade(12))
	tap.Write(rawlog.GroupTrade, rawlog.Record{Channel: "bookTicker", Symbol: "ENAUSDT", Payload: []byte(`{"a":99}`)})
	if got, ok := tap.Last("ENAUSDT"); !ok || got != 12 {
		t.Fatalf("last = %d, %v", got, ok)
	}
	if len(tap.Symbols()) != 1 || len(s.records) != 2 {
		t.Fatal("symbols or forwarding incorrect")
	}
}

func TestBackfillPagination(t *testing.T) {
	s := &testSink{}
	tap := NewTap(s)
	tap.Write(rawlog.GroupTrade, trade(10))
	var page bytes.Buffer
	page.WriteByte('[')
	for i := 11; i <= 1010; i++ {
		if i > 11 {
			page.WriteByte(',')
		}
		page.WriteString(`{"a":` + strconv.Itoa(i) + `}`)
	}
	page.WriteByte(']')
	d := &testDoer{responses: []response{{200, page.String(), ""}, {200, `[{"a":1011}]`, ""}}}
	b := New(tap, s, d, 1000, 1)
	b.sleep = func(context.Context, time.Duration) error { return nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)
	b.RequestShard([]string{"ENAUSDT"})
	time.Sleep(20 * time.Millisecond)
	d.mu.Lock()
	n := len(d.requests)
	first := d.requests[0].URL.Query().Get("fromId")
	second := d.requests[1].URL.Query().Get("fromId")
	d.mu.Unlock()
	if n != 2 || first != "11" || second != "1011" {
		t.Fatalf("requests=%d first=%s second=%s", n, first, second)
	}
}

func TestBackfillStopsAtFirstNewTrade(t *testing.T) {
	s := &testSink{}
	tap := NewTap(s)
	tap.Write(rawlog.GroupTrade, trade(10))
	d := &testDoer{responses: []response{{200, `[{"a":11},{"a":12}]`, ""}}}
	b := New(tap, s, d, 1000, 1)
	b.sleep = func(context.Context, time.Duration) error { return nil }
	marker := tap.Mark()
	tap.Write(rawlog.GroupTrade, trade(12))
	b.runRequest(context.Background(), request{symbols: []string{"ENAUSDT"}, marker: marker})
	if len(d.requests) != 1 {
		t.Fatal("did not stop at new websocket trade")
	}
}

func TestNon200RecordedWithoutAdvancing(t *testing.T) {
	s := &testSink{}
	tap := NewTap(s)
	tap.Write(rawlog.GroupTrade, trade(10))
	d := &testDoer{responses: []response{{500, `{"code":-1}`, ""}}}
	b := New(tap, s, d, 1000, 1)
	b.runRequest(context.Background(), request{symbols: []string{"ENAUSDT"}})
	if got, _ := tap.Last("ENAUSDT"); got != 10 {
		t.Fatalf("last advanced to %d", got)
	}
	if len(s.records) != 2 {
		t.Fatalf("records=%d", len(s.records))
	}
	var p struct {
		Status int `json:"status"`
	}
	if json.Unmarshal(s.records[1].Payload, &p) != nil || p.Status != 500 {
		t.Fatal("non-200 envelope missing")
	}
}

func TestRateLimitRetry(t *testing.T) {
	s := &testSink{}
	tap := NewTap(s)
	tap.Write(rawlog.GroupTrade, trade(10))
	d := &testDoer{responses: []response{{429, `{"code":-1003}`, "1"}, {200, `[{"a":11}]`, ""}}}
	b := New(tap, s, d, 1000, 1)
	var waits []time.Duration
	b.sleep = func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }
	b.runRequest(context.Background(), request{symbols: []string{"ENAUSDT"}})
	if len(d.requests) != 2 || len(waits) != 1 || waits[0] != time.Second {
		t.Fatalf("retry requests=%d waits=%v", len(d.requests), waits)
	}
}

type errDoer struct{ calls int }

func (d *errDoer) Do(*http.Request) (*http.Response, error) {
	d.calls++
	return nil, io.ErrUnexpectedEOF
}

func TestNetworkErrorsBackOffAndCountAgainstBudget(t *testing.T) {
	tap := NewTap(nil)
	tap.Write(rawlog.GroupTrade, trade(10))
	d := &errDoer{}
	b := New(tap, nil, d, 2400, 0.2)
	var slept []time.Duration
	b.sleep = func(_ context.Context, dur time.Duration) error { slept = append(slept, dur); return nil }
	b.RequestShard([]string{"ENAUSDT"})
	b.runRequest(context.Background(), <-b.requests)
	if d.calls != maxPages {
		t.Fatalf("calls = %d, want the page cap %d", d.calls, maxPages)
	}
	if len(slept) < maxPages-1 || slept[0] != time.Second || slept[1] <= slept[0] {
		t.Fatalf("errors must back off exponentially, slept=%v", slept)
	}
	if len(b.usage) != maxPages {
		t.Fatalf("failed requests must count against the weight budget, usage=%d", len(b.usage))
	}
}

func TestMarkIsReleased(t *testing.T) {
	tap := NewTap(nil)
	tap.Write(rawlog.GroupTrade, trade(10))
	b := New(tap, nil, &testDoer{responses: []response{{status: 200, body: "[]"}}}, 2400, 0.2)
	b.sleep = func(context.Context, time.Duration) error { return nil }
	b.RequestShard([]string{"ENAUSDT"})
	b.runRequest(context.Background(), <-b.requests)
	if len(tap.marks) != 0 || len(tap.starts) != 0 {
		t.Fatalf("marker state leaked: %d %d", len(tap.marks), len(tap.starts))
	}
}
