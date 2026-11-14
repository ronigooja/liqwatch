package collector

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
)

type throttleSink struct {
	mu      sync.Mutex
	records []rawlog.Record
}

func (s *throttleSink) Write(_ string, r rawlog.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, r)
}

func TestThrottleBuckets(t *testing.T) {
	s := &throttleSink{}
	tt := &throttler{sink: s, intervals: map[string]time.Duration{"mark": time.Second}, buckets: map[throttleKey]throttleBucket{}, counts: map[string]*atomic.Int64{"mark": {}}}
	put := func(symbol string, ms int64, seq uint64) {
		tt.write(rawlog.Record{Channel: "mark", Symbol: symbol, RecvWal: ms * int64(time.Millisecond), CSeq: seq, Conn: "c", RecvMon: int64(seq)})
	}
	put("A", 1000, 1)
	put("A", 1200, 2)
	put("B", 1300, 3)
	put("A", 2000, 4)
	if len(s.records) != 1 || s.records[0].CSeq != 2 || s.records[0].RecvMon != 2 {
		t.Fatalf("boundary: %+v", s.records)
	}
	tt.flush(time.UnixMilli(2499), "", false)
	if len(s.records) != 1 {
		t.Fatal("early flush")
	}
	tt.flush(time.UnixMilli(2500), "", false)
	if len(s.records) != 2 || s.records[1].Symbol != "B" {
		t.Fatalf("timer: %+v", s.records)
	}
	tt.flush(time.Time{}, "c", false)
	if len(s.records) != 3 || s.records[2].CSeq != 4 || tt.counts["mark"].Load() != 1 {
		t.Fatal("close or counter")
	}
	tt.intervals["mark"] = 0
	put("A", 3000, 5)
	if len(s.records) != 4 {
		t.Fatal("disabled")
	}
}

func TestThrottleConcurrent(t *testing.T) {
	s := &throttleSink{}
	tt := &throttler{sink: s, intervals: map[string]time.Duration{"mark": time.Second}, buckets: map[throttleKey]throttleBucket{}, counts: map[string]*atomic.Int64{"mark": {}}}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tt.write(rawlog.Record{Channel: "mark", Symbol: "A", RecvWal: int64(time.Second)})
		}()
	}
	wg.Wait()
	tt.flush(time.Time{}, "", true)
	if len(s.records) != 1 || tt.counts["mark"].Load() != 99 {
		t.Fatal("concurrent count")
	}
}
