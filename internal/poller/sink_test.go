package poller

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileSinkRotationAndGap(t *testing.T) {
	dir := t.TempDir()
	s := NewFileSink(dir, time.Hour)
	before := time.Date(2026, 1, 1, 23, 59, 59, 0, time.UTC)
	s.Write(Record{Venue: "binance", Channel: "exchangeInfo", RecvWall: before.UnixNano(), Payload: []byte(`{"x":1}`)})
	s.Write(Record{Venue: "binance", Symbol: "BTCUSDT", Channel: "openInterest", RecvWall: before.Add(time.Second).UnixNano(), Payload: []byte(`{"x":2}`)})
	s.Write(Record{Venue: "binance", Symbol: "BTCUSDT", Channel: "gap", RecvWall: before.Add(time.Second).UnixNano(), Payload: []byte(`{"event":"poll_failed"}`)})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"binance/_all/20260101/exchangeInfo.p.jsonl", "binance/BTCUSDT/20260102/openInterest.p.jsonl", "binance/gap/20260102/gap.p.jsonl"} {
		b, err := os.ReadFile(filepath.Join(dir, path))
		if err != nil {
			t.Fatal(err)
		}
		var row map[string]any
		if err := json.Unmarshal(b, &row); err != nil {
			t.Fatal(err)
		}
		if row["v"] != float64(1) || row["venue"] != "binance" {
			t.Fatalf("invalid envelope: %s", b)
		}
	}
}
