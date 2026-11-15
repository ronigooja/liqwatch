package poller

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestFileSinkRotationAndGap(t *testing.T) {
	dir := t.TempDir()
	s := NewFileSink(dir, time.Hour)
	before := time.Date(2026, 1, 1, 23, 59, 59, 0, time.UTC)
	s.Write(Record{Venue: "binance", Channel: "exchangeInfo", RecvWall: before.UnixNano(), Payload: []byte(`{"symbols":[{"symbol":"BTCUSDT"},{"symbol":"ETHUSDT"}]}`)})
	s.Write(Record{Venue: "binance", Symbol: "BTCUSDT", Channel: "openInterest", RecvWall: before.Add(time.Second).UnixNano(), RecvMono: 12, Meta: map[string]any{"snapshot": true, "last_row_timestamp": int64(42)}, Payload: []byte(`{"x":2,"time":42}`)})
	s.Write(Record{Venue: "binance", Symbol: "BTCUSDT", Channel: "gap", RecvWall: before.Add(time.Second).UnixNano(), Payload: []byte(`{"event":"poll_failed","symbol":"BTCUSDT","channel":"openInterest"}`)})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ path, want string }{
		{"binance/_all/20260101/exchangeInfo.p.jsonl", `{"v":2,"rw":1767311999000000000,"rm":0,"meta":null,"p":{"symbols":[{"symbol":"BTCUSDT"},{"symbol":"ETHUSDT"}]}}`},
		{"binance/BTCUSDT/20260102/openInterest.p.jsonl", `{"v":2,"rw":1767312000000000000,"rm":12,"meta":{"snapshot":true,"last_row_timestamp":42},"p":{"x":2,"time":42}}`},
		{"binance/gap/20260102/gap.p.jsonl", `{"v":2,"rw":1767312000000000000,"rm":0,"meta":null,"p":{"event":"poll_failed","symbol":"BTCUSDT","channel":"openInterest"}}`},
	} {
		path := tc.path
		b, err := os.ReadFile(filepath.Join(dir, path))
		if err != nil {
			t.Fatal(err)
		}
		var row map[string]any
		if err := json.Unmarshal(b, &row); err != nil {
			t.Fatal(err)
		}
		var want map[string]any
		if err := json.Unmarshal([]byte(tc.want), &want); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(row, want) {
			t.Fatalf("invalid envelope: %s", b)
		}
	}
}
