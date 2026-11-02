package rawlog

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func readAll(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec, err := zstd.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	var out []map[string]any
	sc := bufio.NewScanner(dec)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("bad line %q: %v", sc.Text(), err)
		}
		out = append(out, m)
	}
	return out
}

func rec(ch string, at time.Time, p string) Record {
	return Record{Conn: "c1", CSeq: 1, RecvWal: at.UnixNano(), RecvMon: Mono(), Channel: ch, Symbol: "BTCUSDT", Payload: []byte(p)}
}

func TestRoundTripAndRotation(t *testing.T) {
	dir := t.TempDir()
	var closed []Closed
	w, err := NewWriter(Options{Dir: dir, QueueSize: 100, OnClosed: func(c Closed) { closed = append(closed, c) }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()

	h1 := time.Date(2026, 11, 3, 10, 30, 0, 0, time.UTC)
	h2 := h1.Add(time.Hour)
	w.Write(GroupTrade, rec("aggTrade", h1, `{"x":1}`))
	w.Write(GroupTrade, rec("aggTrade", h1.Add(time.Second), `{"x":2}`))
	w.Write(GroupTrade, rec("aggTrade", h2, `{"x":3}`))
	cancel()
	<-done

	f1 := filepath.Join(dir, "20261103", "trade-2026110310.jsonl.zst")
	f2 := filepath.Join(dir, "20261103", "trade-2026110311.jsonl.zst")
	a, b := readAll(t, f1), readAll(t, f2)
	if len(a) != 2 || len(b) != 1 {
		t.Fatalf("got %d and %d records", len(a), len(b))
	}
	if a[0]["wseq"].(float64) >= a[1]["wseq"].(float64) || a[1]["wseq"].(float64) >= b[0]["wseq"].(float64) {
		t.Fatal("wseq not increasing")
	}
	if b[0]["p"].(map[string]any)["x"].(float64) != 3 {
		t.Fatal("payload changed")
	}
	if len(closed) != 1 || closed[0].MsgCount != 2 {
		t.Fatalf("OnClosed on rotation: %+v", closed)
	}
	if m, _ := filepath.Glob(filepath.Join(dir, "*", "*.part")); len(m) != 0 {
		t.Fatalf("leftover parts: %v", m)
	}
}

func TestFinishedFileNeverReopened(t *testing.T) {
	dir := t.TempDir()
	at := time.Date(2026, 11, 3, 10, 5, 0, 0, time.UTC)
	for i := 0; i < 2; i++ {
		w, err := NewWriter(Options{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { w.Run(ctx); close(done) }()
		w.Write(GroupLiq, rec("forceOrder_all", at, `{"i":1}`))
		cancel()
		<-done
	}
	base := filepath.Join(dir, "20261103", "liq-2026110310")
	if len(readAll(t, base+".jsonl.zst")) != 1 || len(readAll(t, base+".r1.jsonl.zst")) != 1 {
		t.Fatal("second run must write a new segment")
	}
}

func TestRecoverPart(t *testing.T) {
	dir := t.TempDir()
	d := filepath.Join(dir, "20261103")
	os.MkdirAll(d, 0o750)
	os.WriteFile(filepath.Join(d, "quote-2026110310.jsonl.zst.part"), []byte("x"), 0o640)
	if _, err := NewWriter(Options{Dir: dir}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(d, "quote-2026110310.jsonl.zst")); err != nil {
		t.Fatal("part not recovered:", err)
	}
	got := Sweep(dir, time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC))
	if len(got) != 1 || got[0].Group != "quote" {
		t.Fatalf("sweep: %+v", got)
	}
}

func TestDropOrderAndProtectedChannels(t *testing.T) {
	w, err := NewWriter(Options{Dir: t.TempDir(), QueueSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for i := 0; i < 5; i++ { // fill to 50%: no writer running
		w.Write(GroupDepth, rec("depth", now, `{}`))
	}
	w.Write(GroupTrade, rec(ChMarkPrice, now, `{}`))
	if w.Stats().Dropped[ChMarkPrice] != 1 {
		t.Fatal("markPrice must drop first at 50%")
	}
	w.Write(GroupTrade, rec(ChTrade, now, `{}`))
	if w.Stats().Dropped[ChTrade] != 0 {
		t.Fatal("aggTrade must survive at 50%")
	}
	for i := 0; i < 3; i++ {
		w.Write(GroupDepth, rec("depth", now, `{}`))
	}
	w.Write(GroupQuote, rec(ChQuote, now, `{}`))
	if w.Stats().Dropped[ChQuote] != 1 {
		t.Fatal("bookTicker must drop at 90% fill")
	}
	if w.Stats().Dropped["depth"] != 0 {
		t.Fatal("depth is never counted as dropped")
	}
}
