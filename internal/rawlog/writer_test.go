package rawlog

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func readAll(t *testing.T, p string) []map[string]any {
	t.Helper()
	f, e := os.Open(p)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	var out []map[string]any
	s := bufio.NewScanner(f)
	for s.Scan() {
		var m map[string]any
		if e := json.Unmarshal(s.Bytes(), &m); e != nil {
			t.Fatal(e)
		}
		out = append(out, m)
	}
	if e := s.Err(); e != nil {
		t.Fatal(e)
	}
	return out
}
func rec(ch string, at time.Time, p string) Record {
	return Record{Conn: "c1", CSeq: 1, RecvWal: at.UnixNano(), RecvMon: Mono(), Channel: ch, Symbol: "BTCUSDT", Payload: []byte(p)}
}
func run(w *Writer) func() {
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	return func() { cancel(); <-w.Done() }
}
func TestDailyRotation(t *testing.T) {
	dir := t.TempDir()
	var closed []Closed
	w, e := NewWriter(Options{Dir: dir, Instance: "a", OnClosed: func(c Closed) { closed = append(closed, c) }})
	if e != nil {
		t.Fatal(e)
	}
	stop := run(w)
	at := time.Date(2026, 11, 3, 23, 59, 59, 0, time.UTC)
	a := rec("trade", at, `{"x":1}`)
	a.Venue = "okx"
	w.Write(GroupTrade, a)
	w.Write(GroupTrade, a)
	a.RecvWal = at.Add(2 * time.Second).UnixNano()
	w.Write(GroupTrade, a)
	stop()
	p := filepath.Join("okx", "BTCUSDT", "20261103", "trade.a.jsonl")
	if len(readAll(t, filepath.Join(dir, p))) != 2 || len(readAll(t, filepath.Join(dir, "okx/BTCUSDT/20261104/trade.a.jsonl"))) != 1 {
		t.Fatal("bad rotation")
	}
	st, _ := os.Stat(filepath.Join(dir, p))
	if len(closed) != 1 || closed[0].Path != p || closed[0].Venue != "okx" || closed[0].Symbol != "BTCUSDT" || closed[0].Channel != "trade" || !closed[0].Day.Equal(day(at)) || closed[0].Bytes != st.Size() || closed[0].MsgCount != 2 {
		t.Fatalf("closed: %+v", closed)
	}
}
func TestRoutingAndGap(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(Options{Dir: dir})
	stop := run(w)
	at := time.Now().UTC()
	for _, s := range []string{"BTC-USDT-SWAP", "BTC_USDT"} {
		for _, ch := range []string{"trade", "mark"} {
			r := rec(ch, at, `{"x":1}`)
			r.Symbol = s
			r.Venue = "okx"
			w.Write(GroupTrade, r)
		}
	}
	g := rec("gap", at, `{}`)
	g.Symbol = ""
	w.Write(GroupGap, g)
	stop()
	for _, s := range []string{"BTC-USDT-SWAP", "BTC_USDT"} {
		for _, ch := range []string{"trade", "mark"} {
			m := readAll(t, filepath.Join(dir, "okx", s, at.Format("20060102"), ch+".jsonl"))
			if len(m) != 1 {
				t.Fatal(m)
			}
		}
	}
	if len(readAll(t, filepath.Join(dir, "binance/gap", at.Format("20060102"), "gap.jsonl"))) != 1 {
		t.Fatal("gap")
	}
}

func TestCompactEnvelope(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(Options{Dir: dir, Instance: "a"})
	if err != nil {
		t.Fatal(err)
	}
	stop := run(w)
	for _, r := range []Record{
		{Venue: "okx", Symbol: "BTC-USDT-SWAP", Channel: "trade", Conn: "diagnostic", CSeq: 7, RecvWal: 1767225600000000000, RecvMon: 12, Payload: []byte(`{"ts":42,"price":"1.2300"}`)},
		{Venue: "kucoin", Channel: "contracts", CSeq: 8, RecvWal: 1767225600000000000, RecvMon: 13, Payload: []byte(`{"data":[{"symbol":"XBTUSDTM"},{"symbol":"ETHUSDTM"}]}`)},
		{Venue: "okx", Channel: "gap", CSeq: 9, RecvWal: 1767225600000000000, RecvMon: 14, Payload: []byte(`{"event":"conn_closed","streams":["trades:BTC-USDT-SWAP"]}`)},
	} {
		w.Write(GroupTrade, r)
	}
	stop()
	cases := []struct{ path, want string }{
		{"okx/BTC-USDT-SWAP/20260101/trade.a.jsonl", `{"v":2,"wseq":1,"cseq":7,"rw":1767225600000000000,"rm":12,"p":{"ts":42,"price":"1.2300"}}`},
		{"kucoin/_all/20260101/contracts.a.jsonl", `{"v":2,"wseq":2,"cseq":8,"rw":1767225600000000000,"rm":13,"p":{"data":[{"symbol":"XBTUSDTM"},{"symbol":"ETHUSDTM"}]}}`},
		{"okx/gap/20260101/gap.a.jsonl", `{"v":2,"wseq":3,"cseq":9,"rw":1767225600000000000,"rm":14,"p":{"event":"conn_closed","streams":["trades:BTC-USDT-SWAP"]}}`},
	}
	for _, tc := range cases {
		var want map[string]any
		if err := json.Unmarshal([]byte(tc.want), &want); err != nil {
			t.Fatal(err)
		}
		got := readAll(t, filepath.Join(dir, tc.path))
		if !reflect.DeepEqual(got, []map[string]any{want}) {
			t.Fatalf("%s: got %v want %v", tc.path, got, want)
		}
	}
}
func TestLRUAppend(t *testing.T) {
	dir := t.TempDir()
	var closed []Closed
	w, _ := NewWriter(Options{Dir: dir, MaxOpenFiles: 1, OnClosed: func(c Closed) { closed = append(closed, c) }})
	stop := run(w)
	at := time.Now().UTC()
	for _, s := range []string{"1000PEPEUSDT", "XBTUSDTM", "1000PEPEUSDT"} {
		r := rec("trade", at, `{}`)
		r.Symbol = s
		w.Write(GroupTrade, r)
	}
	stop()
	a := readAll(t, filepath.Join(dir, "binance/1000PEPEUSDT", at.Format("20060102"), "trade.jsonl"))
	if len(a) != 2 || a[0]["wseq"] == a[1]["wseq"] || len(closed) != 0 {
		t.Fatalf("%v %+v", a, closed)
	}
}
func TestDropOrderAndGap(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(Options{Dir: dir, QueueSize: 10})
	now := time.Now()
	for i := 0; i < 5; i++ {
		w.Write(GroupDepth, rec("depth", now, `{}`))
	}
	r := rec(ChMarkPrice, now, `{}`)
	r.Venue = "okx"
	w.Write(GroupTrade, r)
	w.Write(GroupTrade, rec(ChTrade, now, `{}`))
	for i := 0; i < 3; i++ {
		w.Write(GroupDepth, rec("depth", now, `{}`))
	}
	w.Write(GroupQuote, rec(ChQuote, now, `{}`))
	w.Write(GroupTrade, rec(ChTrade, now, `{}`))
	s := w.Stats()
	if s.Dropped[ChMarkPrice] != 1 || s.Dropped[ChQuote] != 1 || s.Dropped[ChTrade] != 1 {
		t.Fatal(s)
	}
	stop := run(w)
	stop()
	if len(readAll(t, filepath.Join(dir, "okx/gap", now.UTC().Format("20060102"), "gap.jsonl"))) != 1 {
		t.Fatal("missing drop gap")
	}
}
func TestCustomDropClass(t *testing.T) {
	w, err := NewWriter(Options{Dir: t.TempDir(), QueueSize: 2, DropClass: func(ch string) (string, bool) {
		return "mark", ch == "custom"
	}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	w.Write(GroupTrade, rec("unclassified", now, `{}`))
	w.Write(GroupTrade, rec("custom", now, `{}`))
	if got := w.Stats().Dropped["custom"]; got != 1 {
		t.Fatalf("custom drops = %d", got)
	}
	stop := run(w)
	stop()
	if len(readAll(t, filepath.Join(w.opts.Dir, "binance/gap", now.UTC().Format("20060102"), "gap.jsonl"))) != 1 {
		t.Fatal("missing custom drop gap")
	}
}
func TestUnicodeSymbol(t *testing.T) {
	w, err := NewWriter(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	r := rec("aggTrade", now, `{}`)
	r.Symbol = "牛来USDT"
	stop := run(w)
	w.Write(GroupTrade, r)
	stop()
	if len(readAll(t, filepath.Join(w.opts.Dir, "binance", r.Symbol, now.Format("20060102"), "aggTrade.jsonl"))) != 1 {
		t.Fatal("missing unicode symbol")
	}
}
func TestDiskStop(t *testing.T) {
	w, _ := NewWriter(Options{Dir: t.TempDir(), MaxDiskPct: 0.000000001})
	stop := run(w)
	w.Write(GroupDepth, rec("depth", time.Now(), `{}`))
	stop()
	if w.Stats().DiskStops != 1 {
		t.Fatal(w.Stats())
	}
}
func TestSweep(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"20260101", "20260102", "20260103"} {
		p := filepath.Join(dir, "binance/BTC", d, "trade.a.jsonl")
		os.MkdirAll(filepath.Dir(p), 0750)
		os.WriteFile(p, []byte("x"), 0640)
	}
	a := Sweep(dir, time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC))
	if len(a) != 1 || a[0].Channel != "trade" || a[0].MsgCount != -1 || a[0].Bytes != 1 {
		t.Fatal(a)
	}
}
func TestPathSafety(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(Options{Dir: dir})
	stop := run(w)
	r := rec("trade", time.Now(), `{}`)
	r.Symbol = "../x"
	w.Write(GroupTrade, r)
	stop()
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatal(entries)
	}
	if _, e := NewWriter(Options{Dir: dir, Instance: "Bad1"}); e == nil {
		t.Fatal("invalid instance")
	}
}
