package bybit

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

func TestTickerMerge(t *testing.T) {
	a := &Adapter{}
	sh := venue.Shard{Name: "merge", Streams: []string{"tickers.X"}}
	send := func(frame, want string) {
		t.Helper()
		records := a.Split(sh, []byte(frame))
		var got []string
		for _, r := range records {
			if !strings.HasPrefix(string(r.Payload), `{"sample":`) {
				got = append(got, string(r.Payload))
			}
		}
		if want == "" {
			if len(got) != 0 {
				t.Fatalf("got %v", got)
			}
			return
		}
		if len(got) != 1 || got[0] != want {
			t.Fatalf("got %v want %s", got, want)
		}
	}
	send(`{"topic":"tickers.X","type":"delta","data":{"markPrice":"1.00"},"ts":1}`, `{"data":{"markPrice":"1.00"},"ts":1,"state":"partial"}`)
	send(`{"topic":"tickers.X","type":"snapshot","data":{"symbol":"X","markPrice":"2.00","indexPrice":"3\\u0030","openInterest":"4","bid1Price":"5"},"ts":2}`, `{"data":{"markPrice":"2.00","indexPrice":"3\\u0030","openInterest":"4"},"ts":2}`)
	send(`{"topic":"tickers.X","type":"delta","data":{"markPrice":"2.50"},"ts":3}`, `{"data":{"markPrice":"2.50","indexPrice":"3\\u0030","openInterest":"4"},"ts":3}`)
	send(`{"topic":"tickers.X","type":"delta","data":{"openInterest":"6"},"ts":4}`, `{"data":{"markPrice":"2.50","indexPrice":"3\\u0030","openInterest":"6"},"ts":4}`)
	before := a.DroppedDeltas()
	send(`{"topic":"tickers.X","type":"delta","data":{"symbol":"X","bid1Price":"6"},"ts":5}`, "")
	if a.DroppedDeltas() != before+1 {
		t.Fatal("dropped counter")
	}
	send(`{"topic":"tickers.Y","type":"snapshot","data":{"markPrice":"Y"},"ts":6}`, `{"data":{"markPrice":"Y"},"ts":6}`)
	send(`{"topic":"tickers.X","type":"snapshot","data":{"indexPrice":"new"},"ts":7}`, `{"data":{"indexPrice":"new"},"ts":7}`)
	send(`{"topic":"tickers.X","type":"delta","data":{"markPrice":"newmark"},"ts":8}`, `{"data":{"indexPrice":"new","markPrice":"newmark"},"ts":8}`)
	send(`{"topic":"tickers.Y","type":"delta","data":{"indexPrice":"yi"},"ts":9}`, `{"data":{"markPrice":"Y","indexPrice":"yi"},"ts":9}`)
	a.Subscribe(sh)
	send(`{"topic":"tickers.X","type":"delta","data":{"openInterest":"fresh"},"ts":10}`, `{"data":{"openInterest":"fresh"},"ts":10,"state":"partial"}`)
	send(`{"topic":"tickers.Y","type":"delta","data":{"indexPrice":"yi2"},"ts":11}`, `{"data":{"markPrice":"Y","indexPrice":"yi2"},"ts":11}`)
}

func TestTickerMergeConcurrent(t *testing.T) {
	a := &Adapter{}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sym := fmt.Sprintf("S%d", i)
			sh := venue.Shard{Name: sym, Streams: []string{"tickers." + sym}}
			a.Subscribe(sh)
			for j := 0; j < 100; j++ {
				typ := "delta"
				if j == 0 {
					typ = "snapshot"
				}
				frame := fmt.Sprintf(`{"topic":"tickers.%s","type":"%s","data":{"markPrice":"%d"}}`, sym, typ, j)
				for _, r := range a.Split(sh, []byte(frame)) {
					if !strings.HasPrefix(string(r.Payload), `{"sample":`) && !strings.Contains(string(r.Payload), fmt.Sprintf(`"markPrice":"%d"`, j)) {
						t.Errorf("symbol %s: %s", sym, r.Payload)
					}
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestTickerConnectionIsolationAndClose(t *testing.T) {
	a := &Adapter{}
	type connected interface {
		ConnOpened(venue.Shard, string)
		ConnClosed(venue.Shard, string)
		SplitConn(venue.Shard, string, []byte) []rawlog.Record
	}
	c, ok := any(a).(connected)
	if !ok {
		t.Fatal("missing connection-aware interfaces")
	}
	sh := venue.Shard{Name: "isolation"}
	c.ConnOpened(sh, "a")
	c.ConnOpened(sh, "b")
	send := func(id, typ, data string) string {
		t.Helper()
		frame := fmt.Sprintf(`{"topic":"tickers.X","type":%q,"data":%s}`, typ, data)
		for _, r := range c.SplitConn(sh, id, []byte(frame)) {
			if !strings.HasPrefix(string(r.Payload), `{"sample":`) {
				return string(r.Payload)
			}
		}
		return ""
	}
	send("a", "snapshot", `{"markPrice":"1","indexPrice":"a"}`)
	send("a", "delta", `{"markPrice":"2"}`)
	send("b", "snapshot", `{"markPrice":"10","indexPrice":"b"}`)
	if got := send("a", "delta", `{"markPrice":"3"}`); !strings.Contains(got, `"indexPrice":"a"`) {
		t.Fatalf("A: %s", got)
	}
	if got := send("b", "delta", `{"openInterest":"4"}`); !strings.Contains(got, `"markPrice":"10"`) || !strings.Contains(got, `"indexPrice":"b"`) {
		t.Fatalf("B: %s", got)
	}
	c.ConnClosed(sh, "a")
	c.ConnOpened(sh, "a")
	if got := send("a", "delta", `{"markPrice":"5"}`); got != "" {
		t.Fatalf("partial should not displace complete overlap state: %s", got)
	}
	c.ConnClosed(sh, "b")
	if got := send("a", "delta", `{"markPrice":"5"}`); !strings.Contains(got, `"state":"partial"`) || strings.Contains(got, `"indexPrice"`) {
		t.Fatalf("closed state retained: %s", got)
	}
}

func TestTickerSnapshotsNoGapWholeRecordGolden(t *testing.T) {
	a := &Adapter{}
	sh := venue.Shard{Name: "market"}
	dir := t.TempDir()
	w, err := rawlog.NewWriter(rawlog.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	defer func() { cancel(); <-w.Done() }()
	// Multiple symbols, repeated snapshots on a healthy connection, and a reconnect.
	for _, id := range []string{"c1", "c2"} {
		a.ConnOpened(sh, id)
		for _, symbol := range []string{"X", "Y", "X"} {
			frame := fmt.Sprintf(`{"topic":"tickers.%s","type":"snapshot","data":{"markPrice":"1"}}`, symbol)
			for _, r := range a.SplitConn(sh, id, []byte(frame)) {
				if r.Channel == "gap" {
					t.Fatalf("snapshot must not emit gap: %s", r.Payload)
				}
				if strings.HasPrefix(string(r.Payload), `{"sample":`) {
					continue
				}
				r.Venue, r.Conn, r.CSeq, r.RecvWal, r.RecvMon = "bybit", id, 1, 42, 3
				w.Write(rawlog.GroupTrade, r)
			}
		}
		a.ConnClosed(sh, id)
	}
	cancel()
	<-w.Done()
	data, err := os.ReadFile(filepath.Join(dir, "bybit/X/19700101/mark.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	const golden = `{"v":2,"wseq":1,"cseq":1,"rw":42,"rm":3,"p":{"data":{"markPrice":"1"}}}
{"v":2,"wseq":3,"cseq":1,"rw":42,"rm":3,"p":{"data":{"markPrice":"1"}}}
{"v":2,"wseq":4,"cseq":1,"rw":42,"rm":3,"p":{"data":{"markPrice":"1"}}}
{"v":2,"wseq":6,"cseq":1,"rw":42,"rm":3,"p":{"data":{"markPrice":"1"}}}
`
	if string(data) != golden {
		t.Fatalf("whole records:\n%s\nwant:\n%s", data, golden)
	}
	data, err = os.ReadFile(filepath.Join(dir, "bybit/Y/19700101/mark.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	const otherGolden = `{"v":2,"wseq":2,"cseq":1,"rw":42,"rm":3,"p":{"data":{"markPrice":"1"}}}
{"v":2,"wseq":5,"cseq":1,"rw":42,"rm":3,"p":{"data":{"markPrice":"1"}}}
`
	if string(data) != otherGolden {
		t.Fatalf("whole records:\n%s\nwant:\n%s", data, otherGolden)
	}
	if _, err := os.Stat(filepath.Join(dir, "bybit/gap")); !os.IsNotExist(err) {
		t.Fatalf("unexpected gap directory: %v", err)
	}
}

func TestSnapshotDiagnosticScope469Symbols(t *testing.T) {
	syms := make([]venue.Symbol, 469)
	for i := range syms {
		syms[i] = fmt.Sprintf("S%03dUSDT", i)
	}
	a := &Adapter{}
	shards, err := a.Shards(context.Background(), syms)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != 16 {
		t.Fatalf("shards = %d", len(shards))
	}
	count := 0
	for i, sh := range shards {
		tickers := 0
		for _, stream := range sh.Streams {
			if strings.HasPrefix(stream, "tickers.") {
				tickers++
			}
		}
		want := 30
		if i == 15 {
			want = 19
		}
		if tickers != want {
			t.Fatalf("%s ticker streams = %d, want %d", sh.Name, tickers, want)
		}
		count += tickers
	}
	if count != 469 {
		t.Fatalf("ticker streams = %d", count)
	}
}
