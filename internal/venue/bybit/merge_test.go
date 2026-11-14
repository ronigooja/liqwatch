package bybit

import (
	"fmt"
	"strings"
	"sync"
	"testing"

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
	send(`{"topic":"tickers.X","type":"delta","data":{"markPrice":"1.00"},"ts":1}`, `{"topic":"tickers.X","type":"delta","data":{"markPrice":"1.00"},"ts":1,"state":"partial"}`)
	send(`{"topic":"tickers.X","type":"snapshot","data":{"symbol":"X","markPrice":"2.00","indexPrice":"3\\u0030","openInterest":"4","bid1Price":"5"},"ts":2}`, `{"topic":"tickers.X","type":"snapshot","data":{"symbol":"X","markPrice":"2.00","indexPrice":"3\\u0030","openInterest":"4"},"ts":2}`)
	send(`{"topic":"tickers.X","type":"delta","data":{"markPrice":"2.50"},"ts":3}`, `{"topic":"tickers.X","type":"delta","data":{"symbol":"X","markPrice":"2.50","indexPrice":"3\\u0030","openInterest":"4"},"ts":3}`)
	send(`{"topic":"tickers.X","type":"delta","data":{"openInterest":"6"},"ts":4}`, `{"topic":"tickers.X","type":"delta","data":{"symbol":"X","markPrice":"2.50","indexPrice":"3\\u0030","openInterest":"6"},"ts":4}`)
	before := a.DroppedDeltas()
	send(`{"topic":"tickers.X","type":"delta","data":{"symbol":"X","bid1Price":"6"},"ts":5}`, "")
	if a.DroppedDeltas() != before+1 {
		t.Fatal("dropped counter")
	}
	send(`{"topic":"tickers.Y","type":"snapshot","data":{"markPrice":"Y"},"ts":6}`, `{"topic":"tickers.Y","type":"snapshot","data":{"markPrice":"Y"},"ts":6}`)
	send(`{"topic":"tickers.X","type":"snapshot","data":{"indexPrice":"new"},"ts":7}`, `{"topic":"tickers.X","type":"snapshot","data":{"indexPrice":"new"},"ts":7}`)
	send(`{"topic":"tickers.X","type":"delta","data":{"markPrice":"newmark"},"ts":8}`, `{"topic":"tickers.X","type":"delta","data":{"indexPrice":"new","markPrice":"newmark"},"ts":8}`)
	send(`{"topic":"tickers.Y","type":"delta","data":{"indexPrice":"yi"},"ts":9}`, `{"topic":"tickers.Y","type":"delta","data":{"markPrice":"Y","indexPrice":"yi"},"ts":9}`)
	a.Subscribe(sh)
	send(`{"topic":"tickers.X","type":"delta","data":{"openInterest":"fresh"},"ts":10}`, `{"topic":"tickers.X","type":"delta","data":{"openInterest":"fresh"},"ts":10,"state":"partial"}`)
	send(`{"topic":"tickers.Y","type":"delta","data":{"indexPrice":"yi2"},"ts":11}`, `{"topic":"tickers.Y","type":"delta","data":{"markPrice":"Y","indexPrice":"yi2"},"ts":11}`)
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
				a.Split(sh, []byte(frame))
			}
		}(i)
	}
	wg.Wait()
}
