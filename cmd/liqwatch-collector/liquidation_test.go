package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ronigooja/liqwatch/internal/backfill"
	"github.com/ronigooja/liqwatch/internal/collector"
	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue/binance"
)

const liquidationFrame = `{"stream":"!forceOrder@arr","data":{"e":"forceOrder","E":1727960825201,"o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"0.014","p":"60910.70","ap":"60912.00","X":"FILLED","l":"0.014","z":"0.014","T":1727960825200},"extra":"retain me"}}`

func TestBinanceLiquidationRecord(t *testing.T) {
	a := &binance.Adapter{}
	if _, err := a.Shards(context.Background(), []string{"BTCUSDT"}); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	w, err := rawlog.NewWriter(rawlog.Options{Dir: dir, Instance: "a", DropClass: a.Classes()})
	if err != nil {
		t.Fatal(err)
	}
	tap := backfill.NewTap(w)
	for _, rec := range a.Split(collector.Shard{}, []byte(liquidationFrame)) {
		rec.Venue, rec.CSeq, rec.RecvWal, rec.RecvMon = a.Name(), 7, 1727960825201000000, 42
		tap.Write(rawlog.GroupTrade, rec)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w.Run(ctx) // drains the queue and closes all members
	path := filepath.Join(dir, "binance", "BTCUSDT", "20241003", "forceOrder_all.a.jsonl")
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"v":2,"wseq":1,"cseq":7,"rw":1727960825201000000,"rm":42,"p":{"stream":"!forceOrder@arr","data":{"e":"forceOrder","E":1727960825201,"o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"0.014","p":"60910.70","ap":"60912.00","X":"FILLED","l":"0.014","z":"0.014","T":1727960825200},"extra":"retain me"}}}` + "\n"
	if string(got) != want {
		t.Fatalf("member bytes:\ngot  %s\nwant %s", got, want)
	}
}

func TestBinanceLiquidationMetrics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sent := make(chan error, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(rw, r, nil)
		if err != nil {
			sent <- err
			return
		}
		defer ws.CloseNow()
		for _, frame := range []string{liquidationFrame, strings.Replace(liquidationFrame, "BTCUSDT", "OUTUSDT", 1)} {
			if err := ws.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
				sent <- err
				return
			}
		}
		sent <- nil
		<-ctx.Done()
	}))
	defer srv.Close()
	a := &binance.Adapter{}
	shards, err := a.Shards(ctx, []string{"BTCUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	sh := shards[len(shards)-1]
	sh.URL = "ws" + strings.TrimPrefix(srv.URL, "http")
	w, err := rawlog.NewWriter(rawlog.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	col := collector.New(w, a, []collector.Shard{sh})
	h := metricsHandler(w, col, &readiness{})
	scrape := func() string {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		return r.Body.String()
	}
	const frames = `liqwatch_frames_received_total{shard="misc",channel="forceOrder_all"} `
	const records = `liqwatch_records_produced_total{shard="misc",channel="forceOrder_all"} `
	if got := scrape(); !strings.Contains(got, frames+"0\n") || !strings.Contains(got, records+"0\n") {
		t.Fatalf("counters missing before any frames:\n%s", got)
	}
	done := make(chan struct{})
	go func() { defer close(done); col.Run(ctx) }()
	defer func() { cancel(); <-done }()
	select {
	case err := <-sent:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		got := scrape()
		if strings.Contains(got, frames+"2\n") && strings.Contains(got, records+"1\n") {
			return
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatalf("want two received frames and one produced record:\n%s", got)
		}
	}
}
