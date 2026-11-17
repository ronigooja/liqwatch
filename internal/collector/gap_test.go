package collector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ronigooja/liqwatch/internal/rawlog"
)

// Literal whole-record goldens intentionally do not derive expectations from field tables.
const closedGolden = `{"v":2,"wseq":1,"cseq":0,"rw":1794873605000000000,"rm":6000000000,"p":{"cause":"reconnect","conn":"market-1","err":"EOF","event":"conn_closed","last_rm":4000000000,"last_rw":1794873603000000000,"msgs":7,"prev":null,"shard":"market","streams":["one","two"],"up_ms":5000}}`
const openedGolden = `{"v":2,"wseq":2,"cseq":0,"rw":1794873607500000000,"rm":8500000000,"p":{"attempt":2,"backoff_ms":1000,"conn":"market-2","down_ms":2500,"event":"conn_opened","initial":false,"prev":"market-1","shard":"market","subs_ok":true,"subscribed":2}}`

func TestGapWholeRecordGoldensAndEnvelope(t *testing.T) {
	dir := t.TempDir()
	w, err := rawlog.NewWriter(rawlog.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	base := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	c := New(w, fakeAdapter{}, nil)
	c.clock = func() (time.Time, int64) { return base.Add(7500 * time.Millisecond), 8500000000 }
	sh := Shard{Name: "market", Streams: []string{"one", "two"}}
	k := &conn{id: "market-1", up: base, upMon: 1000000000, closed: base.Add(5 * time.Second), closedMon: 6000000000,
		last: base.Add(3 * time.Second), lastMon: 4000000000, msgs: 7, e: errors.New("EOF")}
	c.gap(sh, k, "reconnect")
	c.openGap(sh, &conn{id: "market-2", gen: 2, previous: k, backoff: time.Second})
	cancel()
	<-w.Done()
	b, err := os.ReadFile(filepath.Join(dir, "fake/gap/20261117/gap.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	want := closedGolden + "\n" + openedGolden + "\n"
	if string(b) != want {
		t.Fatalf("whole records:\n%s\nwant:\n%s", b, want)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &envelope); err != nil {
			t.Fatal(err)
		}
		if len(envelope) != 6 {
			t.Fatalf("envelope keys changed: %s", line)
		}
		for _, key := range []string{"v", "wseq", "cseq", "rw", "rm", "p"} {
			if envelope[key] == nil {
				t.Fatalf("missing envelope key %s", key)
			}
		}
	}
}

func TestGapInitialAndNoMessages(t *testing.T) {
	s := &captureSink{}
	c := New(s, fakeAdapter{}, nil)
	now := time.Unix(10, 0)
	c.clock = func() (time.Time, int64) { return now, 2000000000 }
	sh := Shard{Name: "empty"}
	k := &conn{id: "empty-1", gen: 1, closed: now, closedMon: 2000000000}
	c.openGap(sh, k)
	c.gap(sh, k, "reconnect")
	if got := string(s.records[0].Payload); got != `{"attempt":1,"backoff_ms":0,"conn":"empty-1","down_ms":null,"event":"conn_opened","initial":true,"prev":null,"shard":"empty","subs_ok":true,"subscribed":0}` {
		t.Fatal(got)
	}
	if got := string(s.records[1].Payload); got != `{"cause":"reconnect","conn":"empty-1","err":"","event":"conn_closed","last_rm":0,"last_rw":0,"msgs":0,"prev":null,"shard":"empty","streams":null,"up_ms":0}` {
		t.Fatal(got)
	}
}

func TestGapReconnectSequence(t *testing.T) {
	s := &captureSink{}
	connections := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		if _, _, err := ws.Read(r.Context()); err != nil {
			return
		}
		connections++
		if ws.Write(r.Context(), websocket.MessageText, []byte("control")) != nil {
			return
		}
		if connections == 1 {
			// Wait for the collector to consume the frame before ending this connection.
			time.Sleep(20 * time.Millisecond)
			return
		}
		_, _, _ = ws.Read(r.Context())
	}))
	defer srv.Close()
	sh := Shard{Name: "market", URL: srv.URL, Streams: []string{"one", "two"}}
	c := New(s, activeAdapter{}, []Shard{sh})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	deadline := time.After(5 * time.Second)
	for {
		s.mu.Lock()
		records := append([]rawlog.Record(nil), s.records...)
		s.mu.Unlock()
		var closed *rawlog.Record
		opened, first := false, false
		for i := range records {
			r := &records[i]
			var p struct {
				Event      string
				Down       int64 `json:"down_ms"`
				Last       int64 `json:"last_rm"`
				Msgs       int
				Prev       string
				Initial    bool
				Subscribed int
				OK         bool `json:"subs_ok"`
			}
			if err := json.Unmarshal(r.Payload, &p); err != nil {
				t.Fatal(err)
			}
			switch p.Event {
			case "conn_closed":
				closed = r
				if p.Last == 0 || p.Last > r.RecvMon || p.Msgs != 1 {
					t.Fatalf("bad close: %s", r.Payload)
				}
			case "conn_opened":
				if closed != nil {
					if p.Down != (r.RecvMon-closed.RecvMon)/int64(time.Millisecond) || p.Prev != closed.Conn || p.Initial || !p.OK || p.Subscribed != 2 {
						t.Fatalf("inconsistent reopen: %s", r.Payload)
					}
					opened = true
				}
			case "conn_first":
				if closed != nil {
					first = true
				}
			}
		}
		if opened && first {
			return
		}
		select {
		case <-deadline:
			t.Fatal("missing reconnect gap sequence")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
