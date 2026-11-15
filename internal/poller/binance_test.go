package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBinanceUniverseBracketsIncremental(t *testing.T) {
	tier := `{"bracketSeq":1,"bracketNotionalFloor":0,"bracketNotionalCap":1,"bracketMaintenanceMarginRate":1,"cumFastMaintenanceAmount":1,"minOpenPosLeverage":1,"maxOpenPosLeverage":2}`
	bracket := fmt.Sprintf(`{"code":"0","message":"","messageDetail":"","success":true,"data":{"brackets":[{"symbol":"BTCUSDT","updateTime":null,"notionalLimit":1,"riskBrackets":[%s]},{"symbol":"ETHUSDT","updateTime":null,"notionalLimit":1,"riskBrackets":[%s]}]}}`, tier, tier)
	var start string
	invalid := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/exchangeInfo":
			fmt.Fprint(w, `{"symbols":[{"symbol":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING","quoteAsset":"USDT"},{"symbol":"OTHER","contractType":"CURRENT_QUARTER","status":"TRADING","quoteAsset":"USDT"}]}`)
		case "/brackets":
			if invalid {
				fmt.Fprint(w, `{}`)
			} else {
				fmt.Fprint(w, bracket)
			}
		default:
			start = r.URL.Query().Get("startTime")
			fmt.Fprint(w, `[{"timestamp":1767225600000}]`)
		}
	}))
	defer server.Close()
	clock := &fakeClock{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b := &Binance{Clock: clock, Doer: server.Client(), Config: PollConfig{BaseURL: server.URL, BracketsURL: server.URL + "/brackets"}}
	channels := BinanceChannels(b)
	sink := &memorySink{}
	scheduler, _ := NewScheduler(channels, sink, clock, "", 240)
	b.Scheduler = scheduler
	if _, err := b.request(context.Background(), "exchangeInfo", ""); err != nil {
		t.Fatal(err)
	}
	if len(b.Symbols()) != 1 || b.Symbols()[0] != "BTCUSDT" {
		t.Fatal(b.Symbols())
	}
	rows, err := b.request(context.Background(), "brackets", "")
	if err != nil || len(rows) != 2 || rows[1].Symbol != "ETHUSDT" {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	invalid = true
	if _, err = b.request(context.Background(), "brackets", ""); err == nil {
		t.Fatal("schema accepted")
	}
	scheduler.state.Jobs["binance/openInterestHist/BTCUSDT"] = jobState{RowTimestamp: 12345}
	clock.t = clock.t.Add(10 * time.Minute)
	rows, err = b.request(context.Background(), "openInterestHist", "BTCUSDT")
	if err != nil || start == "" {
		t.Fatalf("start=%s err=%v", start, err)
	}
	if !json.Valid(rows[0].Payload) {
		t.Fatal("invalid payload")
	}
}
func TestBracketsFailureStopsJob(t *testing.T) {
	c := &fakeClock{time.Now()}
	sink := &memorySink{}
	calls := 0
	ch := Channel{Venue: "binance", Name: "brackets", Daily: true, Fetch: func(context.Context, string) ([]Record, error) {
		calls++
		return nil, &PermanentError{fmt.Errorf("schema"), []byte(`{}`)}
	}}
	s, _ := NewScheduler([]Channel{ch}, sink, c, "", 240)
	s.Step(context.Background())
	s.Step(context.Background())
	if calls != 1 || len(sink.records) != 2 || sink.records[1].Channel != "brackets_error" {
		t.Fatal(calls, sink.records)
	}
}
