package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestOKXFixtures(t *testing.T) {
	for i := 0; i < 7; i++ {
		b, err := os.ReadFile(fmt.Sprintf("testdata/okx/response%d.json", i))
		if err != nil {
			t.Fatal(err)
		}
		var v okxEnvelope
		if err = json.Unmarshal(b, &v); err != nil || v.Code != "0" {
			t.Fatalf("fixture %d: %v", i, err)
		}
	}
}

func TestOKXHistoryAndSnapshots(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.UserAgent() != DefaultUserAgent {
			t.Errorf("user agent %q", r.UserAgent())
		}
		paths = append(paths, r.URL.Path)
		fixture := 0
		switch {
		case strings.Contains(r.URL.Path, "top-trader") && strings.Contains(r.URL.Path, "position"):
			fixture = 2
		case strings.Contains(r.URL.Path, "top-trader"):
			fixture = 1
		case strings.Contains(r.URL.Path, "taker-volume"):
			fixture = 3
		case strings.HasSuffix(r.URL.Path, "long-short-account-ratio"):
			fixture = 4
		case strings.HasSuffix(r.URL.Path, "position-tiers"):
			fixture = 5
			if r.URL.Query().Get("instFamily") != "BTC-USDT,ETH-USDT" {
				t.Errorf("families %s", r.URL.RawQuery)
			}
		case strings.HasSuffix(r.URL.Path, "instruments"):
			fixture = 6
		}
		b, err := os.ReadFile(fmt.Sprintf("testdata/okx/response%d.json", fixture))
		if err != nil {
			t.Error(err)
		}
		w.Write(b)
	}))
	defer server.Close()
	e := &Env{Venue: "okx", Doer: uaDoer{server.Client()}, Clock: RealClock{}, BaseURL: server.URL, Config: PollConfig{}, Universe: func() []string { return []string{"BTC-USDT-SWAP", "ETH-USDT-SWAP"} }, Periods: map[string]time.Duration{}, Weights: map[string]float64{}, ChannelRPS: map[string]float64{}}
	channels := okxFactory(e)
	for _, c := range channels {
		if c.Name == "instruments" || c.Name == "positionTiers" {
			r, err := c.Fetch(context.Background(), "")
			if err != nil || len(r) == 0 {
				t.Fatalf("%s %v", c.Name, err)
			}
		}
	}
	rows, err := okxRows(context.Background(), e, "/api/v5/rubik/stat/contracts/long-short-account-ratio-contract", "BTC-USDT-SWAP", 1, 2000000000000, false)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows %d: %v", len(rows), err)
	}
	if len(paths) != 4 {
		t.Fatalf("requests %d", len(paths))
	}
	if okxCurrency("1000PEPE-USDT-SWAP") != "1000PEPE" {
		t.Fatal("currency")
	}
}
