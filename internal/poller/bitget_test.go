package poller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBitgetChannels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") != DefaultUserAgent {
			t.Error("missing user agent")
		}
		if strings.HasSuffix(r.URL.Path, "contracts") || strings.HasSuffix(r.URL.Path, "query-position-lever") {
			if r.URL.Query().Get("productType") != "USDT-FUTURES" {
				t.Error("wrong productType")
			}
			fmt.Fprint(w, `{"code":"00000","data":[]}`)
			return
		}
		if r.URL.Query().Get("period") != "5m" || r.URL.Query().Get("symbol") == "" {
			t.Error("wrong history query")
		}
		fmt.Fprint(w, `{"code":"00000","data":[{"ts":"1700000100000"},{"ts":"1700000400000"}]}`)
	}))
	defer server.Close()
	e := &Env{Venue: "bitget", BaseURL: server.URL, Doer: uaDoer{server.Client()}, Clock: RealClock{}, RPS: 1, Periods: map[string]time.Duration{}, ChannelRPS: map[string]float64{}, Weights: map[string]float64{}}
	channels := bitgetChannels(e)
	if len(channels) != 6 {
		t.Fatalf("got %d channels", len(channels))
	}
	for i, c := range channels {
		records, err := c.Fetch(context.Background(), "BTCUSDT")
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		want := 1
		if i < 4 {
			want = 2
			if c.Period != time.Hour {
				t.Errorf("%s period %v", c.Name, c.Period)
			}
		}
		if len(records) != want {
			t.Errorf("%s got %d records", c.Name, len(records))
		}
	}
}
