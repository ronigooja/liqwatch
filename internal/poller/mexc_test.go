package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMEXCContractDetail(t *testing.T) {
	fixture, err := os.ReadFile("testdata/mexc/detail.json")
	if err != nil {
		t.Fatal(err)
	}
	body := string(fixture)
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.String() != "/api/v1/contract/detail" || r.Header.Get("User-Agent") != "Mozilla/5.0" {
			t.Errorf("unexpected request: %s UA %q", r.URL, r.Header.Get("User-Agent"))
		}
		fmt.Fprint(w, body)
	}))
	defer server.Close()
	mapPath := filepath.Join(t.TempDir(), "map.toml")
	mapping := "[[pair]]\nbinance='BTCUSDT'\nvenue='mexc'\nsymbol='BTC_USDT'\nexp10=0\nstatus='auto'\n"
	if err := os.WriteFile(mapPath, []byte(mapping), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := PollConfig{MapPath: mapPath, Venues: map[string]VenueConfig{"mexc": {BaseURL: server.URL}}}
	env, channels, err := NewVenue("mexc", cfg, Deps{Doer: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if len(channels) != 1 {
		t.Fatalf("channels: %d", len(channels))
	}
	c := channels[0]
	if c.Name != "contractDetail" || !c.Daily || c.PerSymbol || c.Period != 24*time.Hour || c.RPS != 1 {
		t.Fatalf("channel: %+v", c)
	}
	if len(env.Universe()) != 1 {
		t.Fatalf("universe: %v", env.Universe())
	}
	records, err := c.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if requests != 1 || len(records) != 1 || records[0].Symbol != "BTC_USDT" {
		t.Fatalf("requests %d records %+v", requests, records)
	}
	var contract map[string]json.RawMessage
	if err := json.Unmarshal(records[0].Payload, &contract); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"symbol", "contractSize", "priceUnit", "state", "openingTime", "riskLimitMode", "riskLimitCustom"} {
		if _, ok := contract[key]; !ok {
			t.Errorf("missing %s", key)
		}
	}
	for _, bad := range []string{`{"success":false,"code":0,"data":[]}`, `{"success":true,"code":123,"data":[]}`, `{"success":true,"code":0,"data":null}`, `{"success":true,"code":0,"data":[{}]}`} {
		body = bad
		if _, err := c.Fetch(context.Background(), ""); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}
