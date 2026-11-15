package poller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	okxvenue "github.com/ronigooja/liqwatch/internal/venue/okx"
)

func TestOKXPolicyClassification(t *testing.T) {
	cases := []struct{ channel, keep, drop string }{
		{"instruments", "instId instFamily ctVal ctMult ctValCcy ctType settleCcy lever", "instType uly groupId category instCategory baseCcy quoteCcy optType stk alias seriesId listTime expTime auctionEndTime contTdSwTime preMktSwTime openType state ruleType tickSz lotSz minSz maxLmtSz maxMktSz maxLmtAmt maxMktAmt maxTwapSz maxIcebergSz maxTriggerSz maxStopSz initPxLmtPct floatPxLmtPct maxPxLmtPct rpiMinLevel rpiMinPxBand futureSettlement tradeQuoteCcyList instIdCode upcChg freq method longPosRemainingQuota shortPosRemainingQuota maxPlatOICoinLmt maxPlatOILmt posLmtAmt posLmtPct"},
		{"positionTiers", "instFamily tier minSz maxSz mmr imr maxLever", "uly instId optMgnFactor baseMaxLoan quoteMaxLoan"},
	}
	for _, tc := range cases {
		t.Run(tc.channel, func(t *testing.T) {
			before := UnknownFields("okx", tc.channel)
			got, record, err := ApplyPolicy("okx", tc.channel, []byte(`{"data":[{"newField":1}]}`))
			if err != nil || !record || string(got) != `{"data":[{"newField":1}]}` || UnknownFields("okx", tc.channel)-before != 1 {
				t.Fatal("nested unknown not retained and counted")
			}
			for _, group := range []struct {
				names string
				keep  bool
			}{{tc.keep, true}, {tc.drop, false}} {
				for _, name := range strings.Fields(group.names) {
					raw := []byte(fmt.Sprintf(`{"code":"0","msg":"","data":[{%q:1.2300}],"novel":true}`, name))
					before := UnknownFields("okx", tc.channel)
					got, record, err := ApplyPolicy("okx", tc.channel, raw)
					if err != nil || !record {
						t.Fatalf("%s: %v", name, err)
					}
					want := `{"data":[{}],"novel":true}`
					if group.keep {
						want = fmt.Sprintf(`{"data":[{%q:1.2300}],"novel":true}`, name)
					}
					if string(got) != want || UnknownFields("okx", tc.channel)-before != 1 {
						t.Errorf("%s: %s unknown delta=%d", name, got, UnknownFields("okx", tc.channel)-before)
					}
				}
			}
		})
	}
}

func TestOKXCapturedRecords(t *testing.T) {
	for _, channel := range []string{"instruments", "positionTiers"} {
		t.Run(channel, func(t *testing.T) {
			b, err := os.ReadFile("testdata/okx/" + channel + "-record.txt")
			if err != nil {
				t.Fatal(err)
			}
			var rec struct {
				Payload json.RawMessage `json:"p"`
				Meta    json.RawMessage `json:"meta"`
			}
			if err = json.Unmarshal(b, &rec); err != nil {
				t.Fatal(err)
			}
			got, record, err := ApplyPolicy("okx", channel, rec.Payload)
			if err != nil || !record {
				t.Fatal(err)
			}
			want, err := os.ReadFile("testdata/okx/" + channel + "-filtered.txt")
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, bytes.TrimSpace(want)) {
				t.Fatal("whole payload differs")
			}
		})
	}
}

func TestOKXRatioFixturesUnfiltered(t *testing.T) {
	for _, tc := range []struct {
		n       int
		channel string
	}{{0, "longShortAccountRatioContract"}, {1, "longShortAccountRatioTopTrader"}, {2, "longShortPositionRatioTopTrader"}, {4, "longShortAccountRatio"}} {
		b, err := os.ReadFile(fmt.Sprintf("testdata/okx/response%d.json", tc.n))
		if err != nil {
			t.Fatal(err)
		}
		var e okxEnvelope
		if err = json.Unmarshal(b, &e); err != nil {
			t.Fatal(err)
		}
		var rows []json.RawMessage
		if err = json.Unmarshal(e.Data, &rows); err != nil || len(rows) == 0 {
			t.Fatal("missing rows")
		}
		policies.RLock()
		_, exists := policies.m[policyKey{"okx", tc.channel}]
		policies.RUnlock()
		if exists {
			t.Fatal("ratio policy must not be registered")
		}
		for _, row := range rows {
			var fields []json.RawMessage
			if json.Unmarshal(row, &fields) != nil || len(fields) != 2 {
				t.Fatalf("%s: %s", tc.channel, row)
			}
			got, record, err := ApplyPolicy("okx", tc.channel, row)
			if err != nil || !record || !bytes.Equal(row, got) {
				t.Fatal(tc.channel)
			}
		}
	}
}

func TestOKXDropTaker(t *testing.T) {
	got, record, err := ApplyPolicy("okx", "takerVolumeContract", []byte(`["1","2","3"]`))
	if err != nil || record || got != nil {
		t.Fatal("taker recorded")
	}
	clock := &fakeClock{time.UnixMilli(1000)}
	sink := &memorySink{}
	c := Channel{Venue: "okx", Name: "takerVolumeContract", Fetch: func(context.Context, string) ([]Record, error) {
		return []Record{{Venue: "okx", Channel: "takerVolumeContract", Payload: []byte(`["1","2","3"]`)}}, nil
	}}
	s, err := NewScheduler([]Channel{c}, sink, clock, "", 240)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Step(context.Background()); err != nil || len(sink.records) != 0 {
		t.Fatalf("sink=%v err=%v", sink.records, err)
	}
}

func TestOKXUniverseUsesFetchedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"code":"0","msg":"","data":[{"instId":"BTC-USDT-SWAP","instType":"SWAP","state":"live","ctType":"linear","settleCcy":"USDT","instFamily":"","uly":"BTC-USDT"}]}`)
	}))
	defer server.Close()
	a := &okxvenue.Adapter{InstrumentsURL: server.URL, HTTPClient: server.Client()}
	syms, err := a.Symbols(context.Background())
	if err != nil || len(syms) != 1 || syms[0] != "BTC-USDT-SWAP" {
		t.Fatalf("symbols=%v err=%v", syms, err)
	}
	shards, err := a.Shards(context.Background(), syms)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, sh := range shards {
		for _, stream := range sh.Streams {
			if strings.Contains(stream, `"channel":"index-tickers"`) && strings.Contains(stream, `"instId":"BTC-USDT"`) {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("index fallback not built from fetched uly")
	}
}

func TestOKXGolden(t *testing.T) { assertVenueGolden(t, "okx") }

func TestOKXDedup(t *testing.T) {
	row := func(s string) []Record {
		return []Record{{Venue: "okx", Channel: "instruments", Meta: map[string]any{"last_row_timestamp": int64(42)}, Payload: []byte(s)}}
	}
	assertVenueDedup(t, "okx", "instruments", venueDedupFixture{Original: row(`{"data":[{"instId":"A","state":"live"}]}`), Dropped: row(`{"data":[{"instId":"A","state":"suspended"}]}`), Kept: row(`{"data":[{"instId":"B","state":"live"}]}`)})
}

func TestOKXPolicyTimestamps(t *testing.T) {
	policies.RLock()
	defer policies.RUnlock()
	for _, ch := range []string{"instruments", "positionTiers", "takerVolumeContract"} {
		p, ok := policies.m[policyKey{"okx", ch}]
		if !ok || p.TimestampKey != "" || p.TimestampIndex != nil || p.TimestampPath != "" {
			t.Fatalf("unexpected timestamp registration: %s", ch)
		}
	}
}

// Each removed classification must change either the retained bytes or the
// unknown-field count checked independently by TestOKXPolicyClassification.
func TestOKXPolicyMutations(t *testing.T) {
	for _, channel := range []string{"instruments", "positionTiers"} {
		policies.RLock()
		p := policies.m[policyKey{"okx", channel}]
		policies.RUnlock()
		nested := p.Nested["data[]"]
		for _, group := range []struct {
			fields []string
			keep   bool
		}{{nested.Keep, true}, {nested.Drop, false}} {
			for i, name := range group.fields {
				mutated := nested
				fields := append([]string(nil), group.fields...)
				fields = append(fields[:i], fields[i+1:]...)
				if group.keep {
					mutated.Keep = fields
				} else {
					mutated.Drop = fields
				}
				raw := []byte(fmt.Sprintf(`{"data":[{%q:1.2300}],"novel":true}`, name))
				var unknown uint64
				got, err := filterPolicy(raw, p, map[string]Policy{"data[]": mutated}, "", &unknown)
				if err != nil {
					t.Fatal(err)
				}
				want := `{"data":[{}],"novel":true}`
				if group.keep {
					want = fmt.Sprintf(`{"data":[{%q:1.2300}],"novel":true}`, name)
				}
				if string(got) == want && unknown == 1 {
					t.Errorf("%s/%s mutation survived", channel, name)
				}
			}
		}
	}
}
