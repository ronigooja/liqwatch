package poller

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fixtureGoldenRecord struct {
	Fixture                string
	Venue, Channel, Symbol string
	RecvWall               int64
	Meta                   map[string]any
	Payload                json.RawMessage
}

// TestFixtureRecordGolden uses the generic snapshot producer on every captured
// response. The golden was generated with commit 71267c2 (before policies).
func TestFixtureRecordGolden(t *testing.T) {
	withoutPolicies(t)
	encoded := fixtureGolden(t, "*")
	golden := "testdata/records-golden.jsonl.txt"
	if os.Getenv("GENERATE_FIXTURE_GOLDEN") == "1" {
		if err := os.WriteFile(golden, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, want) {
		t.Fatal("fixture records differ from pre-policy golden")
	}
}

func fixtureGolden(t *testing.T, venue string) []byte {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("testdata", venue, "*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatal(err)
	}
	var got []fixtureGoldenRecord
	clock := &fakeClock{time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)}
	for _, path := range paths {
		venue := filepath.Base(filepath.Dir(path))
		channel := strings.TrimSuffix(filepath.Base(path), ".json")
		if !policiesDisabled {
			var ok bool
			channel, ok = fixtureChannels[venue][filepath.Base(path)]
			if !ok {
				t.Fatalf("unmapped fixture: %s", path)
			}
			if channel == "unregistered" {
				continue
			}
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !policiesDisabled && (venue == "mexc" && channel == "contractDetail" || venue == "bybit" && channel == "accountRatio") {
			var envelope struct {
				Data   []json.RawMessage `json:"data"`
				Result struct {
					List []json.RawMessage `json:"list"`
				} `json:"result"`
			}
			if err := json.Unmarshal(raw, &envelope); err != nil {
				t.Fatal(err)
			}
			rows := envelope.Data
			if venue == "bybit" {
				rows = envelope.Result.List
			}
			if len(rows) == 0 {
				t.Fatalf("no rows in %s", path)
			}
			raw = rows[0]
		}
		fetch := SnapshotFetch(venue, channel, clock, SnapshotSpec{Mode: SnapshotPerSymbol, Fetch: func(context.Context, string) ([]byte, error) { return raw, nil }})
		sink := &memorySink{}
		channelSpec := Channel{Venue: venue, Name: channel, Fetch: func(ctx context.Context, _ string) ([]Record, error) { return fetch(ctx, "FIXTURE") }}
		scheduler, err := NewScheduler([]Channel{channelSpec}, sink, clock, "", 240)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := scheduler.Step(context.Background()); err != nil {
			t.Fatal(path, err)
		}
		for _, r := range sink.records {
			got = append(got, fixtureGoldenRecord{path, r.Venue, r.Channel, r.Symbol, r.RecvWall, r.Meta, r.Payload})
		}
	}
	encoded, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(encoded, '\n')
}
