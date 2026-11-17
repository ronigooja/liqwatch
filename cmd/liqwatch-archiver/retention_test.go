package main

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
)

func retentionFixture(t *testing.T) (config.Config, *manifest.SQLiteStore) {
	t.Helper()
	cfg := config.Config{DataDir: t.TempDir(), Pack: config.Pack{Dir: t.TempDir()}, Archive: config.Archive{Retention: true, RetentionHighPct: 90, RetentionLowPct: 80, RetentionMaxDaysPerRun: 3}}
	s, err := manifest.Open(filepath.Join(t.TempDir(), "manifest.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	old := archiveUsage
	t.Cleanup(func() { archiveUsage = old })
	return cfg, s
}

func retentionPack(t *testing.T, cfg config.Config, s *manifest.SQLiteStore, day int, sym string, state manifest.State) {
	t.Helper()
	path := fmt.Sprintf("v/%s/%08d.tar", sym, day)
	writeLocal(t, filepath.Join(cfg.PacksPath(), path), "packed")
	if err := s.RegisterPack(manifest.Pack{Venue: "v", Symbol: sym, Day: day, Path: path, Bytes: 6, State: state, ClosedAt: localNow, Members: []manifest.Member{{Name: "trade.jsonl", Bytes: 3, SHA256: "history"}}}); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionWholeDayGolden(t *testing.T) {
	cfg, s := retentionFixture(t)
	retentionPack(t, cfg, s, 20261113, "a", manifest.StateLocalDeleted)
	retentionPack(t, cfg, s, 20261113, "b", manifest.StateLocalDeleted)
	retentionPack(t, cfg, s, 20261114, "a", manifest.StateLocalDeleted)
	retentionPack(t, cfg, s, 20261116, "a", manifest.StateLocalDeleted)
	values := []float64{95, 85, 79}
	calls := 0
	archiveUsage = func(string) (float64, error) { v := values[calls]; calls++; return v, nil }
	got, err := localLog(t, func() error { return retainLocal(cfg, s) })
	want := "local pruned day=20261113 packs=2 bytes=12\nlocal pruned day=20261114 packs=1 bytes=6\n"
	if err != nil || got != want {
		t.Fatalf("got=(%q,%v) want=%q", got, err, want)
	}
	wantFiles := []string{"v/", "v/a/", "v/a/20261116.tar=packed", "v/b/"}
	if got := listing(t, cfg.PacksPath()); !reflect.DeepEqual(got, wantFiles) {
		t.Fatal(got, wantFiles)
	}
	ps, err := s.ListByState(manifest.StatePruned)
	for i := range ps {
		ps[i].ClosedAt = ps[i].ClosedAt.UTC()
	}
	wantPacks := []manifest.Pack{
		{ID: 1, Venue: "v", Symbol: "a", Day: 20261113, Path: "v/a/20261113.tar", Bytes: 6, MemberCount: 1, State: manifest.StatePruned, ClosedAt: localNow},
		{ID: 2, Venue: "v", Symbol: "b", Day: 20261113, Path: "v/b/20261113.tar", Bytes: 6, MemberCount: 1, State: manifest.StatePruned, ClosedAt: localNow},
		{ID: 3, Venue: "v", Symbol: "a", Day: 20261114, Path: "v/a/20261114.tar", Bytes: 6, MemberCount: 1, State: manifest.StatePruned, ClosedAt: localNow},
	}
	if err != nil || !reflect.DeepEqual(ps, wantPacks) {
		t.Fatal(ps, err)
	}
	for _, p := range ps {
		ms, err := s.Members(p.ID)
		if err != nil || !reflect.DeepEqual(ms, []manifest.Member{{Name: "trade.jsonl", Bytes: 3, SHA256: "history"}}) {
			t.Fatal(ms, err)
		}
	}
	archiveUsage = func(string) (float64, error) { return 79, nil }
	got, err = localLog(t, func() error { return retainLocal(cfg, s) })
	if err != nil || got != "" {
		t.Fatal(got, err)
	}
}

func TestRetentionCapGolden(t *testing.T) {
	cfg, s := retentionFixture(t)
	for day := 20261111; day <= 20261116; day++ {
		retentionPack(t, cfg, s, day, "s", manifest.StateLocalDeleted)
	}
	archiveUsage = func(string) (float64, error) { return 95, nil }
	got, err := localLog(t, func() error { return retainLocal(cfg, s) })
	want := "local pruned day=20261111 packs=1 bytes=6\nlocal pruned day=20261112 packs=1 bytes=6\nlocal pruned day=20261113 packs=1 bytes=6\nWARNING local retention usage=95.0% remains above high=90% pruned_days=3 cap=3\n"
	if err == nil || got != want {
		t.Fatal(got, err)
	}
	ps, e := s.ListByState(manifest.StatePruned)
	if e != nil || len(ps) != 3 {
		t.Fatal(ps, e)
	}
}

func TestRetentionSafetyGolden(t *testing.T) {
	cfg, s := retentionFixture(t)
	retentionPack(t, cfg, s, 20261111, "unverified", manifest.StateClosed)
	retentionPack(t, cfg, s, 20261112, "raw", manifest.StateLocalDeleted)
	retentionPack(t, cfg, s, 20261113, "pending", manifest.StateLocalVerified)
	retentionPack(t, cfg, s, 20261114, "deleting", manifest.StateLocalDeleted)
	retentionPack(t, cfg, s, 20261116, "newest", manifest.StateLocalDeleted)
	writeLocal(t, filepath.Join(cfg.DataDir, "raw/v/raw/20261112/late.jsonl"), "new")
	writeLocal(t, filepath.Join(cfg.DataDir, "raw/v/deleting/20261114.deleting/late.jsonl"), "new")
	writeLocal(t, filepath.Join(cfg.PacksPath(), "v/unknown/20261110.tar"), "unknown")
	before := listing(t, cfg.PacksPath())
	archiveUsage = func(string) (float64, error) { return 95, nil }
	got, err := localLog(t, func() error { return retainLocal(cfg, s) })
	want := "WARNING local retention unknown pack=v/unknown/20261110.tar; untouched\nWARNING local retention usage=95.0% remains above high=90% pruned_days=0 cap=3\n"
	if err == nil || got != want {
		t.Fatal(got, err)
	}
	if after := listing(t, cfg.PacksPath()); !reflect.DeepEqual(after, before) {
		t.Fatal(after, before)
	}
}

func TestRetentionDisabledAndBelowThreshold(t *testing.T) {
	for _, disabled := range []bool{true, false} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			cfg, s := retentionFixture(t)
			cfg.Archive.Retention = !disabled
			retentionPack(t, cfg, s, 20261111, "s", manifest.StateLocalDeleted)
			retentionPack(t, cfg, s, 20261116, "s", manifest.StateLocalDeleted)
			calls := 0
			archiveUsage = func(string) (float64, error) {
				calls++
				if disabled {
					return 95, nil
				}
				return 89, nil
			}
			before := listing(t, cfg.PacksPath())
			got, err := localLog(t, func() error { return retainLocal(cfg, s) })
			if err != nil || got != "" {
				t.Fatal(got, err)
			}
			if disabled && calls != 0 {
				t.Fatal("disabled retention measured disk")
			}
			if after := listing(t, cfg.PacksPath()); !reflect.DeepEqual(after, before) {
				t.Fatal(after, before)
			}
		})
	}
}

func TestRetentionUnknownNewestProtected(t *testing.T) {
	cfg, s := retentionFixture(t)
	retentionPack(t, cfg, s, 20261116, "s", manifest.StateLocalDeleted)
	archiveUsage = func(string) (float64, error) { return 95, nil }
	if err := retainLocal(cfg, s); err == nil {
		t.Fatal("expected pressure warning")
	}
	if _, err := os.Stat(filepath.Join(cfg.PacksPath(), "v/s/20261116.tar")); err != nil {
		t.Fatal(err)
	}
}
