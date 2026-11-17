package main

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
	"github.com/ronigooja/liqwatch/internal/pack"
	"github.com/ronigooja/liqwatch/internal/webdav"
)

var localNow = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

func localFixture(t *testing.T, unit string, extra ...bool) (config.Config, *manifest.SQLiteStore, manifest.Pack, string, string) {
	t.Helper()
	cfg := config.Config{DataDir: t.TempDir(), Pack: config.Pack{Dir: t.TempDir(), QuietMinutes: 10}, Archive: config.Archive{Mode: "local", DeleteSources: true}}
	raw := filepath.Join(cfg.DataDir, "raw/v/s/20261116")
	name := "trade.a.jsonl"
	if unit == pack.StoredTar {
		name += ".zst"
	}
	writeLocal(t, filepath.Join(raw, name), "original\n")
	old := localNow.Add(-48 * time.Hour)
	if err := os.Chtimes(filepath.Join(raw, name), old, old); err != nil {
		t.Fatal(err)
	}
	if len(extra) != 0 && extra[0] {
		second := filepath.Join(raw, "second.jsonl")
		writeLocal(t, second, "second verified member\n")
		if err := os.Chtimes(second, old, old); err != nil {
			t.Fatal(err)
		}
	}
	if unit == pack.StoredTar {
		unit = pack.TarThenZstd
	}
	r, err := pack.Pack(filepath.Join(cfg.DataDir, "raw"), "v", "s", "20261116", pack.Options{PackRoot: cfg.PacksPath(), Unit: unit, Now: localNow})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(cfg.ManifestPath()), 0750); err != nil {
		t.Fatal(err)
	}
	s, err := manifest.Open(cfg.ManifestPath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	p := manifest.Pack{Venue: "v", Symbol: "s", Day: 20261116, Path: r.Path, Bytes: r.Bytes, SHA256: r.SHA256, ClosedAt: localNow}
	for _, m := range r.Members {
		p.Members = append(p.Members, manifest.Member{Name: m.Name, Bytes: m.Bytes, SHA256: m.SHA256, MtimeNS: m.MtimeNS})
	}
	if err = s.RegisterPack(p); err != nil {
		t.Fatal(err)
	}
	ps, err := s.ListByState(manifest.StateClosed)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, s, ps[0], raw, filepath.Join(cfg.PacksPath(), r.Path)
}

func writeLocal(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0640); err != nil {
		t.Fatal(err)
	}
}

func listing(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if d.IsDir() {
			out = append(out, rel+"/")
		} else {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			out = append(out, rel+"="+string(b))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func localLog(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	var b bytes.Buffer
	old, flags := log.Writer(), log.Flags()
	log.SetOutput(&b)
	log.SetFlags(0)
	defer func() { log.SetOutput(old); log.SetFlags(flags) }()
	err := fn()
	return b.String(), err
}

func TestLocalDeleteGolden(t *testing.T) {
	for _, unit := range []string{pack.TarThenZstd, pack.ZstdThenTar, pack.StoredTar} {
		t.Run(unit, func(t *testing.T) {
			cfg, s, _, raw, packed := localFixture(t, unit)
			writeLocal(t, filepath.Join(cfg.DataDir, "raw/v/s/20261115/keep.jsonl"), "other-day")
			writeLocal(t, filepath.Join(cfg.DataDir, "raw/v/other/20261116/keep.jsonl"), "other-symbol")
			beforePack, err := os.ReadFile(packed)
			if err != nil {
				t.Fatal(err)
			}
			got, err := localLog(t, func() error { return runLocal(context.Background(), cfg, s, localNow) })
			want := "local deleted day=v/s/20261116 members=1 source_bytes=9\nlocal verified=1 would-delete=1 blocked=0 deleted=1\n"
			if err != nil || got != want {
				t.Fatalf("result=(%q,%v), want=(%q,nil)", got, err, want)
			}
			wantListing := []string{"v/", "v/other/", "v/other/20261116/", "v/other/20261116/keep.jsonl=other-symbol", "v/s/", "v/s/20261115/", "v/s/20261115/keep.jsonl=other-day"}
			if got := listing(t, filepath.Join(cfg.DataDir, "raw")); !reflect.DeepEqual(got, wantListing) {
				t.Fatalf("listing=%q want=%q", got, wantListing)
			}
			afterPack, err := os.ReadFile(packed)
			if err != nil || !bytes.Equal(beforePack, afterPack) {
				t.Fatalf("pack altered: %v", err)
			}
			if _, err := os.Stat(raw); !os.IsNotExist(err) {
				t.Fatalf("raw remains: %v", err)
			}
			ps, err := s.ListByState(manifest.StateLocalDeleted)
			if err != nil || len(ps) != 1 {
				t.Fatalf("states=%+v %v", ps, err)
			}
			got, err = localLog(t, func() error { return runLocal(context.Background(), cfg, s, localNow) })
			if err != nil || got != "local verified=0 would-delete=0 blocked=0 deleted=0\n" {
				t.Fatalf("second run=%q %v", got, err)
			}
		})
	}
}

func TestLocalReportOnlyGolden(t *testing.T) {
	cfg, s, _, _, packed := localFixture(t, pack.TarThenZstd)
	cfg.Archive.DeleteSources = false
	before := listing(t, filepath.Join(cfg.DataDir, "raw"))
	packBefore, err := os.ReadFile(packed)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		got, err := localLog(t, func() error { return runLocal(context.Background(), cfg, s, localNow) })
		if err != nil || got != "local verified=1 would-delete=1 blocked=0 deleted=0\n" {
			t.Fatalf("result=%q %v", got, err)
		}
		if after := listing(t, filepath.Join(cfg.DataDir, "raw")); !reflect.DeepEqual(after, before) {
			t.Fatalf("before=%q after=%q", before, after)
		}
		packAfter, err := os.ReadFile(packed)
		if err != nil || !bytes.Equal(packAfter, packBefore) {
			t.Fatal("pack changed", err)
		}
	}
}

func TestLocalNoDAV(t *testing.T) {
	cfg, s, _, _, _ := localFixture(t, pack.StoredTar)
	s.Close()
	if err := runWithDAV(context.Background(), cfg, func(string, string, string) *webdav.Client { t.Fatal("local created DAV client"); return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestLocalVerificationFailuresGolden(t *testing.T) {
	cases := []string{"truncated", "pack-hash", "missing-tar-member", "source-changed", "late-file", "source-missing", "current-day", "quiet-period"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			cfg, s, p, raw, packed := localFixture(t, pack.TarThenZstd)
			ms, err := s.Members(p.ID)
			if err != nil {
				t.Fatal(err)
			}
			var wantReason string
			rule := false
			switch name {
			case "truncated":
				if err := os.Truncate(packed, 1); err != nil {
					t.Fatal(err)
				}
				wantReason = "pack checksum or size mismatch"
			case "pack-hash":
				b, err := os.ReadFile(packed)
				if err != nil {
					t.Fatal(err)
				}
				b[0] ^= 1
				if err = os.WriteFile(packed, b, 0640); err != nil {
					t.Fatal(err)
				}
				wantReason = "pack checksum or size mismatch"
			case "missing-tar-member":
				// Register a new, coherent manifest/pack hash whose expected member
				// is absent from the tar; outer hash alone must not authorize deletion.
				ms = append(ms, manifest.Member{Name: "missing.jsonl", Bytes: 9, SHA256: ms[0].SHA256})
				writeLocal(t, filepath.Join(raw, "missing.jsonl"), "original\n")
				old := localNow.Add(-48 * time.Hour)
				if err := os.Chtimes(filepath.Join(raw, "missing.jsonl"), old, old); err != nil {
					t.Fatal(err)
				}
				wantReason = "manifest member missing in tar"
			case "source-changed":
				writeLocal(t, filepath.Join(raw, ms[0].Name), "mutated!\n")
				old := time.Unix(0, ms[0].MtimeNS)
				if err := os.Chtimes(filepath.Join(raw, ms[0].Name), old, old); err != nil {
					t.Fatal(err)
				}
				wantReason = "source checksum/identity changed: trade.a.jsonl"
			case "late-file":
				writeLocal(t, filepath.Join(raw, "late.jsonl"), "late\n")
				wantReason = "archive blocked by rule: source-day leftover late.jsonl"
				rule = true
			case "source-missing":
				if err := os.Remove(filepath.Join(raw, ms[0].Name)); err != nil {
					t.Fatal(err)
				}
				wantReason = "source-missing"
			case "current-day":
				p.Day = 20261117
				newRaw := filepath.Join(filepath.Dir(raw), "20261117")
				if err := os.Rename(raw, newRaw); err != nil {
					t.Fatal(err)
				}
				raw = newRaw
				p.Path = "v/s/20261117.tar.zst"
				newPack := filepath.Join(cfg.PacksPath(), p.Path)
				if err := os.Rename(packed, newPack); err != nil {
					t.Fatal(err)
				}
				packed = newPack
				wantReason = "archive blocked by rule: current/future UTC day"
				rule = true
			case "quiet-period":
				recent := localNow.Add(-time.Minute)
				if err := os.Chtimes(filepath.Join(raw, ms[0].Name), recent, recent); err != nil {
					t.Fatal(err)
				}
				wantReason = "archive blocked by rule: quiet period trade.a.jsonl"
				rule = true
			}
			// Replace fixture manifest with the full test record, independent of SQL layout.
			s.Close()
			db := filepath.Join(t.TempDir(), "manifest.db")
			s, err = manifest.Open(db)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			p.ID = 0
			p.Members = ms
			p.State = manifest.StateClosed
			if err = s.RegisterPack(p); err != nil {
				t.Fatal(err)
			}
			before := listing(t, filepath.Join(cfg.DataDir, "raw"))
			got, err := localLog(t, func() error { return runLocal(context.Background(), cfg, s, localNow) })
			if name == "source-missing" {
				start := strings.Index(got, "reason=")
				end := strings.Index(got, "\n")
				if start < 0 || end < start {
					t.Fatal(got)
				}
				got = got[:start] + "reason=source-missing" + got[end:]
			}
			want := fmt.Sprintf("local blocked day=v/s/%08d reason=%s\nlocal verified=0 would-delete=0 blocked=1 deleted=0\n", p.Day, wantReason)
			if got != want || (err == nil) != rule {
				t.Fatalf("result=(%q,%v), want=(%q, rule=%v)", got, err, want, rule)
			}
			if after := listing(t, filepath.Join(cfg.DataDir, "raw")); !reflect.DeepEqual(before, after) {
				t.Fatalf("source changed before=%q after=%q", before, after)
			}
			if _, err := os.Stat(packed); err != nil {
				t.Fatal(err)
			}
			ps, err := s.ListByState(manifest.StateClosed)
			if err != nil || len(ps) != 1 {
				t.Fatalf("state=%+v %v", ps, err)
			}
		})
	}
}

func TestLocalTarMemberChecksum(t *testing.T) {
	cfg, s, p, _, _ := localFixture(t, pack.StoredTar)
	ms, err := s.Members(p.ID)
	if err != nil {
		t.Fatal(err)
	}
	// A valid-length but wrong member hash must fail independently of source checks.
	ms[0].SHA256 = strings.Repeat("0", 64)
	err = pack.VerifyLocal(filepath.Join(cfg.DataDir, "raw"), cfg.PacksPath(), p.Venue, p.Symbol, "20261116", p.Path, p.Bytes, p.SHA256, []pack.Member{{Name: ms[0].Name, Bytes: ms[0].Bytes, SHA256: ms[0].SHA256}}, 10*time.Minute, localNow, &pack.ReadLimiter{Context: context.Background()})
	if err == nil || err.Error() != "tar member checksum or size mismatch: trade.a.jsonl.zst" {
		t.Fatal(err)
	}
}

func TestLocalReverifiesBeforeDelete(t *testing.T) {
	cfg, s, _, raw, packed := localFixture(t, pack.TarThenZstd)
	cfg.Archive.DeleteSources = false
	if err := runLocal(context.Background(), cfg, s, localNow); err != nil {
		t.Fatal(err)
	}
	writeLocal(t, filepath.Join(raw, "late.jsonl"), "late")
	cfg.Archive.DeleteSources = true
	got, err := localLog(t, func() error { return runLocal(context.Background(), cfg, s, localNow) })
	want := "local blocked day=v/s/20261116 reason=archive blocked by rule: source-day leftover late.jsonl\nlocal verified=0 would-delete=0 blocked=1 deleted=0\n"
	if err != nil || got != want {
		t.Fatalf("result=%q %v", got, err)
	}
	if _, err := os.Stat(raw); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(packed); err != nil {
		t.Fatal(err)
	}
}

func TestLocalSourceMetadataAndMissingPack(t *testing.T) {
	for _, change := range []string{"mtime", "size", "missing-pack", "symlink"} {
		t.Run(change, func(t *testing.T) {
			cfg, s, p, raw, packed := localFixture(t, pack.StoredTar)
			ms, err := s.Members(p.ID)
			if err != nil {
				t.Fatal(err)
			}
			filename := filepath.Join(raw, ms[0].Name)
			switch change {
			case "mtime":
				old := localNow.Add(-24 * time.Hour)
				err = os.Chtimes(filename, old, old)
			case "size":
				writeLocal(t, filename, "short")
				old := time.Unix(0, ms[0].MtimeNS)
				err = os.Chtimes(filename, old, old)
			case "missing-pack":
				err = os.Remove(packed)
			case "symlink":
				err = os.Remove(filename)
				if err == nil {
					err = os.Symlink(packed, filename)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = runLocal(context.Background(), cfg, s, localNow); err == nil {
				t.Fatal("accepted changed source/pack")
			}
			if _, err = os.Stat(raw); err != nil {
				t.Fatal(err)
			}
			ps, err := s.ListByState(manifest.StateClosed)
			if err != nil || len(ps) != 1 {
				t.Fatal(ps, err)
			}
		})
	}
}
