package checks

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
)

func TestDiskRoots(t *testing.T) {
	c := &config.Config{DataDir: "/data/liqwatch", Pack: config.Pack{Dir: "/var/lib/liqwatch/pack"}, Checks: config.Checks{DiskWarnPct: 80}}
	for _, tc := range []struct {
		raw, pack float64
		want      []finding
	}{
		{81, 92, []finding{{"disk.high", "warning", "disk.high", "disk usage is high", map[string]any{"percent": float64(81), "path": "/data/liqwatch"}}, {"disk.high:pack", "warning", "disk.high", "disk usage is high", map[string]any{"percent": float64(92), "path": "/var/lib/liqwatch/pack"}}}},
		{79, 80, []finding{{"disk.high:pack", "warning", "disk.high", "disk usage is high", map[string]any{"percent": float64(80), "path": "/var/lib/liqwatch/pack"}}}},
		{80, 79, []finding{{"disk.high", "warning", "disk.high", "disk usage is high", map[string]any{"percent": float64(80), "path": "/data/liqwatch"}}}},
		{79, 79, nil},
	} {
		var paths []string
		got, err := diskFindings(c, func(path string) (float64, error) {
			paths = append(paths, path)
			if path == "/data/liqwatch" {
				return tc.raw, nil
			}
			return tc.pack, nil
		})
		if err != nil || !reflect.DeepEqual(got, tc.want) || !reflect.DeepEqual(paths, []string{"/data/liqwatch", "/var/lib/liqwatch/pack"}) {
			t.Fatalf("findings=%+v paths=%v err=%v", got, paths, err)
		}
	}
	c.Pack.Dir = ""
	var paths []string
	got, err := diskFindings(c, func(path string) (float64, error) { paths = append(paths, path); return 10, nil })
	if err != nil || got != nil || !reflect.DeepEqual(paths, []string{"/data/liqwatch"}) {
		t.Fatalf("default=%+v %v %v", got, paths, err)
	}
}

func TestEvaluateSeparateBacklog(t *testing.T) {
	c := &config.Config{DataDir: t.TempDir(), Pack: config.Pack{Dir: t.TempDir()}, Checks: config.Checks{DiskWarnPct: 100, UnverifiedHours: 3}}
	if err := os.MkdirAll(filepath.Dir(c.ManifestPath()), 0750); err != nil {
		t.Fatal(err)
	}
	s, err := manifest.Open(c.ManifestPath())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RegisterPack(manifest.Pack{Path: "v/s/20260101.tar.zst", ClosedAt: time.Now().Add(-4 * time.Hour), State: manifest.StateClosed}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	got, err := evaluate(c)
	if err != nil {
		t.Fatal(err)
	}
	var backlog []finding
	for _, f := range got {
		if f.Type == "archive.unverified" {
			backlog = append(backlog, f)
		}
	}
	want := []finding{{"archive.unverified", "warning", "archive.unverified", "closed files remain unverified", map[string]any{"count": 1}}}
	if !reflect.DeepEqual(backlog, want) {
		t.Fatalf("backlog=%+v", backlog)
	}
}
