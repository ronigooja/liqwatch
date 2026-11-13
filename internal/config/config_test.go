package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaultsAndValidation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	contents := "data_dir = \"/tmp/liqwatch\"\n"
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Archive.MaxPackMB != 400 || c.Archive.VerifySamplePercent != 10 || c.Pack.Unit != "tar_then_zstd" || c.Pack.Level != 3 || c.Pack.QuietMinutes != 10 || c.Checks.DiskWarnPct != 80 {
		t.Fatalf("defaults not applied: %+v", c)
	}
	if c.Collect.Venue != "binance" || c.Checks.StaleSec["aggTrade"] != 120 || c.Checks.StaleSec["markPrice_all"] != 60 || c.Checks.StaleSec["forceOrder_all"] != 0 {
		t.Fatalf("bad defaults: %+v", c)
	}
	if c.Collect.MapPath != "/etc/liqwatch/venue-map.toml" {
		t.Fatal(c.Collect.MapPath)
	}
	if err := os.WriteFile(path, []byte("[collect]\nmap_path = \"/tmp/map.toml\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err = Load(path)
	if err != nil || c.Collect.MapPath != "/tmp/map.toml" {
		t.Fatalf("map override: %+v %v", c, err)
	}
	bad := filepath.Join(dir, "bad.toml")
	if err := os.WriteFile(bad, []byte("[pack]\nlevel=23\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(bad); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestVerifySamplePercent(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{{"-1", false}, {"0", true}, {"100", true}, {"101", false}} {
		t.Run(tc.value, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(p, []byte("[archive]\nverify_sample_percent="+tc.value+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(p)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v: %v", tc.valid, err)
			}
		})
	}
}
