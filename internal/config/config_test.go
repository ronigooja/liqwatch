package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pelletier/go-toml/v2"
)

func TestExamplePollerLookback(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "config", "config.toml.example"))
	if err != nil {
		t.Fatal(err)
	}
	var c struct {
		Poller struct {
			InitialLookback string `toml:"initial_lookback"`
		} `toml:"poller"`
	}
	if err := toml.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	if c.Poller.InitialLookback != "2h" {
		t.Fatalf("initial lookback = %q", c.Poller.InitialLookback)
	}
}

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
	if c.Collect.PprofAddr != "" {
		t.Fatalf("pprof enabled by default: %q", c.Collect.PprofAddr)
	}
	if err := os.WriteFile(path, []byte("[collect]\nmap_path = \"/tmp/map.toml\"\npprof_addr = \"127.0.0.1:6060\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err = Load(path)
	if err != nil || c.Collect.MapPath != "/tmp/map.toml" {
		t.Fatalf("map override: %+v %v", c, err)
	}
	if c.Collect.PprofAddr != "127.0.0.1:6060" {
		t.Fatalf("pprof override: %q", c.Collect.PprofAddr)
	}
	bad := filepath.Join(dir, "bad.toml")
	if err := os.WriteFile(bad, []byte("[pack]\nlevel=23\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(bad); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestThrottleConfig(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.toml")
	for _, tc := range []struct {
		text  string
		valid bool
	}{
		{"[collect.throttle]\nmark='1s'\n", true},
		{"[collect.throttle]\nenabled=false\nfunding='0s'\n", true},
		{"[collect.throttle]\nmark='bad'\n", false},
		{"[collect.throttle]\nenabled='yes'\n", false},
	} {
		if err := os.WriteFile(p, []byte(tc.text), 0600); err != nil {
			t.Fatal(err)
		}
		c, err := Load(p)
		if (err == nil) != tc.valid {
			t.Fatalf("%s: %v", tc.text, err)
		}
		if err == nil && tc.text == "[collect.throttle]\nmark='1s'\n" && c.Collect.Throttle["enabled"] != true {
			t.Fatal("enabled default")
		}
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

func TestPollerVenuesConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	write := func(s string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(s), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("[collect]\nmap_path = \"/m.toml\"\n[poller]\n[poller.venues.okx]\nrps = 2.5\nbase_url = \"http://x\"\nweight_per_minute = 100\n[poller.venues.okx.periods]\nopenInterest = \"1h\"\n")
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	v := c.Poller.Venues["okx"]
	if v.RPS != 2.5 || v.BaseURL != "http://x" || v.WeightPerMinute != 100 || v.Periods["openInterest"] != "1h" || c.Poller.MapPath != "/m.toml" {
		t.Fatalf("%+v", c.Poller)
	}
	write("[poller]\nmap_path = \"/p.toml\"\n")
	if c, err = Load(path); err != nil || c.Poller.MapPath != "/p.toml" {
		t.Fatalf("%+v %v", c.Poller, err)
	}
	for _, bad := range []string{
		"[poller.venues.nope]\nrps = 1\n",
		"[poller.venues.okx]\nrps = -1\n",
		"[poller.venues.okx.periods]\nx = \"soon\"\n",
		"[poller.venues.okx]\noverlap = \"-1h\"\n",
		"[poller]\ninitial_lookback = \"abc\"\n",
		"[poller.venues.okx.channel_rps]\nx = -2\n",
	} {
		write(bad)
		if _, err := Load(path); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}

func TestExampleConfigLoads(t *testing.T) {
	c, err := Load(filepath.Join("..", "..", "config", "config.toml.example"))
	if err != nil {
		t.Fatal(err)
	}
	if v := c.Poller.Venues["bybit"]; v.RPS != 2 || v.WeightPerMinute != 240 {
		t.Fatalf("%+v", c.Poller.Venues)
	}
}
