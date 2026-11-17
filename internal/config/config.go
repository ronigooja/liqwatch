// Package config holds the shared configuration structure, loaded from /etc/liqwatch/config.toml.
// Secrets are not in this file; they come from the environment (/etc/liqwatch/secrets.env).
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

type Config struct {
	DataDir string  `toml:"data_dir"` // default /var/lib/liqwatch
	Collect Collect `toml:"collect"`
	Archive Archive `toml:"archive"`
	Pack    Pack    `toml:"pack"`
	Checks  Checks  `toml:"checks"`
	Poller  Poller  `toml:"poller"`
	Record  Record  `toml:"record"`
}

type Record struct {
	Compress bool   `toml:"compress"`
	Level    int    `toml:"level"`
	Flush    string `toml:"flush"`
}

// FlushEvery resolves a validated record setting against the caller's legacy tick.
func (r Record) FlushEvery(legacy time.Duration) time.Duration {
	if r.Flush != "" {
		d, _ := time.ParseDuration(r.Flush)
		return d
	}
	if r.Compress {
		return 30 * time.Second
	}
	return legacy
}

// Poller is the validated subset of [poller] shared with cmd/liqwatch-poller, which decodes the full table
// into poller.PollConfig.
type Poller struct {
	MapPath         string                 `toml:"map_path"` // venue map for per-venue symbol universes; default collect.map_path
	Overlap         string                 `toml:"overlap"`
	InitialLookback string                 `toml:"initial_lookback"`
	Venues          map[string]PollerVenue `toml:"venues"`
}

// PollerVenue is one [poller.venues.<name>] table.
type PollerVenue struct {
	RPS             float64            `toml:"rps"`               // requests per second per endpoint
	WeightPerMinute float64            `toml:"weight_per_minute"` // request budget per minute
	BaseURL         string             `toml:"base_url"`          // overrides the venue default
	Periods         map[string]string  `toml:"periods"`           // channel -> period
	ChannelRPS      map[string]float64 `toml:"channel_rps"`
	Weights         map[string]float64 `toml:"weights"`
	Overlap         string             `toml:"overlap"`
	InitialLookback string             `toml:"initial_lookback"`
}

var pollerVenues = map[string]bool{"binance": true, "bybit": true, "okx": true, "bitget": true, "gate": true, "mexc": true, "kucoin": true}

func (p Poller) validate() error {
	dur := func(what, v string) error {
		if v == "" {
			return nil
		}
		if d, err := time.ParseDuration(v); err != nil || d <= 0 {
			return fmt.Errorf("poller %s %q is not a positive duration", what, v)
		}
		return nil
	}
	if err := dur("overlap", p.Overlap); err != nil {
		return err
	}
	if err := dur("initial_lookback", p.InitialLookback); err != nil {
		return err
	}
	for name, v := range p.Venues {
		if !pollerVenues[name] {
			return fmt.Errorf("unknown poller venue %q", name)
		}
		if v.RPS < 0 || v.WeightPerMinute < 0 {
			return fmt.Errorf("poller.venues.%s: rps and weight_per_minute must not be negative", name)
		}
		for _, d := range []struct{ what, v string }{{"overlap", v.Overlap}, {"initial_lookback", v.InitialLookback}} {
			if err := dur("venues."+name+"."+d.what, d.v); err != nil {
				return err
			}
		}
		for k, d := range v.Periods {
			if err := dur("venues."+name+".periods."+k, d); err != nil {
				return err
			}
		}
		for _, m := range []map[string]float64{v.ChannelRPS, v.Weights} {
			for k, f := range m {
				if f < 0 {
					return fmt.Errorf("poller.venues.%s: %s must not be negative", name, k)
				}
			}
		}
	}
	return nil
}

type Collect struct {
	MetricsAddr     string         `toml:"metrics_addr"`        // e.g. 127.0.0.1:9101
	PprofAddr       string         `toml:"pprof_addr"`          // empty disables profiling
	ExtraMetrics    []string       `toml:"extra_metrics_addrs"` // other slots checked too, e.g. 127.0.0.1:9102
	Venue           string         `toml:"venue"`
	MapPath         string         `toml:"map_path"`
	MaxStreamsPerWS int            `toml:"max_streams_per_ws"` // 100
	QueueSize       int            `toml:"queue_size"`
	OpenSpacingMS   int            `toml:"open_spacing_ms"` // gap between opening shard connections; 500
	Throttle        map[string]any `toml:"throttle"`
}

type Archive struct {
	Retention              bool   `toml:"retention"`
	RetentionHighPct       int    `toml:"retention_high_pct"`
	RetentionLowPct        int    `toml:"retention_low_pct"`
	RetentionMaxDaysPerRun int    `toml:"retention_max_days_per_run"`
	Mode                   string `toml:"mode"`           // webdav (default) or local
	DeleteSources          bool   `toml:"delete_sources"` // local only; default true
	RemoteRoot             string `toml:"remote_root"`    // https://.../dav/ronigooja_onedrive/liqwatch/
	MaxPackMB              int    `toml:"max_pack_mb"`
	FullVerifyDays         int    `toml:"full_verify_days"` // 7: download-and-hash
	VerifySamplePercent    int    `toml:"verify_sample_percent"`
	MaxKBps                int    `toml:"max_kbps"`
}

type Pack struct {
	Dir          string `toml:"dir"` // empty uses DataDir for packs and manifest
	Unit         string `toml:"unit"`
	Level        int    `toml:"level"`
	QuietMinutes int    `toml:"quiet_minutes"`
}

// PackDir is the shared output/state root; raw recording always uses DataDir.
func (c Config) PackDir() string {
	if c.Pack.Dir != "" {
		return filepath.Clean(c.Pack.Dir)
	}
	return c.DataDir
}

func (c Config) PacksPath() string    { return filepath.Join(c.PackDir(), "packs") }
func (c Config) ManifestPath() string { return filepath.Join(c.PackDir(), "manifest", "manifest.db") }

type Checks struct {
	DiskWarnPct     int            `toml:"disk_warn_pct"` // 80
	StaleSec        map[string]int `toml:"stale_sec"`
	UnverifiedHours int            `toml:"unverified_hours"` // 3
}

// Env var names for WebDAV credentials (never printed or logged).
const (
	EnvWebDAVUser = "LIQWATCH_WEBDAV_USER"
	EnvWebDAVPass = "LIQWATCH_WEBDAV_PASS"
)

func Load(path string) (*Config, error) {
	c := Config{
		DataDir: "/var/lib/liqwatch",
		Record:  Record{Level: 1},
		Collect: Collect{Venue: "binance", MapPath: "/etc/liqwatch/venue-map.toml", MetricsAddr: "127.0.0.1:9101", MaxStreamsPerWS: 100, QueueSize: 10000, OpenSpacingMS: 500, Throttle: map[string]any{"enabled": true}},
		Archive: Archive{Mode: "webdav", DeleteSources: true, Retention: true, RetentionHighPct: 90, RetentionLowPct: 80, RetentionMaxDaysPerRun: 3, MaxPackMB: 400, FullVerifyDays: 7, VerifySamplePercent: 10},
		Pack:    Pack{Unit: "tar_then_zstd", Level: 3, QuietMinutes: 10},
		Checks:  Checks{DiskWarnPct: 80, StaleSec: map[string]int{"aggTrade": 120, "markPrice_all": 60, "forceOrder_all": 0}, UnverifiedHours: 3},
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := toml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	if c.Poller.MapPath == "" {
		c.Poller.MapPath = c.Collect.MapPath
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c Config) validate() error {
	if c.Archive.RetentionLowPct <= 0 || c.Archive.RetentionLowPct >= c.Archive.RetentionHighPct || c.Archive.RetentionHighPct > 99 || c.Archive.RetentionMaxDaysPerRun <= 0 {
		return fmt.Errorf("archive retention requires 0 < low < high <= 99 and max_days_per_run > 0")
	}
	if c.Archive.Mode != "webdav" && c.Archive.Mode != "local" {
		return fmt.Errorf("archive.mode must be webdav or local")
	}
	if c.Record.Flush != "" {
		d, err := time.ParseDuration(c.Record.Flush)
		if err != nil || d <= 0 || d > 5*time.Minute {
			return fmt.Errorf("record.flush %q must be a positive Go duration no greater than 5m", c.Record.Flush)
		}
	}
	if c.Record.Level < 1 || c.Record.Level > 22 {
		return fmt.Errorf("record.level must be 1..22")
	}
	if c.Pack.Dir != "" {
		if !filepath.IsAbs(c.Pack.Dir) {
			return fmt.Errorf("pack.dir must be absolute")
		}
		raw, err := filepath.Abs(filepath.Join(c.DataDir, "raw"))
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(raw, filepath.Clean(c.Pack.Dir))
		if err != nil {
			return err
		}
		if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return fmt.Errorf("pack.dir must not be inside data_dir/raw")
		}
	}
	for key, value := range c.Collect.Throttle {
		if key == "enabled" {
			if _, ok := value.(bool); !ok {
				return fmt.Errorf("collect.throttle.enabled must be a boolean")
			}
			continue
		}
		s, ok := value.(string)
		if !ok || key == "" {
			return fmt.Errorf("invalid collect.throttle interval %q", key)
		}
		d, err := time.ParseDuration(s)
		if err != nil || d < 0 {
			return fmt.Errorf("invalid collect.throttle interval %q", key)
		}
	}
	if err := c.Poller.validate(); err != nil {
		return err
	}
	if c.DataDir == "" || c.Collect.MetricsAddr == "" {
		return fmt.Errorf("data_dir and collect.metrics_addr are required")
	}
	if c.Collect.Venue == "" || c.Collect.MaxStreamsPerWS <= 0 || c.Collect.QueueSize <= 0 || c.Collect.OpenSpacingMS < 0 {
		return fmt.Errorf("collect values are invalid")
	}
	if c.Archive.MaxPackMB <= 0 || c.Archive.FullVerifyDays < 0 || c.Archive.MaxKBps < 0 || c.Archive.VerifySamplePercent < 0 || c.Archive.VerifySamplePercent > 100 {
		return fmt.Errorf("archive values are invalid")
	}
	if (c.Pack.Unit != "tar_then_zstd" && c.Pack.Unit != "zstd_then_tar") || c.Pack.Level < 1 || c.Pack.Level > 22 || c.Pack.QuietMinutes <= 0 {
		return fmt.Errorf("pack values are invalid")
	}
	if c.Checks.DiskWarnPct < 1 || c.Checks.DiskWarnPct > 100 || c.Checks.UnverifiedHours <= 0 {
		return fmt.Errorf("checks values are invalid")
	}
	for channel, seconds := range c.Checks.StaleSec {
		if channel == "" || seconds < 0 {
			return fmt.Errorf("checks.stale_sec values are invalid")
		}
	}
	return nil
}
