// Package config holds the shared configuration structure, loaded from /etc/liqwatch/config.toml.
// Secrets are not in this file; they come from the environment (/etc/liqwatch/secrets.env).
package config

import (
	"fmt"
	"os"

	"github.com/pelletier/go-toml/v2"
)

type Config struct {
	DataDir string  `toml:"data_dir"` // default /var/lib/liqwatch
	Collect Collect `toml:"collect"`
	Archive Archive `toml:"archive"`
	Pack    Pack    `toml:"pack"`
	Checks  Checks  `toml:"checks"`
}

type Collect struct {
	MetricsAddr     string   `toml:"metrics_addr"`        // e.g. 127.0.0.1:9101
	ExtraMetrics    []string `toml:"extra_metrics_addrs"` // other slots checked too, e.g. 127.0.0.1:9102
	Venue           string   `toml:"venue"`
	MapPath         string   `toml:"map_path"`
	MaxStreamsPerWS int      `toml:"max_streams_per_ws"` // 100
	QueueSize       int      `toml:"queue_size"`
	OpenSpacingMS   int      `toml:"open_spacing_ms"` // gap between opening shard connections; 500
}

type Archive struct {
	RemoteRoot          string `toml:"remote_root"` // https://.../dav/ronigooja_onedrive/liqwatch/
	MaxPackMB           int    `toml:"max_pack_mb"`
	FullVerifyDays      int    `toml:"full_verify_days"` // 7: download-and-hash
	VerifySamplePercent int    `toml:"verify_sample_percent"`
	MaxKBps             int    `toml:"max_kbps"`
}

type Pack struct {
	Unit         string `toml:"unit"`
	Level        int    `toml:"level"`
	QuietMinutes int    `toml:"quiet_minutes"`
}

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
		Collect: Collect{Venue: "binance", MapPath: "/etc/liqwatch/venue-map.toml", MetricsAddr: "127.0.0.1:9101", MaxStreamsPerWS: 100, QueueSize: 10000, OpenSpacingMS: 500},
		Archive: Archive{MaxPackMB: 400, FullVerifyDays: 7, VerifySamplePercent: 10},
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
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c Config) validate() error {
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
