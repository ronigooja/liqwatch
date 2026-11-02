// Package config holds the shared configuration structure, loaded from /etc/wicker/config.toml.
// Secrets are not in this file; they come from the environment (/etc/wicker/secrets.env).
package config

import (
	"fmt"
	"os"

	"github.com/pelletier/go-toml/v2"
)

type Config struct {
	DataDir  string   `toml:"data_dir"` // default /var/lib/wicker
	Collect  Collect  `toml:"collect"`
	Snapshot Snapshot `toml:"snapshot"`
	Archive  Archive  `toml:"archive"`
	Checks   Checks   `toml:"checks"`
}

type Collect struct {
	MetricsAddr     string   `toml:"metrics_addr"`       // e.g. 127.0.0.1:9101
	QuoteTopN       int      `toml:"quote_top_n"`        // bookTicker for top N by volume; 50
	DepthSymbols    []string `toml:"depth_symbols"`      // owner-edited list, 10-20
	DepthSpeed      string   `toml:"depth_speed"`        // "100ms"
	MaxStreamsPerWS int      `toml:"max_streams_per_ws"` // 100
	QueueSize       int      `toml:"queue_size"`
}

type Snapshot struct {
	Limit          int     `toml:"limit"`            // 1000
	VerifyEveryMin int     `toml:"verify_every_min"` // 5-10
	WeightFraction float64 `toml:"weight_fraction"`  // share of REST weight/min budget, e.g. 0.25
}

type Archive struct {
	RemoteRoot     string `toml:"remote_root"`      // https://.../dav/raw_data/ronigooja_onedrive/wicker/
	ChunkMB        int    `toml:"chunk_mb"`         // 64
	DeleteAfterH   int    `toml:"delete_after_h"`   // 6, counted from verified
	FullVerifyDays int    `toml:"full_verify_days"` // 7: download-and-hash
	MaxKBps        int    `toml:"max_kbps"`
}

type Checks struct {
	DiskWarnPct     int `toml:"disk_warn_pct"`    // 80
	StaleWriteSec   int `toml:"stale_write_sec"`  // newest .part not updated
	UnverifiedHours int `toml:"unverified_hours"` // 3
}

// Env var names for WebDAV credentials (never printed or logged).
const (
	EnvWebDAVUser = "WICKER_WEBDAV_USER"
	EnvWebDAVPass = "WICKER_WEBDAV_PASS"
)

func Load(path string) (*Config, error) {
	c := Config{
		DataDir:  "/var/lib/wicker",
		Collect:  Collect{MetricsAddr: "127.0.0.1:9101", QuoteTopN: 50, DepthSpeed: "100ms", MaxStreamsPerWS: 100, QueueSize: 10000},
		Snapshot: Snapshot{Limit: 1000, VerifyEveryMin: 5, WeightFraction: 0.25},
		Archive:  Archive{ChunkMB: 64, DeleteAfterH: 6, FullVerifyDays: 7, MaxKBps: 0},
		Checks:   Checks{DiskWarnPct: 80, StaleWriteSec: 120, UnverifiedHours: 3},
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
	if c.Collect.QuoteTopN <= 0 || len(c.Collect.DepthSymbols) < 1 || len(c.Collect.DepthSymbols) > 20 {
		return fmt.Errorf("collect.quote_top_n must be positive and depth_symbols must contain 1-20 entries")
	}
	for i, symbol := range c.Collect.DepthSymbols {
		if symbol == "" {
			return fmt.Errorf("collect.depth_symbols[%d] is empty", i)
		}
	}
	if c.Collect.DepthSpeed == "" || c.Collect.MaxStreamsPerWS <= 0 || c.Collect.QueueSize <= 0 {
		return fmt.Errorf("collect depth_speed, max_streams_per_ws, and queue_size must be positive")
	}
	if c.Snapshot.Limit <= 0 || c.Snapshot.VerifyEveryMin <= 0 || c.Snapshot.WeightFraction <= 0 || c.Snapshot.WeightFraction > 1 {
		return fmt.Errorf("snapshot values are invalid")
	}
	if c.Archive.ChunkMB <= 0 || c.Archive.DeleteAfterH < 0 || c.Archive.FullVerifyDays < 0 {
		return fmt.Errorf("archive values are invalid")
	}
	if c.Checks.DiskWarnPct < 1 || c.Checks.DiskWarnPct > 100 || c.Checks.StaleWriteSec <= 0 || c.Checks.UnverifiedHours <= 0 {
		return fmt.Errorf("checks values are invalid")
	}
	return nil
}
