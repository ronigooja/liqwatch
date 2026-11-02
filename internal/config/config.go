// Package config holds the shared configuration structure, loaded from /etc/wicker/config.toml.
// Secrets are not in this file; they come from the environment (/etc/wicker/secrets.env).
package config

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
