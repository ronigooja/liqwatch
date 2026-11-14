package bitget

import "time"

func (*Adapter) Throttles() map[string]time.Duration {
	return map[string]time.Duration{
		"ticker":  1 * time.Second,
		"funding": 60 * time.Second,
	}
}
