package mexc

import "time"

func (*Adapter) Throttles() map[string]time.Duration {
	return map[string]time.Duration{
		"mark":    1 * time.Second,
		"index":   1 * time.Second,
		"funding": 60 * time.Second,
		"ticker":  1 * time.Second,
	}
}
