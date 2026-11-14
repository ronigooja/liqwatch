package kucoin

import "time"

func (*Adapter) Throttles() map[string]time.Duration {
	return map[string]time.Duration{
		"mark":    1 * time.Second,
		"utamark": 1 * time.Second,
	}
}
