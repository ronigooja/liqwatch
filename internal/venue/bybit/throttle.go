package bybit

import "time"

func (*Adapter) Throttles() map[string]time.Duration {
	return map[string]time.Duration{
		"mark": 1 * time.Second,
	}
}
