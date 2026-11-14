package gate

import "time"

func (*Adapter) Throttles() map[string]time.Duration {
	return map[string]time.Duration{
		"tickers": 1 * time.Second,
	}
}
