package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func init() { RegisterVenue("bybit", bybitChannels, WithBaseURL("https://api.bybit.com")) }

type bybitResponse struct {
	Code    *int   `json:"retCode"`
	Message string `json:"retMsg"`
	Result  struct {
		List   []json.RawMessage `json:"list"`
		Cursor string            `json:"nextPageCursor"`
	} `json:"result"`
}

func bybitGet(ctx context.Context, e *Env, path string, q url.Values) ([]byte, bybitResponse, error) {
	var v bybitResponse
	r, err := e.Get(ctx, path, q)
	if err != nil {
		return nil, v, err
	}
	if err = json.Unmarshal(r.Body, &v); err != nil {
		return nil, v, err
	}
	if v.Code == nil {
		return nil, v, fmt.Errorf("bybit: missing retCode")
	}
	return r.Body, v, nil
}

func bybitChannels(e *Env) []Channel {
	if e.Config.Venues["bybit"].RPS <= 0 {
		e.RPS = 5
	}
	if e.WeightPerMinute <= 0 {
		e.WeightPerMinute = 600
	}
	history := e.HistoryChannel("accountRatio", HistorySpec{Step: 5 * time.Minute, Mode: PageBackward, PageSize: 500, Period: 24 * time.Hour,
		Fetch: func(ctx context.Context, symbol string, start, end int64) ([]Row, error) {
			q := url.Values{"category": {"linear"}, "symbol": {symbol}, "period": {"5min"}, "startTime": {strconv.FormatInt(start, 10)}, "endTime": {strconv.FormatInt(end, 10)}, "limit": {"500"}}
			_, v, err := bybitGet(ctx, e, "/v5/market/account-ratio", q)
			if err != nil {
				return nil, err
			}
			if *v.Code != 0 {
				if *v.Code == 10001 && strings.Contains(strings.ToLower(v.Message), "symbol not support") {
					return nil, ErrHistoryUnavailable
				}
				return nil, fmt.Errorf("bybit account-ratio: %d: %s", *v.Code, v.Message)
			}
			if v.Result.List == nil {
				return nil, fmt.Errorf("bybit account-ratio: missing list")
			}
			rows := make([]Row, 0, len(v.Result.List))
			for _, raw := range v.Result.List {
				var item struct {
					Timestamp string `json:"timestamp"`
				}
				if err := json.Unmarshal(raw, &item); err != nil {
					return nil, err
				}
				ts, err := strconv.ParseInt(item.Timestamp, 10, 64)
				if err != nil || ts <= 0 {
					return nil, fmt.Errorf("bybit: invalid timestamp %q", item.Timestamp)
				}
				rows = append(rows, Row{TS: ts, Raw: raw})
			}
			return rows, nil
		}})
	return []Channel{history, bybitSnapshot(e, "riskLimit", "/v5/market/risk-limit", false), bybitSnapshot(e, "instruments", "/v5/market/instruments-info", true)}
}

// Snapshots retain whole response pages, including all symbols and the exchange envelope.
func bybitSnapshot(e *Env, name, path string, instruments bool) Channel {
	return e.Channel(name, ChannelSpec{Daily: true, Fetch: func(ctx context.Context, _ string) ([]Record, error) {
		q := url.Values{"category": {"linear"}}
		if instruments {
			q.Set("limit", "1000")
		}
		seen := map[string]bool{}
		var records []Record
		for {
			raw, v, err := bybitGet(ctx, e, path, q)
			if err != nil {
				return nil, err
			}
			if *v.Code != 0 {
				return nil, fmt.Errorf("bybit %s: %d: %s", name, *v.Code, v.Message)
			}
			if v.Result.List == nil {
				return nil, fmt.Errorf("bybit %s: missing list", name)
			}
			records = append(records, Record{Venue: e.Venue, Channel: name, RecvWall: e.Clock.Now().UnixNano(), RecvMono: time.Since(monoOrigin).Nanoseconds(), Meta: map[string]any{"snapshot": true}, Payload: raw})
			if v.Result.Cursor == "" {
				break
			}
			if seen[v.Result.Cursor] {
				return nil, fmt.Errorf("bybit %s: cursor did not advance", name)
			}
			seen[v.Result.Cursor] = true
			q.Set("cursor", v.Result.Cursor)
			if e.Scheduler != nil {
				if err := e.Scheduler.waitRequest(ctx, e.Venue, name); err != nil {
					return nil, err
				}
			}
		}
		return records, nil
	}})
}
