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

func init() { RegisterVenue("bitget", bitgetChannels, WithBaseURL("https://api.bitget.com")) }

type bitgetEnvelope struct {
	Code string          `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

func bitgetChannels(e *Env) []Channel {
	out := make([]Channel, 0, 6)
	for _, endpoint := range []struct{ name, path string }{
		{"longShort", "long-short"}, {"accountLongShort", "account-long-short"},
		{"positionLongShort", "position-long-short"}, {"takerBuySell", "taker-buy-sell"},
	} {
		name, path := endpoint.name, endpoint.path
		c := e.HistoryChannel(name, HistorySpec{Step: 5 * time.Minute, Mode: PageForward, Period: time.Hour,
			Fetch: func(ctx context.Context, symbol string, _, _ int64) ([]Row, error) {
				resp, err := e.Get(ctx, "/api/v2/mix/market/"+path, url.Values{"symbol": {symbol}, "period": {"5m"}})
				if err != nil {
					// Env.Get includes the body of HTTP errors in its message.
					var envelope bitgetEnvelope
					if strings.HasPrefix(err.Error(), "HTTP 400: ") && json.Unmarshal([]byte(strings.TrimPrefix(err.Error(), "HTTP 400: ")), &envelope) == nil && (envelope.Code == "40309" || envelope.Code == "40054") {
						return nil, nil
					}
					return nil, err
				}
				var envelope bitgetEnvelope
				if err := json.Unmarshal(resp.Body, &envelope); err != nil {
					return nil, err
				}
				if envelope.Code != "00000" {
					return nil, fmt.Errorf("bitget %s: %s", envelope.Code, envelope.Msg)
				}
				var data []json.RawMessage
				if err := json.Unmarshal(envelope.Data, &data); err != nil {
					return nil, err
				}
				rows := make([]Row, 0, len(data))
				for _, raw := range data {
					var x struct {
						TS string `json:"ts"`
					}
					if err := json.Unmarshal(raw, &x); err != nil {
						return nil, err
					}
					ts, err := strconv.ParseInt(x.TS, 10, 64)
					if err != nil || ts <= 0 {
						return nil, fmt.Errorf("bitget: invalid timestamp %q", x.TS)
					}
					rows = append(rows, Row{TS: ts, Raw: raw})
				}
				return rows, nil
			}})
		fetch := c.Fetch
		c.Fetch = func(ctx context.Context, symbol string) ([]Record, error) {
			var cursor int64
			if e.Scheduler != nil {
				cursor = e.Scheduler.RowTimestamp(e.Venue, name, symbol)
			}
			records, err := fetch(ctx, symbol)
			if err != nil {
				return nil, err
			}
			var first int64
			for _, r := range records {
				if ts, ok := r.Meta["last_row_timestamp"].(int64); ok && (first == 0 || ts < first) {
					first = ts
				}
			}
			if cursor > 0 && first > cursor+int64((5*time.Minute)/time.Millisecond) {
				raw, _ := json.Marshal(map[string]any{"event": "source_gap", "kind": "source_gap", "channel": name, "symbol": symbol, "start_ms": cursor + int64((5*time.Minute)/time.Millisecond), "end_ms": first})
				records = append(records, Record{Venue: e.Venue, Symbol: symbol, Channel: "gap", RecvWall: e.Clock.Now().UnixNano(), Payload: raw})
			}
			return records, nil
		}
		out = append(out, c)
	}
	for _, endpoint := range []struct {
		name, path string
		mode       SnapshotMode
	}{{"positionTiers", "query-position-lever", SnapshotPerSymbol}, {"contracts", "contracts", SnapshotBulk}} {
		path := endpoint.path
		out = append(out, e.SnapshotChannel(endpoint.name, SnapshotSpec{Mode: endpoint.mode, Fetch: func(ctx context.Context, symbol string) ([]byte, error) {
			q := url.Values{"productType": {"USDT-FUTURES"}}
			if symbol != "" {
				q.Set("symbol", symbol)
			}
			resp, err := e.Get(ctx, "/api/v2/mix/market/"+path, q)
			if err != nil {
				return nil, err
			}
			var envelope bitgetEnvelope
			if err := json.Unmarshal(resp.Body, &envelope); err != nil {
				return nil, err
			}
			if envelope.Code != "00000" {
				return nil, fmt.Errorf("bitget %s: %s", envelope.Code, envelope.Msg)
			}
			return resp.Body, nil
		}}))
	}
	return out
}
