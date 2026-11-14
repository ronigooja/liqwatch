package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"
)

func init() { RegisterVenue("kucoin", kucoinChannels, WithBaseURL("https://api-futures.kucoin.com")) }

func kucoinChannels(e *Env) []Channel {
	if e.Config.Venues["kucoin"].RPS <= 0 {
		e.RPS = 2
	}
	if e.WeightPerMinute <= 0 {
		e.WeightPerMinute = 4000
	}
	get := func(ctx context.Context, path string) ([]byte, int, error) {
		resp, err := e.Get(ctx, path, nil)
		if err != nil {
			return nil, 0, err
		}
		var envelope struct {
			Code string          `json:"code"`
			Data json.RawMessage `json:"data"`
			Msg  string          `json:"msg"`
		}
		if err := json.Unmarshal(resp.Body, &envelope); err != nil {
			return nil, 0, err
		}
		if envelope.Code != "200000" {
			return nil, 0, fmt.Errorf("kucoin code %q: %s", envelope.Code, envelope.Msg)
		}
		var entries []json.RawMessage
		if len(envelope.Data) == 0 || string(envelope.Data) == "null" || json.Unmarshal(envelope.Data, &entries) != nil {
			return nil, 0, fmt.Errorf("kucoin: invalid data array")
		}
		return resp.Body, len(entries), nil
	}
	// These tiers apply to classic isolated margin only; the surveyed UTA position-tier endpoints returned 404.
	risk := e.Channel("riskLimit", ChannelSpec{PerSymbol: true, Daily: true, Weight: 5,
		Skip: func(sym string) bool { return e.Scheduler != nil && e.Scheduler.NoDataSkip(e.Venue, "riskLimit", sym) },
		Fetch: func(ctx context.Context, sym string) ([]Record, error) {
			raw, n, err := get(ctx, "/api/v1/contracts/risk-limit/"+url.PathEscape(sym))
			if err != nil {
				return nil, err
			}
			now := e.Clock.Now()
			if n == 0 {
				if e.Scheduler != nil {
					previous := e.Scheduler.NoDataAt(e.Venue, "riskLimit", sym)
					e.Scheduler.MarkNoData(e.Venue, "riskLimit", sym, now)
					if previous != 0 {
						return nil, nil
					}
				}
				p, _ := json.Marshal(map[string]any{"event": "no_data", "channel": "riskLimit", "symbol": sym})
				return []Record{{Venue: e.Venue, Symbol: sym, Channel: "gap", RecvWall: now.UnixNano(), RecvMono: time.Since(monoOrigin).Nanoseconds(), Payload: p}}, nil
			}
			return []Record{{Venue: e.Venue, Symbol: sym, Channel: "riskLimit", RecvWall: now.UnixNano(), RecvMono: time.Since(monoOrigin).Nanoseconds(), Meta: map[string]any{"snapshot": true}, Payload: raw}}, nil
		}})
	// One bulk record preserves the entire contracts response; no per-symbol copies are written.
	contracts := e.SnapshotChannel("contracts", SnapshotSpec{Mode: SnapshotBulk, Fetch: func(ctx context.Context, _ string) ([]byte, error) {
		raw, _, err := get(ctx, "/api/v1/contracts/active")
		return raw, err
	}})
	return []Channel{risk, contracts}
}
