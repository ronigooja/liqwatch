package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

func init() { RegisterVenue("gate", gateChannels, WithBaseURL("https://api.gateio.ws/api/v4")) }

func gateChannels(e *Env) []Channel {
	if e.Config.Venues["gate"].RPS <= 0 {
		e.RPS = 5
	}
	history := e.HistoryChannel("contractStats", HistorySpec{
		Step: time.Minute, Retention: 179 * 24 * time.Hour, Mode: PageBackward,
		PageSize: 2000, Period: 24 * time.Hour,
		Fetch: func(ctx context.Context, symbol string, start, end int64) ([]Row, error) {
			q := url.Values{"contract": {symbol}, "interval": {"1m"}, "limit": {"2000"},
				"from": {strconv.FormatInt(start/1000, 10)}, "to": {strconv.FormatInt(end/1000, 10)}}
			resp, err := e.Get(ctx, "/futures/usdt/contract_stats", q)
			if err != nil {
				return nil, err
			}
			var raw []json.RawMessage
			if err := json.Unmarshal(resp.Body, &raw); err != nil || raw == nil {
				return nil, fmt.Errorf("gate: invalid contract stats")
			}
			rows := make([]Row, 0, len(raw))
			for _, item := range raw {
				var v struct {
					Time int64 `json:"time"`
				}
				if json.Unmarshal(item, &v) != nil || v.Time <= 0 {
					return nil, fmt.Errorf("gate: invalid contract stats timestamp")
				}
				// Keep rows before start until History finishes paging: it filters them after
				// using the unfiltered count to decide whether the page was full.
				if v.Time*1000 <= end {
					rows = append(rows, Row{TS: v.Time * 1000, Raw: item})
				}
			}
			return rows, nil
		},
	})
	tiers := e.SnapshotChannel("riskLimitTiers", SnapshotSpec{Mode: SnapshotSplit,
		Fetch: func(ctx context.Context, _ string) ([]byte, error) {
			// Gate's offset counts contracts, not individual tier rows.
			all := []json.RawMessage{}
			for offset := 0; ; offset += 100 {
				if offset > 0 && e.Scheduler != nil {
					if err := e.Scheduler.waitRequest(ctx, e.Venue, "riskLimitTiers"); err != nil {
						return nil, err
					}
				}
				resp, err := e.Get(ctx, "/futures/usdt/risk_limit_tiers", url.Values{"limit": {"1000"}, "offset": {strconv.Itoa(offset)}})
				if err != nil {
					return nil, err
				}
				var rows []json.RawMessage
				if json.Unmarshal(resp.Body, &rows) != nil || rows == nil {
					return nil, fmt.Errorf("gate: invalid risk tiers")
				}
				markets := map[string]bool{}
				for _, row := range rows {
					var v struct {
						Contract string `json:"contract"`
					}
					if json.Unmarshal(row, &v) != nil || v.Contract == "" {
						return nil, fmt.Errorf("gate: invalid tier contract")
					}
					markets[v.Contract] = true
				}
				all = append(all, rows...)
				if len(markets) < 100 {
					break
				}
			}
			return json.Marshal(all)
		},
		Split: func(raw []byte) ([]SnapshotRow, error) {
			var rows []json.RawMessage
			if err := json.Unmarshal(raw, &rows); err != nil {
				return nil, err
			}
			groups := map[string][]json.RawMessage{}
			order := []string{}
			for _, row := range rows {
				var v struct {
					Contract string `json:"contract"`
				}
				if json.Unmarshal(row, &v) != nil || v.Contract == "" {
					return nil, fmt.Errorf("gate: invalid tier contract")
				}
				if _, ok := groups[v.Contract]; !ok {
					order = append(order, v.Contract)
				}
				groups[v.Contract] = append(groups[v.Contract], row)
			}
			out := make([]SnapshotRow, 0, len(order))
			for _, symbol := range order {
				b, err := json.Marshal(groups[symbol])
				if err != nil {
					return nil, err
				}
				out = append(out, SnapshotRow{Symbol: symbol, Raw: b})
			}
			return out, nil
		},
	})
	contracts := e.SnapshotChannel("contracts", SnapshotSpec{Mode: SnapshotBulk, Fetch: func(ctx context.Context, _ string) ([]byte, error) {
		resp, err := e.Get(ctx, "/futures/usdt/contracts", nil)
		if err != nil {
			return nil, err
		}
		return resp.Body, nil
	}})
	return []Channel{history, tiers, contracts}
}
