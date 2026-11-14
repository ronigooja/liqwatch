package poller

import (
	"context"
	"encoding/json"
	"fmt"
)

func init() { RegisterVenue("mexc", mexcChannels, WithBaseURL("https://contract.mexc.com")) }

// mexcChannels stores each complete contract detail, including its risk tiers,
// as one daily record per mapped universe symbol from a single bulk request.
func mexcChannels(e *Env) []Channel {
	return []Channel{e.SnapshotChannel("contractDetail", SnapshotSpec{
		Mode: SnapshotSplit,
		Fetch: func(ctx context.Context, _ string) ([]byte, error) {
			resp, err := e.Get(ctx, "/api/v1/contract/detail", nil)
			if err != nil {
				return nil, err
			}
			return resp.Body, nil
		},
		Split: func(raw []byte) ([]SnapshotRow, error) {
			var envelope struct {
				Success *bool             `json:"success"`
				Code    *int              `json:"code"`
				Data    []json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(raw, &envelope); err != nil {
				return nil, fmt.Errorf("mexc contract detail: %w", err)
			}
			if envelope.Success == nil || !*envelope.Success || envelope.Code == nil || *envelope.Code != 0 || envelope.Data == nil {
				return nil, fmt.Errorf("mexc contract detail: unsuccessful or invalid response")
			}
			allowed := make(map[string]bool)
			for _, symbol := range e.Universe() {
				allowed[symbol] = true
			}
			var rows []SnapshotRow
			for _, raw := range envelope.Data {
				var contract struct {
					Symbol string `json:"symbol"`
				}
				if err := json.Unmarshal(raw, &contract); err != nil || contract.Symbol == "" {
					return nil, fmt.Errorf("mexc contract detail: invalid symbol")
				}
				if allowed[contract.Symbol] {
					rows = append(rows, SnapshotRow{Symbol: contract.Symbol, Raw: raw})
				}
			}
			return rows, nil
		},
	})}
}
