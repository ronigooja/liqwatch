package poller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// SnapshotMode selects how a daily snapshot response becomes records.
type SnapshotMode int

const (
	// SnapshotBulk writes the whole response as one record (symbol "").
	SnapshotBulk SnapshotMode = iota
	// SnapshotSplit writes one record per symbol from one response; Split cuts the response.
	SnapshotSplit
	// SnapshotPerSymbol requests each symbol separately and writes one record per request.
	SnapshotPerSymbol
)

// SnapshotRow is one per-symbol piece of a snapshot response.
type SnapshotRow struct {
	Symbol string
	Raw    []byte
}

// SnapshotSpec describes a daily snapshot channel (tiers, contract info). Records have no
// last_row_timestamp, so nothing is deduplicated.
type SnapshotSpec struct {
	Mode SnapshotMode
	// Fetch returns the raw JSON response. symbol is "" except in SnapshotPerSymbol mode.
	Fetch func(ctx context.Context, symbol string) ([]byte, error)
	// Split cuts a response into per-symbol rows (SnapshotSplit only).
	Split func(raw []byte) ([]SnapshotRow, error)
	// Universe scopes split output at fetch time. Nil means no configured scope;
	// a configured callback returning no symbols permits no per-symbol records.
	Universe func() []string
}

// SnapshotFetch builds the Channel.Fetch callback of a snapshot channel.
func SnapshotFetch(venue, channel string, clock Clock, spec SnapshotSpec) func(context.Context, string) ([]Record, error) {
	return func(ctx context.Context, symbol string) ([]Record, error) {
		if spec.Fetch == nil {
			return nil, errors.New("snapshot: no Fetch")
		}
		raw, err := spec.Fetch(ctx, symbol)
		if err != nil {
			return nil, err
		}
		if !json.Valid(raw) {
			return nil, fmt.Errorf("invalid JSON")
		}
		recv := clock.Now()
		rec := Record{Venue: venue, Symbol: symbol, Channel: channel, RecvWall: recv.UnixNano(), RecvMono: time.Since(monoOrigin).Nanoseconds(), Meta: map[string]any{"snapshot": true}, Payload: raw}
		if spec.Mode != SnapshotSplit {
			return []Record{rec}, nil
		}
		if spec.Split == nil {
			return nil, errors.New("snapshot: SnapshotSplit needs Split")
		}
		rows, err := spec.Split(raw)
		if err != nil {
			return nil, err
		}
		out := make([]Record, 0, len(rows))
		allowed := snapshotScope(spec.Universe)
		for _, row := range rows {
			if row.Symbol == "" || !json.Valid(row.Raw) {
				return nil, fmt.Errorf("invalid snapshot row")
			}
			if allowed != nil && !allowed[row.Symbol] {
				continue
			}
			r := rec
			r.Meta = map[string]any{"snapshot": true}
			r.Symbol, r.Payload = row.Symbol, row.Raw
			out = append(out, r)
		}
		return out, nil
	}
}

func snapshotScope(universe func() []string) map[string]bool {
	if universe == nil {
		return nil
	}
	allowed := map[string]bool{}
	for _, symbol := range universe() {
		allowed[symbol] = true
	}
	return allowed
}
