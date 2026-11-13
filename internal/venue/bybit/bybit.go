// Package bybit implements the Bybit linear perpetual venue adapter.
package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

// WSLinear is Bybit's public linear derivatives endpoint.
const WSLinear = "wss://stream.bybit.com/v5/public/linear"
const instrumentsURL = "https://api.bybit.com/v5/market/instruments-info"
const maxStreams = 90 // Operational cap, not a documented Bybit limit.

// Adapter holds optional overrides for instrument discovery.
type Adapter struct {
	HTTPClient     *http.Client
	InstrumentsURL string
}

func init()                   { venue.Register("bybit", func() venue.Adapter { return &Adapter{} }) }
func (*Adapter) Name() string { return "bybit" }
func (a *Adapter) Symbols(ctx context.Context) ([]venue.Symbol, error) {
	endpoint := a.InstrumentsURL
	if endpoint == "" {
		endpoint = instrumentsURL
	}
	client := a.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	cursor := ""
	seen := map[string]bool{}
	syms := map[string]bool{}
	for {
		u, e := url.Parse(endpoint)
		if e != nil {
			return nil, e
		}
		q := u.Query()
		q.Set("category", "linear")
		q.Set("limit", "1000")
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		u.RawQuery = q.Encode()
		req, e := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if e != nil {
			return nil, e
		}
		resp, e := client.Do(req)
		if e != nil {
			return nil, e
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("bybit instruments HTTP %d", resp.StatusCode)
		}
		var body struct {
			RetCode int    `json:"retCode"`
			RetMsg  string `json:"retMsg"`
			Result  struct {
				List []struct {
					Symbol       string `json:"symbol"`
					ContractType string `json:"contractType"`
					Status       string `json:"status"`
					Quote        string `json:"quoteCoin"`
					Settle       string `json:"settleCoin"`
				} `json:"list"`
				Next string `json:"nextPageCursor"`
			} `json:"result"`
		}
		e = json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&body)
		resp.Body.Close()
		if e != nil {
			return nil, e
		}
		if body.RetCode != 0 {
			return nil, fmt.Errorf("bybit instruments: %d %s", body.RetCode, body.RetMsg)
		}
		for _, s := range body.Result.List {
			if s.Symbol != "" && s.ContractType == "LinearPerpetual" && s.Status == "Trading" && s.Quote == "USDT" && s.Settle == "USDT" {
				syms[s.Symbol] = true
			}
		}
		cursor = body.Result.Next
		if cursor == "" {
			break
		}
		if seen[cursor] {
			return nil, fmt.Errorf("bybit instruments: repeated cursor")
		}
		seen[cursor] = true
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	out := make([]string, 0, len(syms))
	for s := range syms {
		out = append(out, s)
	}
	sort.Strings(out)
	return out, nil
}
func (*Adapter) Shards(_ context.Context, syms []venue.Symbol) ([]venue.Shard, error) {
	var out []venue.Shard
	var streams []string
	size := 2
	flush := func() {
		if len(streams) == 0 {
			return
		}
		out = append(out, venue.Shard{Name: fmt.Sprintf("linear%02d", len(out)), URL: WSLinear, Streams: streams, Stall: 60 * time.Second, MaxAge: 23 * time.Hour, MaxSubsPerConn: maxStreams, SubscribeSpacing: time.Second})
		streams = nil
		size = 2
	}
	for _, sym := range syms {
		for _, prefix := range []string{"publicTrade.", "tickers.", "allLiquidation."} {
			topic := prefix + sym
			b, _ := json.Marshal(topic)
			n := len(b) + 1
			if n+2 > 21000 {
				return nil, fmt.Errorf("bybit topic too long")
			}
			if len(streams) >= maxStreams || size+n > 21000 {
				flush()
			}
			streams = append(streams, topic)
			size += n
		}
	}
	flush()
	return out, nil
}
func (*Adapter) Subscribe(sh venue.Shard) [][]byte {
	const batchSize = 30
	var out [][]byte
	for i := 0; i < len(sh.Streams); i += batchSize {
		b, _ := json.Marshal(struct {
			Op   string   `json:"op"`
			Args []string `json:"args"`
		}{"subscribe", sh.Streams[i:min(i+batchSize, len(sh.Streams))]})
		out = append(out, b)
	}
	return out
}
func (*Adapter) Heartbeat(venue.Shard) (time.Duration, func() (int, []byte)) {
	return 20 * time.Second, func() (int, []byte) { return int(websocket.MessageText), []byte(`{"op":"ping"}`) }
}
func (*Adapter) Split(_ venue.Shard, msg []byte) []rawlog.Record {
	var x struct {
		Topic string          `json:"topic"`
		Data  json.RawMessage `json:"data"`
		Op    string          `json:"op"`
	}
	err := json.Unmarshal(msg, &x)
	if err == nil && len(x.Data) == 0 && x.Topic == "" && x.Op != "" {
		return nil
	}
	if err == nil && len(x.Data) > 0 && string(x.Data) != "null" {
		p := strings.SplitN(x.Topic, ".", 2)
		if len(p) == 2 && p[1] != "" {
			var ch string
			switch p[0] {
			case "publicTrade":
				ch = "trade"
			case "tickers":
				ch = "mark"
			case "allLiquidation":
				ch = "liquidation"
			}
			if ch != "" {
				return []rawlog.Record{{Channel: ch, Symbol: p[1], Payload: msg}}
			}
		}
	}
	b, _ := json.Marshal(map[string]any{"event": "unrecognized_frame", "bytes": len(msg), "head": string(msg[:min(len(msg), 200)])})
	return []rawlog.Record{{Channel: "gap", Payload: b}}
}
func (*Adapter) Classes() func(string) (string, bool) {
	return func(ch string) (string, bool) {
		switch ch {
		case "mark":
			return "mark", true
		case "trade":
			return "trade", true
		}
		return "", false
	}
}
