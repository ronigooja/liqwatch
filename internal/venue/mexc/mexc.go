// Package mexc implements the MEXC futures venue adapter.
package mexc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

const (
	wsURL           = "wss://contract.mexc.com/edge"
	detailURL       = "https://contract.mexc.com/api/v1/contract/detail"
	symbolsPerShard = 20 // 100 subscriptions, exercised live at 150 ms spacing.
)

var topics = []string{"deal", "fair.price", "index.price", "ticker", "funding.rate"}

type Adapter struct {
	// DetailURL and Client optionally override symbol discovery for tests.
	DetailURL string
	Client    *http.Client
	mu        sync.RWMutex
	universe  map[string]struct{}
}

func init()                   { venue.Register("mexc", func() venue.Adapter { return &Adapter{} }) }
func (*Adapter) Name() string { return "mexc" }

func (a *Adapter) Symbols(ctx context.Context) ([]venue.Symbol, error) {
	u := a.DetailURL
	if u == "" {
		u = detailURL
	}
	client := a.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mexc contract detail: HTTP %d", resp.StatusCode)
	}
	var body struct {
		Success bool `json:"success"`
		Code    int  `json:"code"`
		Data    []struct {
			Symbol     string `json:"symbol"`
			State      *int   `json:"state"`
			FutureType int    `json:"futureType"`
			Quote      string `json:"quoteCoin"`
			Settle     string `json:"settleCoin"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("mexc contract detail: %w", err)
	}
	if !body.Success || body.Code != 0 || body.Data == nil {
		return nil, fmt.Errorf("mexc contract detail: unsuccessful or missing data (code %d)", body.Code)
	}
	var syms []string
	for _, d := range body.Data {
		if d.Symbol != "" && d.State != nil && *d.State == 0 && d.FutureType == 1 && d.Quote == "USDT" && d.Settle == "USDT" {
			syms = append(syms, d.Symbol)
		}
	}
	return syms, nil
}

func (a *Adapter) Shards(_ context.Context, syms []venue.Symbol) ([]venue.Shard, error) {
	universe := make(map[string]struct{}, len(syms))
	for _, s := range syms {
		universe[s] = struct{}{}
	}
	a.mu.Lock()
	a.universe = universe
	a.mu.Unlock()
	var out []venue.Shard
	for i := 0; i < len(syms); i += symbolsPerShard {
		var streams []string
		for _, s := range syms[i:min(i+symbolsPerShard, len(syms))] {
			for _, topic := range topics {
				streams = append(streams, topic+"@"+s)
			}
		}
		out = append(out, venue.Shard{Name: fmt.Sprintf("market%02d", len(out)), URL: wsURL, Streams: streams, Stall: 60 * time.Second, MaxSubsPerConn: 100, SubscribeSpacing: 150 * time.Millisecond})
	}
	if len(syms) > 0 {
		out = append(out, venue.Shard{Name: "contract", URL: wsURL, Streams: []string{"contract"}, Stall: 60 * time.Second, MaxSubsPerConn: 1})
	}
	return out, nil
}

func (*Adapter) Subscribe(sh venue.Shard) [][]byte {
	out := make([][]byte, 0, len(sh.Streams))
	for _, stream := range sh.Streams {
		if stream == "contract" {
			out = append(out, []byte(`{"method":"sub.contract"}`))
			continue
		}
		topic, symbol, ok := strings.Cut(stream, "@")
		if !ok {
			continue
		}
		param := map[string]any{"symbol": symbol}
		if topic == "deal" {
			param["compress"] = false
		}
		b, _ := json.Marshal(map[string]any{"method": "sub." + topic, "param": param})
		out = append(out, b)
	}
	return out
}

func (*Adapter) Heartbeat(venue.Shard) (time.Duration, func() (int, []byte)) {
	return 15 * time.Second, func() (int, []byte) { return int(websocket.MessageText), []byte(`{"method":"ping"}`) }
}

func (a *Adapter) Split(_ venue.Shard, msg []byte) []rawlog.Record {
	var v struct {
		Channel string          `json:"channel"`
		Symbol  string          `json:"symbol"`
		Data    json.RawMessage `json:"data"`
	}
	err := json.Unmarshal(msg, &v)
	if err == nil && (v.Channel == "pong" || strings.HasPrefix(v.Channel, "rs.")) {
		return nil
	}
	channels := map[string]string{"push.deal": "trade", "push.fair.price": "mark", "push.index.price": "index", "push.ticker": "ticker", "push.funding.rate": "funding", "push.contract": "contract"}
	ch, known := channels[v.Channel]
	if err == nil && known && len(v.Data) > 0 && string(v.Data) != "null" && v.Symbol != "" {
		if ch == "contract" {
			a.mu.RLock()
			_, ok := a.universe[v.Symbol]
			a.mu.RUnlock()
			if !ok {
				return nil
			}
		}
		return []rawlog.Record{{Channel: ch, Symbol: v.Symbol, Payload: msg}}
	}
	if err == nil && (len(v.Data) == 0 || string(v.Data) == "null") && !strings.HasPrefix(v.Channel, "push.") {
		return nil
	}
	p, _ := json.Marshal(map[string]any{"event": "unrecognized_frame", "bytes": len(msg), "head": string(msg[:min(len(msg), 4096)])})
	return []rawlog.Record{{Channel: "gap", Payload: p}}
}

func (*Adapter) Classes() func(string) (string, bool) {
	return func(ch string) (string, bool) {
		switch ch {
		case "trade":
			return "trade", true
		case "mark", "index":
			return "mark", true
		default:
			return "", false
		}
	}
}
