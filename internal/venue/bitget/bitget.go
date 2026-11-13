// Package bitget implements the Bitget USDT perpetual venue adapter.
package bitget

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

const (
	wsURL            = "wss://ws.bitget.com/v3/ws/public"
	contractsURL     = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
	maxSubscriptions = 48
)

type Adapter struct {
	// HTTPClient and ContractsURL can be overridden for testing.
	HTTPClient   *http.Client
	ContractsURL string
	mu           sync.RWMutex
	universe     map[string]struct{}
}

func init()                   { venue.Register("bitget", func() venue.Adapter { return &Adapter{} }) }
func (*Adapter) Name() string { return "bitget" }

func (a *Adapter) Symbols(ctx context.Context) ([]venue.Symbol, error) {
	u := a.ContractsURL
	if u == "" {
		u = contractsURL
	}
	client := a.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bitget contracts: HTTP %d", resp.StatusCode)
	}
	var data struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			Symbol             string   `json:"symbol"`
			SymbolType         string   `json:"symbolType"`
			SymbolStatus       string   `json:"symbolStatus"`
			QuoteCoin          string   `json:"quoteCoin"`
			SupportMarginCoins []string `json:"supportMarginCoins"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&data); err != nil {
		return nil, fmt.Errorf("bitget contracts: %w", err)
	}
	if data.Code != "00000" {
		return nil, fmt.Errorf("bitget contracts: %s %s", data.Code, data.Msg)
	}
	var result []string
	for _, s := range data.Data {
		margin := false
		for _, c := range s.SupportMarginCoins {
			if c == "USDT" {
				margin = true
				break
			}
		}
		if s.Symbol != "" && s.SymbolType == "perpetual" && s.SymbolStatus == "normal" && s.QuoteCoin == "USDT" && margin {
			result = append(result, s.Symbol)
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("bitget contracts: no normal USDT perpetuals")
	}
	sort.Strings(result)
	return result, nil
}

type subscription struct {
	InstType string `json:"instType"`
	Topic    string `json:"topic"`
	Symbol   string `json:"symbol,omitempty"`
}

func (a *Adapter) Shards(_ context.Context, syms []venue.Symbol) ([]venue.Shard, error) {
	universe := make(map[string]struct{}, len(syms))
	var streams []string
	for _, s := range syms {
		if s == "" {
			return nil, fmt.Errorf("bitget: empty symbol")
		}
		if _, ok := universe[s]; ok {
			continue
		}
		universe[s] = struct{}{}
		for _, topic := range []string{"publicTrade", "ticker", "funding-rate", "position-tier"} {
			b, _ := json.Marshal(subscription{InstType: "usdt-futures", Topic: topic, Symbol: s})
			streams = append(streams, string(b))
		}
	}
	a.mu.Lock()
	a.universe = universe
	a.mu.Unlock()
	if len(universe) == 0 {
		return nil, nil
	}
	out := make([]venue.Shard, 0, (len(streams)+maxSubscriptions-1)/maxSubscriptions+1)
	for i := 0; i < len(streams); i += maxSubscriptions {
		end := min(i+maxSubscriptions, len(streams))
		out = append(out, venue.Shard{Name: fmt.Sprintf("market%03d", len(out)), URL: wsURL, Streams: streams[i:end:end], Stall: 90 * time.Second, MaxSubsPerConn: maxSubscriptions, SubscribeSpacing: time.Second})
	}
	b, _ := json.Marshal(subscription{InstType: "usdt-futures", Topic: "liquidation"})
	out = append(out, venue.Shard{Name: "liquidation", URL: wsURL, Streams: []string{string(b)}, Stall: 90 * time.Second, MaxSubsPerConn: maxSubscriptions, SubscribeSpacing: time.Second})
	return out, nil
}
func (*Adapter) Subscribe(sh venue.Shard) [][]byte {
	var result [][]byte
	// Keep subscription messages well below the V2 documented 4096-byte ceiling too.
	for start := 0; start < len(sh.Streams); {
		var args []json.RawMessage
		end := start
		for end < len(sh.Streams) {
			candidate := append(args, json.RawMessage(sh.Streams[end]))
			p, err := json.Marshal(struct {
				Op   string            `json:"op"`
				Args []json.RawMessage `json:"args"`
			}{"subscribe", candidate})
			if err != nil {
				return nil
			}
			if len(p) > 3500 && end > start {
				break
			}
			args = candidate
			end++
		}
		p, _ := json.Marshal(struct {
			Op   string            `json:"op"`
			Args []json.RawMessage `json:"args"`
		}{"subscribe", args})
		result = append(result, p)
		start = end
	}
	return result
}
func (*Adapter) Heartbeat(venue.Shard) (time.Duration, func() (int, []byte)) {
	return 30 * time.Second, func() (int, []byte) { return int(websocket.MessageText), []byte("ping") }
}
func (*Adapter) Classes() func(string) (string, bool) {
	return func(ch string) (string, bool) {
		switch ch {
		case "trade":
			return "trade", true
		case "ticker":
			return "mark", true
		}
		return "", false
	}
}

func gap(msg []byte) []rawlog.Record {
	p, _ := json.Marshal(map[string]any{"event": "unrecognized_frame", "bytes": len(msg), "head": string(msg[:min(len(msg), 200)])})
	return []rawlog.Record{{Channel: "gap", Payload: p}}
}
func (a *Adapter) Split(_ venue.Shard, msg []byte) []rawlog.Record {
	if bytes.Equal(bytes.TrimSpace(msg), []byte("pong")) {
		return nil
	}
	var v struct {
		Event string `json:"event"`
		Arg   struct {
			Topic  string `json:"topic"`
			Symbol string `json:"symbol"`
		} `json:"arg"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(msg, &v) != nil {
		if bytes.Contains(msg, []byte(`"data"`)) {
			return gap(msg)
		}
		return nil
	}
	if len(v.Data) == 0 {
		return nil
	}
	channel := map[string]string{"publicTrade": "trade", "ticker": "ticker", "funding-rate": "funding", "position-tier": "tier", "liquidation": "liquidation"}[v.Arg.Topic]
	if channel == "" {
		return gap(msg)
	}
	if v.Arg.Symbol != "" {
		return []rawlog.Record{{Channel: channel, Symbol: v.Arg.Symbol, Payload: msg}}
	}
	var elements []json.RawMessage
	if len(v.Data) > 0 && v.Data[0] == '[' {
		if json.Unmarshal(v.Data, &elements) != nil {
			return gap(msg)
		}
	} else {
		elements = []json.RawMessage{v.Data}
	}
	var out []rawlog.Record
	for _, element := range elements {
		var fields struct {
			Symbol string `json:"symbol"`
		}
		if json.Unmarshal(element, &fields) != nil || fields.Symbol == "" {
			out = append(out, gap(element)...)
			continue
		}
		if channel == "liquidation" {
			a.mu.RLock()
			_, ok := a.universe[fields.Symbol]
			a.mu.RUnlock()
			if !ok {
				continue
			}
		}
		var envelope map[string]json.RawMessage
		if json.Unmarshal(msg, &envelope) != nil {
			out = append(out, gap(msg)...)
			continue
		}
		if len(v.Data) > 0 && v.Data[0] == '[' {
			p, _ := json.Marshal([]json.RawMessage{element})
			envelope["data"] = p
		} else {
			envelope["data"] = element
		}
		p, err := json.Marshal(envelope)
		if err != nil {
			out = append(out, gap(msg)...)
			continue
		}
		out = append(out, rawlog.Record{Channel: channel, Symbol: fields.Symbol, Payload: p})
	}
	return out
}
