// Package gate implements Gate's USDT perpetual futures venue adapter.
package gate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

const contractsURL = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
const wsURL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
const shardSize = 100

type Adapter struct {
	HTTPClient   *http.Client
	ContractsURL string
	mu           sync.RWMutex
	universe     map[string]struct{}
}

func init()                   { venue.Register("gate", func() venue.Adapter { return &Adapter{} }) }
func (*Adapter) Name() string { return "gate" }
func (a *Adapter) Symbols(ctx context.Context) ([]venue.Symbol, error) {
	u := a.ContractsURL
	if u == "" {
		u = contractsURL
	}
	client := a.HTTPClient
	if client == nil {
		client = http.DefaultClient
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
		return nil, fmt.Errorf("gate contracts: HTTP %s", resp.Status)
	}
	var contracts []struct {
		Name        string `json:"name"`
		Status      string `json:"status"`
		InDelisting bool   `json:"in_delisting"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&contracts); err != nil {
		return nil, err
	}
	var out []venue.Symbol
	for _, c := range contracts {
		if c.Name != "" && strings.HasSuffix(c.Name, "_USDT") && c.Status == "trading" && !c.InDelisting {
			out = append(out, c.Name)
		}
	}
	sort.Strings(out)
	return out, nil
}
func (a *Adapter) Shards(_ context.Context, syms []venue.Symbol) ([]venue.Shard, error) {
	universe := make(map[string]struct{}, len(syms))
	for _, s := range syms {
		universe[s] = struct{}{}
	}
	a.mu.Lock()
	a.universe = universe
	a.mu.Unlock()
	var shards []venue.Shard
	for i := 0; i < len(syms); i += shardSize {
		part := append([]string(nil), syms[i:min(i+shardSize, len(syms))]...)
		shards = append(shards, venue.Shard{Name: fmt.Sprintf("market%02d", i/shardSize), URL: wsURL, Streams: part, Stall: 90 * time.Second, MaxSubsPerConn: shardSize + 3, SubscribeSpacing: 700 * time.Millisecond})
	}
	shards = append(shards, venue.Shard{Name: "all", URL: wsURL, Streams: []string{"!all"}, Stall: 90 * time.Second, SubscribeSpacing: 700 * time.Millisecond})
	return shards, nil
}
func (*Adapter) Subscribe(sh venue.Shard) [][]byte {
	var out [][]byte
	add := func(channel string, payload []string) {
		b, _ := json.Marshal(map[string]any{"time": time.Now().Unix(), "channel": channel, "event": "subscribe", "payload": payload})
		out = append(out, b)
	}
	if sh.Name == "all" {
		add("futures.public_liquidates", []string{"!all"})
		add("futures.contract_info", []string{"!all"})
		return out
	}
	for _, ch := range []string{"futures.trades", "futures.tickers"} {
		add(ch, sh.Streams)
	}
	for _, s := range sh.Streams {
		add("futures.contract_stats", []string{s, "1m"})
	}
	return out
}
func (*Adapter) Heartbeat(venue.Shard) (time.Duration, func() (int, []byte)) {
	return 25 * time.Second, func() (int, []byte) {
		b, _ := json.Marshal(map[string]any{"time": time.Now().Unix(), "channel": "futures.ping"})
		return 1, b
	}
}
func (*Adapter) Classes() func(string) (string, bool) {
	return func(ch string) (string, bool) {
		switch ch {
		case "trades":
			return "trade", true
		case "tickers":
			return "mark", true
		}
		return "", false
	}
}
func gap(msg []byte) []rawlog.Record {
	b, _ := json.Marshal(map[string]any{"event": "unrecognized_frame", "bytes": len(msg), "head": string(msg[:min(len(msg), 200)])})
	return []rawlog.Record{{Channel: "gap", Payload: b}}
}
func (a *Adapter) Split(_ venue.Shard, msg []byte) []rawlog.Record {
	var env struct {
		Channel string          `json:"channel"`
		Event   string          `json:"event"`
		Result  json.RawMessage `json:"result"`
	}
	if json.Unmarshal(msg, &env) != nil {
		return gap(msg)
	}
	if env.Event == "subscribe" || env.Event == "unsubscribe" || env.Channel == "futures.pong" {
		return nil
	}
	if env.Event != "update" && env.Event != "all" {
		if len(env.Result) > 0 && string(env.Result) != "null" {
			return gap(msg)
		}
		return nil
	}
	channels := map[string]string{"futures.trades": "trades", "futures.tickers": "tickers", "futures.public_liquidates": "liquidations", "futures.contract_stats": "contract_stats", "futures.contract_info": "contract_info"}
	ch, ok := channels[env.Channel]
	if !ok {
		return gap(msg)
	}
	var items []json.RawMessage
	array := len(env.Result) > 0 && env.Result[0] == '['
	if array {
		if json.Unmarshal(env.Result, &items) != nil {
			return gap(msg)
		}
	} else {
		items = []json.RawMessage{env.Result}
	}
	var out []rawlog.Record
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, item := range items {
		var v struct {
			Contract string `json:"contract"`
		}
		if json.Unmarshal(item, &v) != nil || v.Contract == "" {
			out = append(out, gap(msg)...)
			continue
		}
		if _, ok := a.universe[v.Contract]; !ok {
			continue
		}
		if !array {
			out = append(out, rawlog.Record{Channel: ch, Symbol: v.Contract, Payload: msg})
			continue
		}
		var original map[string]json.RawMessage
		if json.Unmarshal(msg, &original) != nil {
			return gap(msg)
		}
		original["result"], _ = json.Marshal([]json.RawMessage{item})
		payload, err := json.Marshal(original)
		if err != nil {
			return gap(msg)
		}
		out = append(out, rawlog.Record{Channel: ch, Symbol: v.Contract, Payload: payload})
	}
	return out
}
