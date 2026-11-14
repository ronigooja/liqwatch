// Package gate implements Gate's USDT perpetual futures venue adapter.
package gate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

const contractsURL = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
const wsURL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
const shardSize = 100

// Each table describes fields belonging to the named recordable items. Unknown
// fields are retained until classified; drop lists are deliberately explicit.
type fieldPolicy struct{ keep, drop map[string]bool }

func fields(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, name := range names {
		m[name] = true
	}
	return m
}

var policies = map[string]fieldPolicy{
	// Trades: execution and contract-unit metadata.
	"trades": {fields("contract", "id", "size", "price", "create_time", "create_time_ms", "time", "time_ms", "t", "quanto_multiplier"), fields()},
	// Liquidations: execution and contract-unit metadata.
	"liquidations": {fields("contract", "size", "price", "time", "time_ms", "t", "create_time", "create_time_ms", "id", "quanto_multiplier"), fields()},
	// Mark, index, funding, and open interest; ticker statistics are excluded.
	"tickers": {fields("contract", "mark_price", "index_price", "funding_rate", "funding_rate_indicative", "funding_interval", "funding_offset", "funding_next_apply", "total_size", "time", "time_ms", "t", "quanto_multiplier", "event_type"), fields("last", "change_percentage", "volume_24h", "volume_24h_base", "volume_24h_quote", "volume_24h_settle", "quanto_base_rate", "low_24h", "high_24h", "price_type", "change_from", "change_price", "highest_bid", "highest_size", "lowest_ask", "lowest_size", "bid", "ask", "bid_size", "ask_size")},
	// Open interest, ratios, mark price and liquidation statistics.
	"contract_stats": {fields("contract", "time", "time_ms", "t", "open_interest", "open_interest_usd", "lsr_account", "lsr_taker", "top_lsr_account", "top_lsr_size", "mark_price", "long_liq_amount", "long_liq_size", "long_liq_usd", "short_liq_amount", "short_liq_size", "short_liq_usd"), fields()},
	// Contract units, leverage tiers, funding, mark, index, and open interest.
	"contract_info": {fields("contract", "time", "time_ms", "t", "quanto_multiplier", "order_price_round", "mark_price_round", "risk_limit_tiers", "mark_price", "index_price", "funding_rate", "funding_next_apply", "total_size", "leverage_min", "leverage_max", "leverage_tiers"), fields("last", "change_percentage", "volume_24h", "volume_24h_base", "volume_24h_quote", "volume_24h_settle", "quanto_base_rate", "low_24h", "high_24h", "highest_bid", "highest_size", "lowest_ask", "lowest_size", "bid", "ask", "bid_size", "ask_size")},
}

var envelopePolicy = fieldPolicy{fields("channel", "event", "time", "time_ms", "result"), fields("conn_id", "trace_id")}

var frameSampler = struct {
	sync.Mutex
	last map[string]time.Time
	now  func() time.Time
}{last: make(map[string]time.Time), now: time.Now}

func sampleDue(shard, channel string) bool {
	frameSampler.Lock()
	defer frameSampler.Unlock()
	now := frameSampler.now()
	key := shard + "\x00" + channel
	last, ok := frameSampler.last[key]
	if ok && now.Sub(last) < time.Hour {
		return false
	}
	frameSampler.last[key] = now
	return true
}

type Adapter struct {
	HTTPClient      *http.Client
	ContractsURL    string
	mu              sync.RWMutex
	universe        map[string]struct{}
	unknownFields   atomic.Uint64
	subscribeAcks   atomic.Uint64
	subscribeErrors atomic.Uint64
	fallbacks       atomic.Uint64
}

// UnknownFields returns the number of unclassified field occurrences retained.
func (a *Adapter) UnknownFields() uint64 { return a.unknownFields.Load() }

// SubscribeAcks returns successful subscription acknowledgements observed.
func (a *Adapter) SubscribeAcks() uint64 { return a.subscribeAcks.Load() }

// SubscribeErrors returns error frames observed.
func (a *Adapter) SubscribeErrors() uint64 { return a.subscribeErrors.Load() }

// filterObject uses RawMessage for values and a decoder for original key order.
func (a *Adapter) filterObject(src []byte, policy fieldPolicy, result []byte) ([]byte, error) {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(src, &values); err != nil || values == nil {
		return nil, fmt.Errorf("invalid object")
	}
	d := json.NewDecoder(bytes.NewReader(src))
	if _, err := d.Token(); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.WriteByte('{')
	first := true
	for d.More() {
		tok, err := d.Token()
		if err != nil {
			return nil, err
		}
		key, ok := tok.(string)
		if !ok {
			return nil, fmt.Errorf("invalid key")
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, err
		}
		if policy.drop[key] {
			continue
		}
		if !policy.keep[key] {
			a.unknownFields.Add(1)
		}
		if key == "result" && result != nil {
			value = result
		}
		if !first {
			out.WriteByte(',')
		}
		first = false
		name, _ := json.Marshal(key)
		out.Write(name)
		out.WriteByte(':')
		if err := json.Compact(&out, value); err != nil {
			return nil, err
		}
	}
	out.WriteByte('}')
	return out.Bytes(), nil
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
		shards = append(shards, venue.Shard{Name: fmt.Sprintf("market%02d", i/shardSize), URL: wsURL, Header: http.Header{"X-Gate-Size-Decimal": {"1"}}, Streams: part, Stall: 90 * time.Second, MaxSubsPerConn: shardSize + 3, SubscribeSpacing: 700 * time.Millisecond})
	}
	shards = append(shards, venue.Shard{Name: "all", URL: wsURL, Header: http.Header{"X-Gate-Size-Decimal": {"1"}}, Streams: []string{"!all"}, Stall: 90 * time.Second, SubscribeSpacing: 700 * time.Millisecond})
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
func (a *Adapter) splitFallback(sh venue.Shard, msg []byte) []rawlog.Record {
	var env struct {
		Channel string          `json:"channel"`
		Event   string          `json:"event"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if json.Unmarshal(msg, &env) != nil {
		return gap(msg)
	}
	if env.Event == "error" || (len(env.Error) > 0 && string(env.Error) != "null") {
		a.subscribeErrors.Add(1)
		var frame any = json.RawMessage(msg)
		if len(msg) > 4096 {
			frame = string(msg[:4096])
		}
		payload, _ := json.Marshal(map[string]any{"event": "subscribe_error", "frame": frame})
		return []rawlog.Record{{Channel: "gap", Payload: payload}}
	}
	if env.Event == "subscribe" {
		var result struct {
			Status string `json:"status"`
		}
		if json.Unmarshal(env.Result, &result) == nil && result.Status == "success" {
			a.subscribeAcks.Add(1)
		}
		return nil
	}
	if env.Event == "unsubscribe" || env.Channel == "futures.pong" {
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
		filtered, err := a.filterObject(item, policies[ch], nil)
		if err != nil {
			out = append(out, gap(msg)...)
			continue
		}
		if array {
			filtered = append(append([]byte{'['}, filtered...), ']')
		}
		payload, err := a.filterObject(msg, envelopePolicy, filtered)
		if err != nil {
			return gap(msg)
		}
		out = append(out, rawlog.Record{Channel: ch, Symbol: v.Contract, Payload: payload})
	}
	if len(out) > 0 && out[0].Channel == ch && sampleDue(sh.Name, ch) {
		payload := make([]byte, 0, len(msg)+11)
		payload = append(payload, `{"sample":`...)
		payload = append(payload, msg...)
		payload = append(payload, '}')
		out = append(out, rawlog.Record{Channel: ch, Symbol: out[0].Symbol, Payload: payload})
	}
	return out
}

// Fallbacks returns the number of frames handled by the reference decoder.
func (a *Adapter) Fallbacks() uint64 { return a.fallbacks.Load() }

func (a *Adapter) Split(sh venue.Shard, msg []byte) []rawlog.Record {
	if records, ok := a.scanSplit(sh, msg); ok {
		return records
	}
	a.fallbacks.Add(1)
	return a.splitFallback(sh, msg)
}
