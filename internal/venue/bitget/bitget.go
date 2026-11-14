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
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

// Fields are grouped by recorded item; unlisted fields are retained and counted.
var keepFields = map[string]map[string]bool{
	"trade":       {"i": true, "p": true, "v": true, "S": true, "T": true, "L": true, "isRPI": true},                                                       // trades
	"ticker":      {"symbol": true, "ts": true, "indexPrice": true, "markPrice": true, "fundingRate": true, "openInterest": true, "nextFundingTime": true}, // prices, funding, OI
	"funding":     {"symbol": true, "ts": true, "fundingRate": true, "nextUpdate": true},                                                                   // funding
	"tier":        {"symbol": true, "ts": true, "list": true, "leverage": true, "maxTierValue": true, "minTierValue": true, "mmr": true, "tier": true},     // leverage tiers
	"liquidation": {"symbol": true, "side": true, "price": true, "amount": true, "ts": true},                                                               // liquidations
}
var dropFields = map[string]map[string]bool{
	"ticker":  {"highPrice24h": true, "lowPrice24h": true, "openPrice24h": true, "lastPrice": true, "turnover24h": true, "volume24h": true, "bid1Price": true, "ask1Price": true, "bid1Size": true, "ask1Size": true, "price24hPcnt": true, "deliveryTime": true, "deliveryStartTime": true, "deliveryStatus": true}, // quotes, statistics, delivery
	"funding": {"baseInterestRate": true, "cashDividend": true, "cashDividendNextUpdate": true, "fundingRateInterval": true, "impactMarginNotional": true, "lastFundingRate": true, "maxFundingRate": true, "minFundingRate": true, "nextFundingRate": true},                                                         // funding inputs and estimates
}

var samples = struct {
	sync.Mutex
	last map[string]time.Time
}{last: make(map[string]time.Time)}

type lastTicker struct {
	payload string
	emitted time.Time
}

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
	clock        func() time.Time
	tickerMu     sync.Mutex
	tickers      map[string]lastTicker
	unknown      atomic.Uint64
	skipped      atomic.Uint64
	acks         atomic.Uint64
	errors       atomic.Uint64
	fallbacks    atomic.Uint64
}

func (a *Adapter) ScannerFallbacks() uint64 { return a.fallbacks.Load() }
func (a *Adapter) UnknownFields() uint64    { return a.unknown.Load() }
func (a *Adapter) Deduplicated() uint64     { return a.skipped.Load() }
func (a *Adapter) SubscribeAcks() uint64    { return a.acks.Load() }
func (a *Adapter) SubscribeErrors() uint64  { return a.errors.Load() }
func (a *Adapter) now() time.Time {
	if a.clock != nil {
		return a.clock()
	}
	return time.Now()
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

// filterObject preserves field order and raw value representations.
func (a *Adapter) filterObject(channel string, data []byte, noTime bool) ([]byte, bool, error) {
	data = bytes.TrimSpace(data)
	if len(data) < 2 || data[0] != '{' || !json.Valid(data) {
		return nil, false, fmt.Errorf("invalid object")
	}
	var out bytes.Buffer
	out.WriteByte('{')
	meaningful := false
	for i := 1; ; {
		for i < len(data) && (data[i] == ' ' || data[i] == '\n' || data[i] == '\r' || data[i] == '\t' || data[i] == ',') {
			i++
		}
		if data[i] == '}' {
			break
		}
		start := i
		i = jsonStringEnd(data, i)
		var key string
		_ = json.Unmarshal(data[start:i], &key)
		for data[i] != ':' {
			i++
		}
		i++
		for data[i] == ' ' || data[i] == '\n' || data[i] == '\r' || data[i] == '\t' {
			i++
		}
		start = i
		depth := 0
		for i < len(data) {
			switch data[i] {
			case '"':
				i = jsonStringEnd(data, i)
				continue
			case '{', '[':
				depth++
			case '}', ']':
				if depth == 0 {
					goto valueDone
				}
				depth--
			case ',':
				if depth == 0 {
					goto valueDone
				}
			}
			i++
		}
	valueDone:
		value := json.RawMessage(bytes.TrimSpace(data[start:i]))
		if dropFields[channel][key] || (noTime && (key == "ts" || key == "T")) {
			continue
		}
		if !keepFields[channel][key] {
			a.unknown.Add(1)
		}
		if channel == "tier" && key == "list" {
			var items []json.RawMessage
			if json.Unmarshal(value, &items) == nil {
				for i, item := range items {
					if filtered, _, err := a.filterObject(channel, item, false); err == nil {
						items[i] = filtered
					}
				}
				value, _ = json.Marshal(items)
			}
		}
		if key != "symbol" && key != "ts" && key != "T" {
			meaningful = true
		}
		if out.Len() > 1 {
			out.WriteByte(',')
		}
		k, _ := json.Marshal(key)
		out.Write(k)
		out.WriteByte(':')
		out.Write(value)
	}
	out.WriteByte('}')
	return out.Bytes(), meaningful, nil
}

// jsonStringEnd skips a validated JSON string, including escaped quotes.
func jsonStringEnd(data []byte, i int) int {
	for i++; i < len(data); i++ {
		if data[i] == '\\' {
			i++
			continue
		}
		if data[i] == '"' {
			return i + 1
		}
	}
	return i
}

func (a *Adapter) legacySplit(sh venue.Shard, msg []byte) []rawlog.Record {
	if bytes.Equal(bytes.TrimSpace(msg), []byte("pong")) {
		return nil
	}
	var v struct {
		Event string          `json:"event"`
		Error json.RawMessage `json:"error"`
		Code  json.RawMessage `json:"code"`
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
	code := string(v.Code)
	if v.Event == "error" || (len(v.Error) > 0 && string(v.Error) != "null" && string(v.Error) != "false") || (code != "" && code != "null" && code != "0" && code != `"0"` && code != `"00000"`) {
		a.errors.Add(1)
		p, _ := json.Marshal(map[string]any{"event": "subscribe_error", "frame": string(msg[:min(len(msg), 4096)])})
		return []rawlog.Record{{Channel: "gap", Payload: p}}
	}
	if v.Event == "subscribe" {
		a.acks.Add(1)
		return nil
	}
	if len(v.Data) == 0 {
		return nil
	}
	channel := map[string]string{"publicTrade": "trade", "ticker": "ticker", "funding-rate": "funding", "position-tier": "tier", "liquidation": "liquidation"}[v.Arg.Topic]
	if channel == "" {
		return gap(msg)
	}
	array := len(v.Data) > 0 && v.Data[0] == '['
	var elements []json.RawMessage
	if array {
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
		if json.Unmarshal(element, &fields) != nil {
			out = append(out, gap(element)...)
			continue
		}
		symbol := fields.Symbol
		if symbol == "" {
			symbol = v.Arg.Symbol
		}
		if symbol == "" {
			out = append(out, gap(element)...)
			continue
		}
		if channel == "liquidation" {
			a.mu.RLock()
			_, ok := a.universe[symbol]
			a.mu.RUnlock()
			if !ok {
				continue
			}
		}
		filtered, meaningful, err := a.filterObject(channel, element, false)
		if err != nil {
			out = append(out, gap(element)...)
			continue
		}
		if channel == "ticker" {
			if !meaningful {
				continue
			}
			// Bitget repeats snapshots; emit unchanged values at least once per minute.
			// Ignore exchange timestamps, but compare all other retained fields exactly.
			var filteredMap map[string]json.RawMessage
			_ = json.Unmarshal(filtered, &filteredMap)
			delete(filteredMap, "ts")
			delete(filteredMap, "T")
			identity, _ := json.Marshal(filteredMap)
			now := a.now()
			key := sh.Name + "\x00" + channel + "\x00" + symbol
			a.tickerMu.Lock()
			if a.tickers == nil {
				a.tickers = make(map[string]lastTicker)
			}
			prev, ok := a.tickers[key]
			if ok && prev.payload == string(identity) && now.Sub(prev.emitted) < time.Minute {
				a.skipped.Add(1)
				a.tickerMu.Unlock()
				continue
			}
			a.tickers[key] = lastTicker{string(identity), now}
			a.tickerMu.Unlock()
		}
		dec := json.NewDecoder(bytes.NewReader(msg))
		_, _ = dec.Token()
		var payload bytes.Buffer
		payload.WriteByte('{')
		for dec.More() {
			tok, err := dec.Token()
			if err != nil {
				return gap(msg)
			}
			key := tok.(string)
			var value json.RawMessage
			if dec.Decode(&value) != nil {
				return gap(msg)
			}
			if key == "data" {
				if array {
					value = append(append([]byte{'['}, filtered...), ']')
				} else {
					value = filtered
				}
			}
			if payload.Len() > 1 {
				payload.WriteByte(',')
			}
			name, _ := json.Marshal(key)
			payload.Write(name)
			payload.WriteByte(':')
			payload.Write(value)
		}
		payload.WriteByte('}')
		out = append(out, rawlog.Record{Channel: channel, Symbol: symbol, Payload: payload.Bytes()})
	}
	if len(out) > 0 {
		now := a.now()
		key := sh.Name + "\x00" + channel
		samples.Lock()
		last, ok := samples.last[key]
		if !ok || now.Sub(last) >= time.Hour {
			samples.last[key] = now
			sample := append([]byte(`{"sample":`), msg...)
			sample = append(sample, '}')
			out = append(out, rawlog.Record{Channel: channel, Symbol: out[0].Symbol, Payload: sample})
		}
		samples.Unlock()
	}
	return out
}
