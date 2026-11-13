// Package mexc implements the MEXC futures venue adapter.
package mexc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
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

// Field tables describe recorded items; unspecified fields are retained and counted.
var keepFields = map[string]map[string]bool{
	"trade":   {"p": true, "v": true, "T": true, "O": true, "M": true, "t": true, "i": true, "cts": true},                                               // trades
	"mark":    {"symbol": true, "price": true},                                                                                                          // mark price
	"index":   {"symbol": true, "price": true},                                                                                                          // index price
	"funding": {"symbol": true, "rate": true, "nextSettleTime": true},                                                                                   // funding
	"ticker":  {"symbol": true, "fairPrice": true, "indexPrice": true, "holdVol": true, "fundingRate": true, "nextSettleTime": true, "timestamp": true}, // prices, open interest, funding
}
var dropFields = map[string]map[string]bool{
	"ticker": {"lastPrice": true, "riseFallRate": true, "volume24": true, "amount24": true, "maxBidPrice": true, "minAskPrice": true, "lower24Price": true, "high24Price": true, "bid1": true, "ask1": true, "riseFallValue": true, "zone": true, "riseFallRates": true, "riseFallRatesOfTimezone": true}, // quote and statistics
}

var samples = struct {
	sync.Mutex
	last map[string]time.Time
	now  func() time.Time
}{last: make(map[string]time.Time), now: time.Now}

type Adapter struct {
	// DetailURL and Client optionally override symbol discovery for tests.
	DetailURL string
	Client    *http.Client
	mu        sync.RWMutex
	universe  map[string]struct{}
	unknown   atomic.Uint64
	acks      atomic.Uint64
	errors    atomic.Uint64
}

// UnknownFields returns the number of unrecognized data fields retained.
func (a *Adapter) UnknownFields() uint64 { return a.unknown.Load() }

// SubscribeAcks and SubscribeErrors count subscription responses across shards.
func (a *Adapter) SubscribeAcks() uint64   { return a.acks.Load() }
func (a *Adapter) SubscribeErrors() uint64 { return a.errors.Load() }

func responseFailed(data json.RawMessage) bool {
	var text string
	if json.Unmarshal(data, &text) == nil {
		return !strings.EqualFold(text, "success")
	}
	var result struct {
		Success *bool `json:"success"`
		Code    *int  `json:"code"`
	}
	return json.Unmarshal(data, &result) == nil && ((result.Success != nil && !*result.Success) || (result.Code != nil && *result.Code != 0))
}

// filterObject preserves source order and raw JSON values while removing known drops.
func (a *Adapter) filterObject(ch string, data []byte) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("invalid object")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if _, err := d.Token(); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.WriteByte('{')
	first := true
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return nil, err
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, err
		}
		name := key.(string)
		if dropFields[ch][name] {
			continue
		}
		if !keepFields[ch][name] {
			a.unknown.Add(1)
		}
		if !first {
			out.WriteByte(',')
		}
		first = false
		k, _ := json.Marshal(name)
		out.Write(k)
		out.WriteByte(':')
		out.Write(value)
	}
	out.WriteByte('}')
	return out.Bytes(), nil
}

func sampled(sh venue.Shard, ch string, msg []byte, records []rawlog.Record) []rawlog.Record {
	samples.Lock()
	defer samples.Unlock()
	now := samples.now()
	key := sh.Name + "\x00" + ch
	last, ok := samples.last[key]
	if !ok || now.Sub(last) >= time.Hour {
		samples.last[key] = now
		payload := append([]byte(`{"sample":`), msg...)
		payload = append(payload, '}')
		records = append(records, rawlog.Record{Channel: ch, Symbol: records[0].Symbol, Payload: payload})
	}
	return records
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

func (a *Adapter) Split(sh venue.Shard, msg []byte) []rawlog.Record {
	var v struct {
		Channel string          `json:"channel"`
		Symbol  string          `json:"symbol"`
		Data    json.RawMessage `json:"data"`
	}
	err := json.Unmarshal(msg, &v)
	if err == nil && v.Channel == "pong" {
		return nil
	}
	if err == nil && strings.HasPrefix(v.Channel, "rs.") {
		if v.Channel == "rs.error" || responseFailed(v.Data) {
			a.errors.Add(1)
			p, _ := json.Marshal(map[string]any{"event": "subscribe_error", "frame": string(msg[:min(len(msg), 4096)])})
			return []rawlog.Record{{Channel: "gap", Payload: p}}
		}
		if strings.HasPrefix(v.Channel, "rs.sub.") {
			a.acks.Add(1)
		}
		return nil
	}
	if err == nil && strings.HasPrefix(v.Channel, "push.contract") {
		a.mu.RLock()
		_, ok := a.universe[v.Symbol]
		a.mu.RUnlock()
		if !ok {
			return nil
		}
		ch := "contractmeta"
		switch v.Channel {
		case "push.contract":
			ch = "contract"
		case "push.contract.extra.update":
			ch = "contractextra"
		case "push.contract.marketOrderSetting.update":
			ch = "ordersetting"
		}
		return sampled(sh, ch, msg, []rawlog.Record{{Channel: ch, Symbol: v.Symbol, Payload: msg}})
	}
	channels := map[string]string{"push.deal": "trade", "push.fair.price": "mark", "push.index.price": "index", "push.ticker": "ticker", "push.funding.rate": "funding"}
	ch, known := channels[v.Channel]
	if err == nil && known && len(v.Data) > 0 && string(v.Data) != "null" && v.Symbol != "" {
		payload := msg
		if ch != "trade" {
			filtered, e := a.filterObject(ch, v.Data)
			if e != nil {
				goto gap
			}
			// Replace only data, preserving the envelope and its order.
			idx := bytes.Index(msg, v.Data)
			if idx < 0 {
				goto gap
			}
			payload = append(append(append([]byte{}, msg[:idx]...), filtered...), msg[idx+len(v.Data):]...)
		} else {
			var items []json.RawMessage
			if len(v.Data) > 0 && v.Data[0] == '[' {
				_ = json.Unmarshal(v.Data, &items)
			} else {
				items = []json.RawMessage{v.Data}
			}
			for _, item := range items {
				if _, e := a.filterObject(ch, item); e != nil {
					goto gap
				}
			}
		}
		return sampled(sh, ch, msg, []rawlog.Record{{Channel: ch, Symbol: v.Symbol, Payload: payload}})
	}
	if err == nil && (len(v.Data) == 0 || string(v.Data) == "null") && !strings.HasPrefix(v.Channel, "push.") {
		return nil
	}
gap:
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
