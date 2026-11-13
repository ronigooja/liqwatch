// Package okx implements OKX USDT-margined perpetual market data.
package okx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

// Per-channel item fields: trades, mark, index, funding, open interest, liquidations.
var fieldRules = map[string]struct{ keep, drop map[string]bool }{
	"trades-all":         {fields("instId tradeId px sz side ts source"), fields("")},
	"mark-price":         {fields("instId instType markPx ts"), fields("")},
	"index-tickers":      {fields("instId idxPx ts"), fields("open24h high24h low24h sodUtc0 sodUtc8")},
	"funding-rate":       {fields("formulaType fundingRate fundingTime impactValue instId instType interestRate maxFundingRate method minFundingRate nextFundingRate nextFundingTime premium prevFundingTime settFundingRate settState ts"), fields("")},
	"open-interest":      {fields("instId instType oi oiCcy oiUsd ts"), fields("")},
	"liquidation-orders": {fields("details instFamily instId instType uly"), fields("")},
}

func fields(s string) map[string]bool {
	m := map[string]bool{}
	for _, k := range strings.Fields(s) {
		m[k] = true
	}
	return m
}

var unknownFields atomic.Uint64
var sampleClock = time.Now
var sampler = struct {
	sync.Mutex
	last map[string]time.Time
}{last: make(map[string]time.Time)}

// UnknownFields reports the number of unclassified item fields observed.
func (*Adapter) UnknownFields() uint64 { return unknownFields.Load() }

func sampleDue(shard, channel string) bool {
	sampler.Lock()
	defer sampler.Unlock()
	now := sampleClock()
	key := shard + "\x00" + channel
	if last, ok := sampler.last[key]; ok && now.Sub(last) < time.Hour {
		return false
	}
	sampler.last[key] = now
	return true
}

// filterItem decodes values as raw JSON and writes members in their original order.
func filterItem(channel string, entry []byte) ([]byte, error) {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(entry, &values); err != nil || values == nil {
		return nil, fmt.Errorf("invalid item")
	}
	d := json.NewDecoder(bytes.NewReader(entry))
	if _, err := d.Token(); err != nil {
		return nil, err
	}
	var b bytes.Buffer
	b.WriteByte('{')
	first := true
	rule := fieldRules[channel]
	for d.More() {
		tok, err := d.Token()
		if err != nil {
			return nil, err
		}
		key := tok.(string)
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, err
		}
		if rule.drop[key] {
			continue
		}
		if !rule.keep[key] {
			unknownFields.Add(1)
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		name, _ := json.Marshal(key)
		b.Write(name)
		b.WriteByte(':')
		b.Write(value)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

const (
	PublicURL      = "wss://ws.okx.com/ws/v5/public"
	BusinessURL    = "wss://ws.okx.com/ws/v5/business"
	instrumentsURL = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
	// One request per connection, at most 100 args and far below 64 KB.
	maxArgs = 100
)

type Adapter struct {
	HTTPClient      *http.Client
	InstrumentsURL  string
	mu              sync.RWMutex
	indexes         map[string]string // swap ID -> index ID from instFamily
	universe        map[string]bool
	indexSymbols    map[string]string
	subscribeAcks   atomic.Uint64
	subscribeErrors atomic.Uint64
}

func init()                   { venue.Register("okx", func() venue.Adapter { return &Adapter{} }) }
func (*Adapter) Name() string { return "okx" }

// SubscribeAcks and SubscribeErrors report control frames observed by this adapter.
func (a *Adapter) SubscribeAcks() uint64   { return a.subscribeAcks.Load() }
func (a *Adapter) SubscribeErrors() uint64 { return a.subscribeErrors.Load() }

type instrument struct {
	InstID     string `json:"instId"`
	InstType   string `json:"instType"`
	State      string `json:"state"`
	CtType     string `json:"ctType"`
	Settle     string `json:"settleCcy"`
	Family     string `json:"instFamily"`
	Underlying string `json:"uly"`
}

func (a *Adapter) Symbols(ctx context.Context) ([]venue.Symbol, error) {
	url := a.InstrumentsURL
	if url == "" {
		url = instrumentsURL
	}
	client := a.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("okx instruments: HTTP %d", resp.StatusCode)
	}
	var result struct {
		Code string       `json:"code"`
		Msg  string       `json:"msg"`
		Data []instrument `json:"data"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&result); err != nil {
		return nil, err
	}
	if result.Code != "0" {
		return nil, fmt.Errorf("okx instruments: %s %s", result.Code, result.Msg)
	}
	indexes := map[string]string{}
	var symbols []string
	for _, v := range result.Data {
		if v.InstType != "SWAP" || v.State != "live" || v.CtType != "linear" || v.Settle != "USDT" || !strings.HasSuffix(v.InstID, "-USDT-SWAP") {
			continue
		}
		index := v.Family
		if index == "" {
			index = v.Underlying
		}
		if index == "" {
			return nil, fmt.Errorf("okx instrument %s missing index family", v.InstID)
		}
		symbols = append(symbols, v.InstID)
		indexes[v.InstID] = index
	}
	sort.Strings(symbols)
	a.mu.Lock()
	a.indexes = indexes
	a.mu.Unlock()
	return symbols, nil
}

type arg struct {
	Channel  string `json:"channel"`
	InstID   string `json:"instId,omitempty"`
	InstType string `json:"instType,omitempty"`
}

func (a *Adapter) Shards(_ context.Context, syms []venue.Symbol) ([]venue.Shard, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	u := map[string]bool{}
	rev := map[string]string{}
	var pub, trade []string
	for _, s := range syms {
		idx, ok := a.indexes[s]
		if !ok {
			return nil, fmt.Errorf("okx: no instrument metadata for %s", s)
		}
		if prior, exists := rev[idx]; exists && prior != s {
			return nil, fmt.Errorf("okx: index %s shared by %s and %s", idx, prior, s)
		}
		u[s] = true
		rev[idx] = s
		for _, ch := range []string{"mark-price", "funding-rate", "open-interest"} {
			b, _ := json.Marshal(arg{Channel: ch, InstID: s})
			pub = append(pub, string(b))
		}
		b, _ := json.Marshal(arg{Channel: "index-tickers", InstID: idx})
		pub = append(pub, string(b))
		b, _ = json.Marshal(arg{Channel: "trades-all", InstID: s})
		trade = append(trade, string(b))
	}
	if len(syms) > 0 {
		b, _ := json.Marshal(arg{Channel: "liquidation-orders", InstType: "SWAP"})
		pub = append(pub, string(b))
	}
	var shards []venue.Shard
	for _, group := range []struct {
		name, url string
		streams   []string
	}{{"public", PublicURL, pub}, {"trade", BusinessURL, trade}} {
		for i := 0; i < len(group.streams); i += maxArgs {
			end := min(i+maxArgs, len(group.streams))
			shards = append(shards, venue.Shard{Name: fmt.Sprintf("%s%03d", group.name, i/maxArgs), URL: group.url, Streams: group.streams[i:end], Stall: 60 * time.Second, MaxSubsPerConn: maxArgs})
		}
	}
	a.universe = u
	a.indexSymbols = rev
	return shards, nil
}
func (*Adapter) Subscribe(sh venue.Shard) [][]byte {
	if len(sh.Streams) == 0 {
		return nil
	}
	args := make([]json.RawMessage, 0, len(sh.Streams))
	for _, s := range sh.Streams {
		args = append(args, json.RawMessage(s))
	}
	b, err := json.Marshal(struct {
		Op   string            `json:"op"`
		Args []json.RawMessage `json:"args"`
	}{"subscribe", args})
	if err != nil {
		return nil
	}
	return [][]byte{b}
}
func (*Adapter) Heartbeat(venue.Shard) (time.Duration, func() (int, []byte)) {
	return 20 * time.Second, func() (int, []byte) { return int(websocket.MessageText), []byte("ping") }
}
func gap(msg []byte) []rawlog.Record {
	b, _ := json.Marshal(map[string]any{"event": "unrecognized_frame", "bytes": len(msg), "head": string(msg[:min(len(msg), 200)])})
	return []rawlog.Record{{Channel: "gap", Payload: b}}
}
func (a *Adapter) Split(sh venue.Shard, msg []byte) []rawlog.Record {
	if string(msg) == "pong" {
		return nil
	}
	var e struct {
		Event  string          `json:"event"`
		Arg    arg             `json:"arg"`
		Data   json.RawMessage `json:"data"`
		RawArg json.RawMessage `json:"-"`
	}
	if json.Unmarshal(msg, &e) != nil {
		return gap(msg)
	}
	switch e.Event {
	case "subscribe":
		a.subscribeAcks.Add(1)
		return nil
	case "error", "channel-conn-count-error", "notice":
		a.subscribeErrors.Add(1)
		var frame any = json.RawMessage(msg)
		if len(msg) > 4096 {
			frame = string(msg[:4096])
		}
		b, _ := json.Marshal(map[string]any{"event": "subscribe_error", "frame": frame})
		return []rawlog.Record{{Channel: "gap", Payload: b}}
	}
	var envelope map[string]json.RawMessage
	_ = json.Unmarshal(msg, &envelope)
	e.RawArg = envelope["arg"]
	if len(e.Data) == 0 {
		if e.Event != "" {
			return nil
		}
		return gap(msg)
	}
	channels := map[string]string{"trades-all": "trade", "mark-price": "mark", "index-tickers": "index", "funding-rate": "funding", "open-interest": "oi", "liquidation-orders": "liquidation"}
	ch, ok := channels[e.Arg.Channel]
	if !ok {
		return gap(msg)
	}
	var entries []json.RawMessage
	if json.Unmarshal(e.Data, &entries) != nil {
		return gap(msg)
	}
	var out []rawlog.Record
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, entry := range entries {
		var item struct {
			InstID string `json:"instId"`
		}
		if json.Unmarshal(entry, &item) != nil {
			out = append(out, gap(msg)...)
			continue
		}
		sym := item.InstID
		if sym == "" {
			sym = e.Arg.InstID
		}
		if e.Arg.Channel == "index-tickers" {
			sym = a.indexSymbols[sym]
		}
		if sym == "" {
			out = append(out, gap(msg)...)
			continue
		}
		if !a.universe[sym] {
			if e.Arg.Channel == "liquidation-orders" {
				continue
			}
			out = append(out, gap(msg)...)
			continue
		}
		filtered, err := filterItem(e.Arg.Channel, entry)
		if err != nil {
			out = append(out, gap(msg)...)
			continue
		}
		payload := append([]byte(`{"arg":`), e.RawArg...)
		payload = append(payload, `,"data":[`...)
		payload = append(payload, filtered...)
		payload = append(payload, ']', '}')
		out = append(out, rawlog.Record{Channel: ch, Symbol: sym, Payload: payload})
		if sampleDue(sh.Name, ch) {
			sample := append([]byte(`{"sample":`), msg...)
			sample = append(sample, '}')
			out = append(out, rawlog.Record{Channel: ch, Symbol: sym, Payload: sample})
		}
	}
	return out
}
func (*Adapter) Classes() func(string) (string, bool) {
	return func(ch string) (string, bool) {
		switch ch {
		case "trade":
			return "trade", true
		case "mark", "index", "funding", "oi":
			return "mark", true
		}
		return "", false
	}
}
