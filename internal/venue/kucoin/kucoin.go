// Package kucoin records public KuCoin futures streams.
package kucoin

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

const api = "https://api-futures.kucoin.com"
const utaURL = "wss://x-push-futures.kucoin.com"
const classicSymbols = 25
const utaSymbols = 50

// Trade item, envelope and timestamps; order metadata is not recorded.
var tradeKeep = fields("data side size price tradeId ts timestamp")
var tradeDrop = fields("topic type subject symbol sn sequence takerOrderId makerOrderId")

// Mark, index and funding items; delivery cadence is not recorded.
var markKeep = fields("data symbol markPrice indexPrice fundingRate nextFundingTime timestamp ts")
var markDrop = fields("topic type subject granularity period")

// UTA mark, index, open interest and funding items; ticker statistics are not recorded.
var utaKeep = fields("d symbol mp ip oi ts fundingRate nextFundingTime fr nft")
var utaOuterKeep = fields("P")
var utaDrop = fields("T s bp bq ap aq bidPrice bidSize askPrice askSize bestBid bestAsk bestBidSize bestAskSize vol volume turnover high low priceChange priceChangePercent lastPrice last p v h l")

func fields(s string) map[string]bool {
	m := make(map[string]bool)
	for _, k := range strings.Fields(s) {
		m[k] = true
	}
	return m
}

var samples = struct {
	sync.Mutex
	last map[string]time.Time
	now  func() time.Time
}{last: make(map[string]time.Time), now: time.Now}

var unknownFields atomic.Uint64

// UnknownFields returns the number of retained fields not classified by the tables.
func (*Adapter) UnknownFields() uint64 { return unknownFields.Load() }

func sample(shard, channel string, msg []byte) []byte {
	samples.Lock()
	defer samples.Unlock()
	now := samples.now()
	key := shard + "\x00" + channel
	if last, ok := samples.last[key]; ok && now.Sub(last) < time.Hour {
		return nil
	}
	samples.last[key] = now
	out := append([]byte(`{"sample":`), msg...)
	return append(out, '}')
}

// filterObject uses the decoder's input offsets to retain source order and raw values.
func filterObject(src []byte, keep, drop, outerKeep map[string]bool) ([]byte, error) {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(src, &values); err != nil || values == nil {
		return nil, fmt.Errorf("invalid object")
	}
	dec := json.NewDecoder(bytes.NewReader(src))
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	out := []byte{'{'}
	for dec.More() {
		start := dec.InputOffset()
		keyToken, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, fmt.Errorf("invalid key")
		}
		keyEnd := dec.InputOffset()
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
		if drop[key] {
			continue
		}
		if !keep[key] && !outerKeep[key] {
			unknownFields.Add(1)
		}
		if key == "data" || key == "d" {
			value, err = filterObject(value, keep, drop, nil)
			if err != nil {
				return nil, err
			}
		}
		keyBytes := bytes.TrimLeft(src[start:keyEnd], " \t\r\n,:")
		if len(out) > 1 {
			out = append(out, ',')
		}
		out = append(out, keyBytes...)
		out = append(out, ':')
		out = append(out, value...)
	}
	return append(out, '}'), nil
}

func filtered(sh venue.Shard, channel, symbol string, msg []byte, keep, drop, outerKeep map[string]bool) []rawlog.Record {
	p, err := filterObject(msg, keep, drop, outerKeep)
	if err != nil {
		return gap(msg)
	}
	out := []rawlog.Record{{Channel: channel, Symbol: symbol, Payload: p}}
	if p := sample(sh.Name, channel, msg); p != nil {
		out = append(out, rawlog.Record{Channel: channel, Symbol: symbol, Payload: p})
	}
	return out
}

// Adapter discovers classic futures and subscribes to classic and UTA public feeds.
type Adapter struct {
	BaseURL   string
	Client    *http.Client
	fallbacks atomic.Uint64
	acks      atomic.Uint64
	errors    atomic.Uint64
	mu        sync.RWMutex
	intervals map[string]time.Duration
}

func init()                   { venue.Register("kucoin", func() venue.Adapter { return &Adapter{} }) }
func (*Adapter) Name() string { return "kucoin" }

// SubscribeAcks returns the number of successful subscription acknowledgements.
func (a *Adapter) SubscribeAcks() uint64 { return a.acks.Load() }

// SubscribeErrors returns the number of subscription error frames.
func (a *Adapter) SubscribeErrors() uint64 { return a.errors.Load() }

func (a *Adapter) request(ctx context.Context, method, path string, dst any) error {
	base := a.BaseURL
	if base == "" {
		base = api
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, nil)
	if err != nil {
		return err
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "Mozilla/5.0")
	}
	c := a.Client
	if c == nil {
		c = &http.Client{Timeout: 20 * time.Second}
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("kucoin %s: HTTP %d", path, resp.StatusCode)
	}
	var result struct {
		Code string          `json:"code"`
		Data json.RawMessage `json:"data"`
		Msg  string          `json:"msg"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}
	if result.Code != "200000" {
		return fmt.Errorf("kucoin %s: %s %s", path, result.Code, result.Msg)
	}
	return json.Unmarshal(result.Data, dst)
}

func (a *Adapter) Symbols(ctx context.Context) ([]venue.Symbol, error) {
	var rows []struct {
		Symbol string `json:"symbol"`
		Settle string `json:"settleCurrency"`
		Type   string `json:"type"`
		Status string `json:"status"`
	}
	if err := a.request(ctx, http.MethodGet, "/api/v1/contracts/active", &rows); err != nil {
		return nil, err
	}
	var syms []venue.Symbol
	for _, r := range rows {
		if r.Symbol != "" && r.Settle == "USDT" && r.Type == "FFWCSX" && r.Status == "Open" {
			syms = append(syms, r.Symbol)
		}
	}
	return syms, nil
}

type publicToken struct {
	Token   string `json:"token"`
	Servers []struct {
		Endpoint     string `json:"endpoint"`
		PingInterval int64  `json:"pingInterval"`
		PingTimeout  int64  `json:"pingTimeout"`
	} `json:"instanceServers"`
}

func (a *Adapter) publicToken(ctx context.Context) (publicToken, error) {
	var token publicToken
	if err := a.request(ctx, http.MethodPost, "/api/v1/bullet-public", &token); err != nil {
		return token, err
	}
	if token.Token == "" || len(token.Servers) == 0 || token.Servers[0].PingInterval <= 0 {
		return token, fmt.Errorf("kucoin: invalid public token response")
	}
	return token, nil
}

func tokenURL(token publicToken) (string, error) {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return "", err
	}
	u, err := url.Parse(token.Servers[0].Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "wss" && u.Scheme != "ws") {
		return "", fmt.Errorf("kucoin: invalid websocket endpoint")
	}
	q := u.Query()
	q.Set("token", token.Token)
	q.Set("connectId", hex.EncodeToString(id))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (a *Adapter) Shards(ctx context.Context, syms []venue.Symbol) ([]venue.Shard, error) {
	if len(syms) == 0 {
		return nil, nil
	}
	token, err := a.publicToken(ctx)
	if err != nil {
		return nil, err
	}
	server := token.Servers[0]
	interval := time.Duration(server.PingInterval) * time.Millisecond / 2
	var shards []venue.Shard
	intervals := map[string]time.Duration{}
	for i := 0; i < len(syms); i += classicSymbols {
		u, err := tokenURL(token)
		if err != nil {
			return nil, err
		}
		var streams []string
		for _, s := range syms[i:min(i+classicSymbols, len(syms))] {
			streams = append(streams, "/contractMarket/execution:"+s, "/contract/instrument:"+s)
		}
		sh := venue.Shard{Name: fmt.Sprintf("classic%03d", i/classicSymbols), URL: u, Streams: streams, Stall: 60 * time.Second, MaxAge: 12 * time.Hour, MaxSubsPerConn: 50, SubscribeSpacing: 200 * time.Millisecond}
		sh.Refresh = func(ctx context.Context) (string, error) {
			token, err := a.publicToken(ctx)
			if err != nil {
				return "", err
			}
			return tokenURL(token)
		}
		shards = append(shards, sh)
		intervals[sh.URL] = interval
	}
	for i := 0; i < len(syms); i += utaSymbols {
		streams := append([]string(nil), syms[i:min(i+utaSymbols, len(syms))]...)
		shards = append(shards, venue.Shard{Name: fmt.Sprintf("uta%03d", i/utaSymbols), URL: utaURL, Streams: streams, Stall: 60 * time.Second, MaxAge: 12 * time.Hour, MaxSubsPerConn: utaSymbols, SubscribeSpacing: 200 * time.Millisecond})
	}
	a.mu.Lock()
	if a.intervals == nil {
		a.intervals = map[string]time.Duration{}
	}
	for k, v := range intervals {
		a.intervals[k] = v
	}
	a.mu.Unlock()
	return shards, nil
}

func (*Adapter) Subscribe(sh venue.Shard) [][]byte {
	var out [][]byte
	for i, s := range sh.Streams {
		var v any
		if strings.HasPrefix(sh.Name, "uta") {
			v = map[string]any{"id": fmt.Sprint(i), "action": "SUBSCRIBE", "channel": "mark-price", "symbol": s}
		} else {
			v = map[string]any{"id": fmt.Sprint(i), "type": "subscribe", "topic": s, "response": true, "privateChannel": false}
		}
		b, _ := json.Marshal(v)
		out = append(out, b)
	}
	return out
}

func (a *Adapter) Heartbeat(sh venue.Shard) (time.Duration, func() (int, []byte)) {
	if strings.HasPrefix(sh.Name, "uta") {
		return 9 * time.Second, func() (int, []byte) { return 1, []byte(fmt.Sprintf(`{"id":"%d","op":"ping"}`, time.Now().UnixMilli())) }
	}
	a.mu.RLock()
	interval := a.intervals[sh.URL]
	a.mu.RUnlock()
	if interval <= 0 {
		return 0, nil
	}
	return interval, func() (int, []byte) {
		return 1, []byte(fmt.Sprintf(`{"id":"%d","type":"ping"}`, time.Now().UnixMilli()))
	}
}

// SplitFallbacks returns frames handled by the compatibility path.
func (a *Adapter) SplitFallbacks() uint64 { return a.fallbacks.Load() }

func (a *Adapter) Split(sh venue.Shard, msg []byte) []rawlog.Record {
	f, ok := scan(msg)
	if ok {
		// Control and uncommon frames retain the exact legacy behavior.
		if f.typ != "error" && f.typ != "ack" && f.typ != "welcome" && f.typ != "pong" && f.code == "" && f.result == "" && f.op == "" {
			channel, symbol := "", ""
			if f.t == "mark-price" && f.symbol != "" {
				channel, symbol = "utamark", f.symbol
			} else if f.typ == "message" && f.hasData {
				parts := strings.SplitN(f.topic, ":", 2)
				if len(parts) == 2 && parts[1] != "" {
					symbol = parts[1]
					if parts[0] == "/contractMarket/execution" && f.subject == "match" {
						channel = "trade"
					}
					if parts[0] == "/contract/instrument" && (f.subject == "mark.index.price" || f.subject == "funding.rate") {
						channel = "mark"
					}
				}
			}
			if channel != "" {
				keep, drop := utaKeep, utaDrop
				outerKeep := utaOuterKeep
				if channel == "trade" {
					keep, drop = tradeKeep, tradeDrop
					outerKeep = nil
				} else if channel == "mark" {
					keep, drop = markKeep, markDrop
					outerKeep = nil
				}
				if p, n, valid := scanFilter(f.fields, keep, drop, outerKeep); valid {
					unknownFields.Add(n)
					out := []rawlog.Record{{Channel: channel, Symbol: symbol, Payload: p}}
					if p := sample(sh.Name, channel, msg); p != nil {
						out = append(out, rawlog.Record{Channel: channel, Symbol: symbol, Payload: p})
					}
					return out
				}
			}
		}
	}
	a.fallbacks.Add(1)
	return a.legacySplit(sh, msg)
}

func (a *Adapter) legacySplit(sh venue.Shard, msg []byte) []rawlog.Record {
	var f struct {
		Type    string          `json:"type"`
		Op      string          `json:"op"`
		Code    json.RawMessage `json:"code"`
		Result  json.RawMessage `json:"result"`
		Topic   string          `json:"topic"`
		Subject string          `json:"subject"`
		Data    json.RawMessage `json:"data"`
		T       string          `json:"T"`
		D       struct {
			Symbol string `json:"s"`
		} `json:"d"`
	}
	if json.Unmarshal(msg, &f) != nil {
		return gap(msg)
	}
	code := strings.Trim(string(f.Code), `"`)
	result := strings.Trim(string(f.Result), `"`)
	if f.Type == "error" || (len(f.Code) > 0 && code != "200000" && code != "0" && code != "null") || result == "false" {
		a.errors.Add(1)
		var frame any = json.RawMessage(msg)
		if len(msg) > 4096 {
			frame = string(msg[:4096])
		}
		p, _ := json.Marshal(map[string]any{"event": "subscribe_error", "frame": frame})
		return []rawlog.Record{{Channel: "gap", Payload: p}}
	}
	if f.Type == "ack" || result == "true" || (f.Op != "" && f.Op != "pong" && (code == "200000" || code == "0")) {
		a.acks.Add(1)
		return nil
	}
	if f.Type == "welcome" || f.Type == "pong" || f.Op == "pong" {
		return nil
	}
	if f.T == "mark-price" && f.D.Symbol != "" {
		return filtered(sh, "utamark", f.D.Symbol, msg, utaKeep, utaDrop, utaOuterKeep)
	}
	if f.Type == "message" {
		parts := strings.SplitN(f.Topic, ":", 2)
		if len(parts) == 2 && parts[1] != "" && len(f.Data) > 0 {
			ch := ""
			switch parts[0] {
			case "/contractMarket/execution":
				if f.Subject == "match" {
					ch = "trade"
				}
			case "/contract/instrument":
				if f.Subject == "mark.index.price" || f.Subject == "funding.rate" {
					ch = "mark"
				}
			}
			if ch != "" {
				if ch == "trade" {
					return filtered(sh, ch, parts[1], msg, tradeKeep, tradeDrop, nil)
				}
				return filtered(sh, ch, parts[1], msg, markKeep, markDrop, nil)
			}
		}
		return gap(msg)
	}
	if f.T != "" || len(f.Data) > 0 {
		return gap(msg)
	}
	return nil
}
func gap(msg []byte) []rawlog.Record {
	p, _ := json.Marshal(map[string]any{"event": "unrecognized_frame", "bytes": len(msg), "head": string(msg[:min(len(msg), 200)])})
	return []rawlog.Record{{Channel: "gap", Payload: p}}
}
func (*Adapter) Classes() func(string) (string, bool) {
	return func(ch string) (string, bool) {
		switch ch {
		case "trade":
			return "trade", true
		case "mark", "utamark":
			return "mark", true
		}
		return "", false
	}
}
