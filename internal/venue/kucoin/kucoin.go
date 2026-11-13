// Package kucoin records public KuCoin futures streams.
package kucoin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

const api = "https://api-futures.kucoin.com"
const utaURL = "wss://x-push-futures.kucoin.com"
const classicSymbols = 25
const utaSymbols = 50

// Adapter discovers classic futures and subscribes to classic and UTA public feeds.
type Adapter struct {
	BaseURL   string
	Client    *http.Client
	mu        sync.RWMutex
	intervals map[string]time.Duration
}

func init()                   { venue.Register("kucoin", func() venue.Adapter { return &Adapter{} }) }
func (*Adapter) Name() string { return "kucoin" }

func (a *Adapter) request(ctx context.Context, method, path string, dst any) error {
	base := a.BaseURL
	if base == "" {
		base = api
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, nil)
	if err != nil {
		return err
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

func (a *Adapter) Shards(ctx context.Context, syms []venue.Symbol) ([]venue.Shard, error) {
	if len(syms) == 0 {
		return nil, nil
	}
	var token struct {
		Token   string `json:"token"`
		Servers []struct {
			Endpoint     string `json:"endpoint"`
			PingInterval int64  `json:"pingInterval"`
			PingTimeout  int64  `json:"pingTimeout"`
		} `json:"instanceServers"`
	}
	if err := a.request(ctx, http.MethodPost, "/api/v1/bullet-public", &token); err != nil {
		return nil, err
	}
	if token.Token == "" || len(token.Servers) == 0 || token.Servers[0].PingInterval <= 0 {
		return nil, fmt.Errorf("kucoin: invalid public token response")
	}
	server := token.Servers[0]
	interval := time.Duration(server.PingInterval) * time.Millisecond / 2
	var shards []venue.Shard
	intervals := map[string]time.Duration{}
	for i := 0; i < len(syms); i += classicSymbols {
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			return nil, err
		}
		u, err := url.Parse(server.Endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "wss" && u.Scheme != "ws") {
			return nil, fmt.Errorf("kucoin: invalid websocket endpoint")
		}
		q := u.Query()
		q.Set("token", token.Token)
		q.Set("connectId", hex.EncodeToString(id))
		u.RawQuery = q.Encode()
		var streams []string
		for _, s := range syms[i:min(i+classicSymbols, len(syms))] {
			streams = append(streams, "/contractMarket/execution:"+s, "/contract/instrument:"+s)
		}
		sh := venue.Shard{Name: fmt.Sprintf("classic%03d", i/classicSymbols), URL: u.String(), Streams: streams, Stall: 60 * time.Second, MaxAge: 12 * time.Hour, MaxSubsPerConn: 50, SubscribeSpacing: 200 * time.Millisecond}
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

func (*Adapter) Split(_ venue.Shard, msg []byte) []rawlog.Record {
	var f struct {
		Type    string          `json:"type"`
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
	if f.T == "mark-price" && f.D.Symbol != "" {
		return []rawlog.Record{{Channel: "openinterest", Symbol: f.D.Symbol, Payload: msg}}
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
				return []rawlog.Record{{Channel: ch, Symbol: parts[1], Payload: msg}}
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
		case "mark", "openinterest":
			return "mark", true
		}
		return "", false
	}
}
