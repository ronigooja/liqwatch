// Package bybit implements the Bybit linear perpetual venue adapter.
package bybit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

// keepFields identifies recordable data items and their interpretation metadata.
var keepFields = map[string]map[string]bool{
	"mark":        {"symbol": true, "markPrice": true, "indexPrice": true, "openInterest": true, "openInterestValue": true, "singleOpenInterest": true, "singleOpenInterestValue": true, "fundingIntervalHour": true, "fundingCap": true, "nextFundingTime": true, "fundingRate": true},
	"trade":       {"T": true, "s": true, "S": true, "v": true, "p": true, "i": true, "BT": true, "RPI": true, "seq": true},
	"liquidation": {"T": true, "s": true, "S": true, "v": true, "p": true},
}

// dropFields identifies ticker statistics and trade tick-direction statistics outside recorded items.
var dropFields = map[string]map[string]bool{
	"mark":        {"tickDirection": true, "price24hPcnt": true, "lastPrice": true, "prevPrice24h": true, "highPrice24h": true, "lowPrice24h": true, "prevPrice1h": true, "turnover24h": true, "volume24h": true, "bid1Price": true, "bid1Size": true, "ask1Price": true, "ask1Size": true, "preOpenPrice": true, "preQty": true, "curPreListingPhase": true},
	"trade":       {"L": true},
	"liquidation": {},
}
var unknownFields atomic.Uint64
var droppedDeltas atomic.Uint64
var sampleMu sync.Mutex
var sampleTimes = map[string]time.Time{}
var sampleNow = time.Now

// UnknownFieldsSeen returns the number of unclassified fields retained.
func (*Adapter) UnknownFieldsSeen() uint64 { return unknownFields.Load() }

// DroppedDeltas returns the number of ticker deltas containing no recordable data.
func (*Adapter) DroppedDeltas() uint64 { return droppedDeltas.Load() }

// orderedObject preserves field order and original value bytes while normalizing keys.
func orderedObject(b []byte, f func(string, json.RawMessage) (json.RawMessage, bool)) ([]byte, error) {
	b = bytes.TrimSpace(b)
	if len(b) < 2 || b[0] != '{' || !json.Valid(b) {
		return nil, fmt.Errorf("expected object")
	}
	out := make([]byte, 0, len(b))
	out = append(out, '{')
	for i := 1; ; {
		for i < len(b) && (b[i] == ' ' || b[i] == '\n' || b[i] == '\r' || b[i] == '\t' || b[i] == ',') {
			i++
		}
		if b[i] == '}' {
			break
		}
		start := i
		i = stringEnd(b, i)
		rawKey := b[start:i]
		var key string
		if bytes.IndexByte(rawKey, '\\') < 0 && utf8.Valid(rawKey) {
			key = string(rawKey[1 : len(rawKey)-1])
		} else {
			_ = json.Unmarshal(rawKey, &key)
		}
		for b[i] != ':' {
			i++
		}
		i++
		for b[i] == ' ' || b[i] == '\n' || b[i] == '\r' || b[i] == '\t' {
			i++
		}
		start = i
		depth := 0
		for i < len(b) {
			switch b[i] {
			case '"':
				i = stringEnd(b, i)
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
		value, keep := f(key, json.RawMessage(bytes.TrimSpace(b[start:i])))
		if !keep {
			continue
		}
		if len(out) > 1 {
			out = append(out, ',')
		}
		if bytes.IndexAny(rawKey, "\\<>&") < 0 && utf8.Valid(rawKey) && !bytes.Contains(rawKey, []byte("\u2028")) && !bytes.Contains(rawKey, []byte("\u2029")) {
			out = append(out, rawKey...)
		} else {
			k, _ := json.Marshal(key)
			out = append(out, k...)
		}
		out = append(out, ':')
		out = append(out, value...)
	}
	return append(out, '}'), nil
}

func stringEnd(b []byte, i int) int {
	for i++; i < len(b); i++ {
		if b[i] == '\\' {
			i++
			continue
		}
		if b[i] == '"' {
			return i + 1
		}
	}
	return i
}

// WSLinear is Bybit's public linear derivatives endpoint.
const WSLinear = "wss://stream.bybit.com/v5/public/linear"
const instrumentsURL = "https://api.bybit.com/v5/market/instruments-info"
const maxStreams = 90 // Operational cap, not a documented Bybit limit.

// Adapter holds optional overrides for instrument discovery.
type Adapter struct {
	HTTPClient      *http.Client
	InstrumentsURL  string
	subscribeAcks   atomic.Uint64
	subscribeErrors atomic.Uint64
}

// SubscribeAcks returns the number of successful subscription acknowledgements.
func (a *Adapter) SubscribeAcks() uint64 { return a.subscribeAcks.Load() }

// SubscribeErrors returns the number of subscription failure frames.
func (a *Adapter) SubscribeErrors() uint64 { return a.subscribeErrors.Load() }

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
func (a *Adapter) Split(sh venue.Shard, msg []byte) []rawlog.Record {
	var x struct {
		Topic   string          `json:"topic"`
		Type    string          `json:"type"`
		Data    json.RawMessage `json:"data"`
		Op      string          `json:"op"`
		Success *bool           `json:"success"`
		RetMsg  string          `json:"ret_msg"`
	}
	err := json.Unmarshal(msg, &x)
	if err == nil && x.Op == "subscribe" {
		if (x.Success != nil && !*x.Success) || x.RetMsg != "" {
			a.subscribeErrors.Add(1)
			var frame any = json.RawMessage(msg)
			if len(msg) > 4096 {
				frame = string(msg[:4096])
			}
			payload, _ := json.Marshal(map[string]any{"event": "subscribe_error", "frame": frame})
			return []rawlog.Record{{Channel: "gap", Payload: payload}}
		}
		if x.Success != nil && *x.Success {
			a.subscribeAcks.Add(1)
		}
		return nil
	}
	if err == nil && len(x.Data) == 0 && x.Topic == "" && x.Op != "" {
		return nil
	}
	if err == nil && len(x.Data) > 0 && string(x.Data) != "null" {
		p := strings.SplitN(x.Topic, ".", 2)
		if len(p) == 2 && p[1] != "" {
			ch := map[string]string{"publicTrade": "trade", "tickers": "mark", "allLiquidation": "liquidation"}[p[0]]
			if ch != "" {
				var items []json.RawMessage
				batch := x.Data[0] == '['
				if batch {
					err = json.Unmarshal(x.Data, &items)
				} else {
					items = []json.RawMessage{x.Data}
				}
				if err == nil {
					var out []rawlog.Record
					valid := true
					for _, item := range items {
						meaningful := false
						filtered, e := orderedObject(item, func(k string, v json.RawMessage) (json.RawMessage, bool) {
							if batch && k == "s" {
								var symbol string
								if json.Unmarshal(v, &symbol) == nil && symbol == p[1] {
									return nil, false
								}
							}
							if dropFields[ch][k] {
								return nil, false
							}
							if !keepFields[ch][k] {
								unknownFields.Add(1)
							}
							if k != "symbol" && k != "s" && k != "ts" {
								meaningful = true
							}
							return v, true
						})
						if e != nil {
							valid = false
							break
						}
						if ch == "mark" && x.Type == "delta" && !meaningful {
							droppedDeltas.Add(1)
							continue
						}
						if batch {
							filtered = append(append([]byte{'['}, filtered...), ']')
						}
						payload, e := orderedObject(msg, func(k string, v json.RawMessage) (json.RawMessage, bool) {
							if k == "data" {
								return filtered, true
							}
							return v, true
						})
						if e != nil {
							valid = false
							break
						}
						out = append(out, rawlog.Record{Channel: ch, Symbol: p[1], Payload: payload})
					}
					if valid {
						sampleMu.Lock()
						now := sampleNow()
						key := sh.Name + "\x00" + ch
						last, exists := sampleTimes[key]
						sample := !exists || now.Sub(last) >= time.Hour
						if sample {
							sampleTimes[key] = now
						}
						sampleMu.Unlock()
						if sample {
							payload := append(append([]byte(`{"sample":`), msg...), '}')
							out = append(out, rawlog.Record{Channel: ch, Symbol: p[1], Payload: payload})
						}
						return out
					}
				}
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
