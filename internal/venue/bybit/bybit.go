// Package bybit implements the Bybit linear perpetual venue adapter.
package bybit

import (
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
	tickerMu        sync.Mutex
	tickers         map[string]*tickerState
}

type tickerField struct {
	key   string
	value []byte
}
type tickerState struct {
	fields   []tickerField
	complete bool
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
		if req.Header.Get("User-Agent") == "" {
			req.Header.Set("User-Agent", "Mozilla/5.0")
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
func (a *Adapter) Subscribe(sh venue.Shard) [][]byte {
	// Subscribe is called at the start of each connection, including reconnects.
	a.tickerMu.Lock()
	for _, stream := range sh.Streams {
		if symbol, ok := strings.CutPrefix(stream, "tickers."); ok {
			delete(a.tickers, symbol)
		}
	}
	a.tickerMu.Unlock()
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
	s := jsonScanner{b: msg}
	root, valid := s.value(0, true)
	s.ws()
	valid = valid && s.i == len(msg)
	var topic, typ, op, ret string
	var data *jsonNode
	success := -1
	if valid && root.start < len(msg) && msg[root.start] == '{' {
		for i := range root.children {
			n := &root.children[i]
			raw := msg[n.start:n.end]
			key := n.key
			switch {
			case strings.EqualFold(key, "topic"), strings.EqualFold(key, "type"), strings.EqualFold(key, "op"), strings.EqualFold(key, "ret_msg"):
				if string(raw) == "null" {
					continue
				}
				if len(raw) == 0 || raw[0] != '"' {
					valid = false
					continue
				}
				v := decodeString(raw)
				switch {
				case strings.EqualFold(key, "topic"):
					topic = v
				case strings.EqualFold(key, "type"):
					typ = v
				case strings.EqualFold(key, "op"):
					op = v
				default:
					ret = v
				}
			case strings.EqualFold(key, "success"):
				switch string(raw) {
				case "null":
					success = -1
				case "true":
					success = 1
				case "false":
					success = 0
				default:
					valid = false
				}
			case strings.EqualFold(key, "data"):
				data = n
			}
		}
	} else if valid && string(msg[root.start:root.end]) != "null" {
		valid = false
	}
	if valid && op == "subscribe" {
		if success == 0 || ret != "" {
			a.subscribeErrors.Add(1)
			var frame any = json.RawMessage(msg)
			if len(msg) > 4096 {
				frame = string(msg[:4096])
			}
			payload, _ := json.Marshal(map[string]any{"event": "subscribe_error", "frame": frame})
			return []rawlog.Record{{Channel: "gap", Payload: payload}}
		}
		if success == 1 {
			a.subscribeAcks.Add(1)
		}
		return nil
	}
	if valid && data == nil && topic == "" && op != "" {
		return nil
	}
	if valid && data != nil && string(msg[data.start:data.end]) != "null" {
		prefix, symbol, ok := strings.Cut(topic, ".")
		ch := ""
		switch prefix {
		case "publicTrade":
			ch = "trade"
		case "tickers":
			ch = "mark"
		case "allLiquidation":
			ch = "liquidation"
		}
		if ok && symbol != "" && ch != "" {
			batch := msg[data.start] == '['
			items := []jsonNode{*data}
			if batch {
				items = data.children
			}
			var out []rawlog.Record
			validItems := true
			for _, item := range items {
				if msg[item.start] != '{' {
					validItems = false
					break
				}
				filtered := make([]byte, 0, item.end-item.start)
				filtered = append(filtered, '{')
				meaningful := false
				for _, field := range item.children {
					k := field.key
					v := msg[field.start:field.end]
					if batch && k == "s" && len(v) > 0 && v[0] == '"' && decodeString(v) == symbol {
						continue
					}
					if dropFields[ch][k] {
						continue
					}
					if !keepFields[ch][k] {
						unknownFields.Add(1)
					}
					if k != "symbol" && k != "s" && k != "ts" {
						meaningful = true
					}
					if len(filtered) > 1 {
						filtered = append(filtered, ',')
					}
					filtered = appendKey(filtered, k)
					filtered = append(filtered, ':')
					filtered = append(filtered, v...)
				}
				filtered = append(filtered, '}')
				if ch == "mark" && typ == "delta" && !meaningful {
					droppedDeltas.Add(1)
					continue
				}
				partial := false
				if ch == "mark" && (typ == "snapshot" || typ == "delta") {
					filtered, partial = a.mergeTicker(symbol, typ, item.children, msg)
				}
				payload := make([]byte, 0, len(msg))
				payload = append(payload, '{')
				for i, field := range root.children {
					if i > 0 {
						payload = append(payload, ',')
					}
					payload = appendKey(payload, field.key)
					payload = append(payload, ':')
					if field.key == "data" {
						if batch {
							payload = append(payload, '[')
						}
						payload = append(payload, filtered...)
						if batch {
							payload = append(payload, ']')
						}
					} else {
						payload = append(payload, msg[field.start:field.end]...)
					}
				}
				if partial {
					payload = append(payload, `,"state":"partial"`...)
				}
				payload = append(payload, '}')
				out = append(out, rawlog.Record{Channel: ch, Symbol: symbol, Payload: payload})
			}
			if validItems {
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
					out = append(out, rawlog.Record{Channel: ch, Symbol: symbol, Payload: payload})
				}
				return out
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

// mergeTicker retains original JSON value bytes and snapshot field order. A delta
// before a snapshot is marked partial so consumers can ignore incomplete state.
func (a *Adapter) mergeTicker(symbol, typ string, fields []jsonNode, msg []byte) ([]byte, bool) {
	a.tickerMu.Lock()
	defer a.tickerMu.Unlock()
	if a.tickers == nil {
		a.tickers = make(map[string]*tickerState)
	}
	state := a.tickers[symbol]
	if state == nil || typ == "snapshot" {
		state = &tickerState{complete: typ == "snapshot"}
		a.tickers[symbol] = state
	}
	for _, field := range fields {
		if dropFields["mark"][field.key] {
			continue
		}
		i := 0
		for i < len(state.fields) && state.fields[i].key != field.key {
			i++
		}
		value := append([]byte(nil), msg[field.start:field.end]...)
		if i == len(state.fields) {
			state.fields = append(state.fields, tickerField{field.key, value})
		} else {
			state.fields[i].value = value
		}
	}
	out := []byte{'{'}
	for i, field := range state.fields {
		if i > 0 {
			out = append(out, ',')
		}
		out = appendKey(out, field.key)
		out = append(out, ':')
		out = append(out, field.value...)
	}
	out = append(out, '}')
	return out, !state.complete
}
