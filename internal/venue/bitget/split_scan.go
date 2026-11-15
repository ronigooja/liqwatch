package bitget

import (
	"bytes"
	"encoding/json"
	"sort"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

// Split scans ordinary frames once and falls back before mutating state for unusual JSON.
func (a *Adapter) Split(sh venue.Shard, msg []byte) []rawlog.Record {
	if bytes.Equal(bytes.TrimSpace(msg), []byte("pong")) {
		return nil
	}
	f, ok := scanFrame(msg)
	if !ok {
		a.fallbacks.Add(1)
		return a.legacySplit(sh, msg)
	}
	code := string(f.code)
	if f.event == "error" || (len(f.err) > 0 && string(f.err) != "null" && string(f.err) != "false") || (code != "" && code != "null" && code != "0" && code != `"0"` && code != `"00000"`) {
		a.errors.Add(1)
		p, _ := json.Marshal(map[string]any{"event": "subscribe_error", "frame": string(msg[:min(len(msg), 4096)])})
		return []rawlog.Record{{Channel: "gap", Payload: p}}
	}
	if f.event == "subscribe" {
		a.acks.Add(1)
		return nil
	}
	if !f.data {
		return nil
	}
	channel := map[string]string{"publicTrade": "trade", "ticker": "ticker", "funding-rate": "funding", "position-tier": "tier", "liquidation": "liquidation"}[f.topic]
	if channel == "" {
		return gap(msg)
	}
	// Nested tier lists need the legacy recursive filtering semantics.
	if channel == "tier" {
		a.fallbacks.Add(1)
		return a.legacySplit(sh, msg)
	}
	for _, it := range f.items {
		if it.symbol == "" && f.symbol == "" {
			a.fallbacks.Add(1)
			return a.legacySplit(sh, msg)
		}
	}
	var out []rawlog.Record
	for _, it := range f.items {
		symbol := it.symbol
		if symbol == "" {
			symbol = f.symbol
		}
		if symbol == "" {
			a.fallbacks.Add(1)
			return a.legacySplit(sh, msg)
		}
		if channel == "liquidation" {
			a.mu.RLock()
			_, ok := a.universe[symbol]
			a.mu.RUnlock()
			if !ok {
				continue
			}
		}
		filtered := []byte{'{'}
		meaningful := false
		identityFields := make([]field, 0, len(it.fields))
		for _, v := range it.fields {
			if dropFields[channel][v.key] {
				continue
			}
			if !keepFields[channel][v.key] {
				a.unknown.Add(1)
			}
			if v.key != "symbol" && v.key != "ts" && v.key != "T" {
				meaningful = true
			}
			if len(filtered) > 1 {
				filtered = append(filtered, ',')
			}
			filtered = append(filtered, '"')
			filtered = append(filtered, v.key...)
			filtered = append(filtered, '"', ':')
			filtered = append(filtered, v.raw...)
			if v.key != "ts" && v.key != "T" {
				identityFields = append(identityFields, v)
			}
		}
		filtered = append(filtered, '}')
		if channel == "ticker" {
			if !meaningful {
				continue
			}
			sort.Slice(identityFields, func(i, j int) bool { return identityFields[i].key < identityFields[j].key })
			var identity bytes.Buffer
			identity.WriteByte('{')
			for i, v := range identityFields {
				if i > 0 {
					identity.WriteByte(',')
				}
				identity.WriteByte('"')
				identity.WriteString(v.key)
				identity.WriteString(`":`)
				var compact bytes.Buffer
				json.Compact(&compact, v.raw)
				json.HTMLEscape(&identity, compact.Bytes())
			}
			identity.WriteByte('}')
			now := a.now()
			key := sh.Name + "\x00" + channel + "\x00" + symbol
			a.tickerMu.Lock()
			if a.tickers == nil {
				a.tickers = make(map[string]lastTicker)
			}
			prev, ok := a.tickers[key]
			if ok && prev.payload == identity.String() && now.Sub(prev.emitted) < time.Minute {
				a.skipped.Add(1)
				a.tickerMu.Unlock()
				continue
			}
			a.tickers[key] = lastTicker{identity.String(), now}
			a.tickerMu.Unlock()
		}
		payload := []byte{'{'}
		for _, v := range f.fields {
			if v.key == "arg" {
				filteredArg, _, err := a.filterObject("arg", v.raw, false)
				if err != nil {
					return gap(msg)
				}
				if bytes.Equal(filteredArg, []byte("{}")) {
					continue
				}
				v.raw = filteredArg
			}
			if len(payload) > 1 {
				payload = append(payload, ',')
			}
			payload = append(payload, '"')
			payload = append(payload, v.key...)
			payload = append(payload, '"', ':')
			if v.key == "data" {
				if f.array {
					payload = append(payload, '[')
				}
				payload = append(payload, filtered...)
				if f.array {
					payload = append(payload, ']')
				}
			} else {
				payload = append(payload, v.raw...)
			}
		}
		payload = append(payload, '}')
		out = append(out, rawlog.Record{Channel: channel, Symbol: symbol, Payload: payload})
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
