package bitget

import (
	"bytes"
	"encoding/json"
	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
	"time"
)

func (a *Adapter) referenceSplit(sh venue.Shard, msg []byte) []rawlog.Record {
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
			if key == "arg" {
				value, _, err = a.filterObject("arg", value, false)
				if err != nil {
					return gap(msg)
				}
				if bytes.Equal(value, []byte("{}")) {
					continue
				}
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
