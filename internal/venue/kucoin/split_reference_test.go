package kucoin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
	"strings"
)

func referenceFilterObject(src []byte, keep, drop map[string]bool, utaOuter bool) ([]byte, error) {
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
		if !keep[key] && !(utaOuter && key == "P") {
			unknownFields.Add(1)
		}
		if key == "data" || key == "d" {
			value, err = referenceFilterObject(value, keep, drop, false)
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

func referenceFiltered(sh venue.Shard, channel, symbol string, msg []byte, keep, drop map[string]bool) []rawlog.Record {
	p, err := referenceFilterObject(msg, keep, drop, channel == "utamark")
	if err != nil {
		return referenceGap(msg)
	}
	out := []rawlog.Record{{Channel: channel, Symbol: symbol, Payload: p}}
	if p := sample(sh.Name, channel, msg); p != nil {
		out = append(out, rawlog.Record{Channel: channel, Symbol: symbol, Payload: p})
	}
	return out
}

func (a *Adapter) referenceSplit(sh venue.Shard, msg []byte) []rawlog.Record {
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
		return referenceGap(msg)
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
		return referenceFiltered(sh, "utamark", f.D.Symbol, msg, utaKeep, utaDrop)
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
				switch f.Subject {
				case "mark.index.price":
					ch = "mark"
				case "funding.rate":
					ch = "funding"
				}
			}
			if ch != "" {
				if ch == "trade" {
					return referenceFiltered(sh, ch, parts[1], msg, tradeKeep, tradeDrop)
				}
				return referenceFiltered(sh, ch, parts[1], msg, markKeep, markDrop)
			}
		}
		return referenceGap(msg)
	}
	if f.T != "" || len(f.Data) > 0 {
		return referenceGap(msg)
	}
	return nil
}
func referenceGap(msg []byte) []rawlog.Record {
	p, _ := json.Marshal(map[string]any{"event": "unrecognized_frame", "bytes": len(msg), "head": string(msg[:min(len(msg), 200)])})
	return []rawlog.Record{{Channel: "gap", Payload: p}}
}
