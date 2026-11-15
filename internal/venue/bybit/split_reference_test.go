package bybit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
	"strings"
	"time"
)

// orderedObject preserves original value bytes and field order.
func legacyOrderedObject(b []byte, f func(string, json.RawMessage) (json.RawMessage, bool)) ([]byte, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	tok, err := d.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, fmt.Errorf("expected object")
	}
	var out bytes.Buffer
	out.WriteByte('{')
	first := true
	for d.More() {
		t, err := d.Token()
		if err != nil {
			return nil, err
		}
		key, ok := t.(string)
		if !ok {
			return nil, fmt.Errorf("invalid key")
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, err
		}
		value, ok = f(key, value)
		if !ok {
			continue
		}
		if !first {
			out.WriteByte(',')
		}
		first = false
		k, _ := json.Marshal(key)
		out.Write(k)
		out.WriteByte(':')
		out.Write(value)
	}
	if _, err := d.Token(); err != nil {
		return nil, err
	}
	out.WriteByte('}')
	return out.Bytes(), nil
}

func (a *Adapter) legacySplit(sh venue.Shard, msg []byte) []rawlog.Record {
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
						filtered, e := legacyOrderedObject(item, func(k string, v json.RawMessage) (json.RawMessage, bool) {
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
						payload, e := legacyOrderedObject(msg, func(k string, v json.RawMessage) (json.RawMessage, bool) {
							if k == "topic" || k == "type" {
								return nil, false
							}
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
