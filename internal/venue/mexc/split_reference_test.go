package mexc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
	"strings"
)

// filterObject preserves source order and raw JSON values while removing known drops.
func (a *Adapter) referenceFilterObject(ch string, data []byte) ([]byte, error) {
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

func (a *Adapter) referenceSplit(sh venue.Shard, msg []byte) []rawlog.Record {
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
		if v.Channel != "push.contract" {
			return nil
		}
		ch := "contract"
		payload, e := a.filterFrame(ch, msg, v.Data)
		if e != nil {
			return a.invalidFrame(msg)
		}
		return sampled(sh, ch, msg, []rawlog.Record{{Channel: ch, Symbol: v.Symbol, Payload: payload}})
	}
	channels := map[string]string{"push.deal": "trade", "push.fair.price": "mark", "push.index.price": "index", "push.ticker": "ticker", "push.funding.rate": "funding"}
	ch, known := channels[v.Channel]
	if err == nil && known && len(v.Data) > 0 && string(v.Data) != "null" && v.Symbol != "" {
		payload, e := a.filterFrame(ch, msg, v.Data)
		if e != nil {
			goto gap
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
