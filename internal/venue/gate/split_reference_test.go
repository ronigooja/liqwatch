package gate

import (
	"encoding/json"
	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

func (a *Adapter) splitReference(sh venue.Shard, msg []byte) []rawlog.Record {
	var env struct {
		Channel string          `json:"channel"`
		Event   string          `json:"event"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if json.Unmarshal(msg, &env) != nil {
		return gap(msg)
	}
	if env.Event == "error" || (len(env.Error) > 0 && string(env.Error) != "null") {
		a.subscribeErrors.Add(1)
		var frame any = json.RawMessage(msg)
		if len(msg) > 4096 {
			frame = string(msg[:4096])
		}
		payload, _ := json.Marshal(map[string]any{"event": "subscribe_error", "frame": frame})
		return []rawlog.Record{{Channel: "gap", Payload: payload}}
	}
	if env.Event == "subscribe" {
		var result struct {
			Status string `json:"status"`
		}
		if json.Unmarshal(env.Result, &result) == nil && result.Status == "success" {
			a.subscribeAcks.Add(1)
		}
		return nil
	}
	if env.Event == "unsubscribe" || env.Channel == "futures.pong" {
		return nil
	}
	if env.Event != "update" && env.Event != "all" {
		if len(env.Result) > 0 && string(env.Result) != "null" {
			return gap(msg)
		}
		return nil
	}
	channels := map[string]string{"futures.trades": "trades", "futures.tickers": "tickers", "futures.public_liquidates": "liquidations", "futures.contract_stats": "contract_stats", "futures.contract_info": "contract_info"}
	ch, ok := channels[env.Channel]
	if !ok {
		return gap(msg)
	}
	var items []json.RawMessage
	array := len(env.Result) > 0 && env.Result[0] == '['
	if array {
		if json.Unmarshal(env.Result, &items) != nil {
			return gap(msg)
		}
	} else {
		items = []json.RawMessage{env.Result}
	}
	var out []rawlog.Record
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, item := range items {
		var v struct {
			Contract string `json:"contract"`
		}
		if json.Unmarshal(item, &v) != nil || v.Contract == "" {
			out = append(out, gap(msg)...)
			continue
		}
		if _, ok := a.universe[v.Contract]; !ok {
			continue
		}
		filtered, err := a.filterObject(item, policies[ch], nil)
		if err != nil {
			out = append(out, gap(msg)...)
			continue
		}
		if array {
			filtered = append(append([]byte{'['}, filtered...), ']')
		}
		payload, err := a.filterObject(msg, envelopePolicy, filtered)
		if err != nil {
			return gap(msg)
		}
		out = append(out, rawlog.Record{Channel: ch, Symbol: v.Contract, Payload: payload})
	}
	if len(out) > 0 && out[0].Channel == ch && sampleDue(sh.Name, ch) {
		payload := make([]byte, 0, len(msg)+11)
		payload = append(payload, `{"sample":`...)
		payload = append(payload, msg...)
		payload = append(payload, '}')
		out = append(out, rawlog.Record{Channel: ch, Symbol: out[0].Symbol, Payload: payload})
	}
	return out
}
