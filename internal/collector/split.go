package collector

import (
	"encoding/json"

	"github.com/ronigooja/liqwatch/internal/rawlog"
)

// SplitAllMarket splits a combined all-market message and filters its symbols.
func SplitAllMarket(message []byte, universe map[string]struct{}) []rawlog.Record {
	var envelope struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	if json.Unmarshal(message, &envelope) != nil {
		return nil
	}
	var elements []json.RawMessage
	var channel string
	switch envelope.Stream {
	case "!markPrice@arr@1s":
		channel = "markPrice_all"
		if json.Unmarshal(envelope.Data, &elements) != nil {
			return nil
		}
	case "!forceOrder@arr":
		channel = "forceOrder_all"
		elements = []json.RawMessage{envelope.Data}
	default:
		return nil
	}
	var out []rawlog.Record
	for _, element := range elements {
		var fields struct {
			Symbol string `json:"s"`
			Order  struct {
				Symbol string `json:"s"`
			} `json:"o"`
		}
		if json.Unmarshal(element, &fields) != nil {
			continue
		}
		symbol := fields.Symbol
		if channel == "forceOrder_all" {
			symbol = fields.Order.Symbol
		}
		if _, ok := universe[symbol]; !ok || symbol == "" {
			continue
		}
		payload, err := json.Marshal(struct {
			Stream string          `json:"stream"`
			Data   json.RawMessage `json:"data"`
		}{envelope.Stream, element})
		if err == nil {
			out = append(out, rawlog.Record{Channel: channel, Symbol: symbol, Payload: payload})
		}
	}
	return out
}
