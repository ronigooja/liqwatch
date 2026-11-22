package binance

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/ronigooja/liqwatch/internal/rawlog"
)

// Unspecified data fields are retained and counted.
var keepFields = map[string]map[string]bool{
	"aggTrade":      {"E": true, "T": true, "a": true, "f": true, "l": true, "p": true, "q": true, "m": true},
	"markPrice_all": {"E": true, "p": true, "i": true, "r": true, "T": true},
}
var dropFields = map[string]map[string]bool{
	"aggTrade":      {"e": true, "s": true, "nq": true, "st": true},
	"markPrice_all": {"e": true, "s": true, "st": true, "ap": true, "P": true},
}

func (a *Adapter) filteredPayload(channel string, data []byte) ([]byte, error) {
	var object map[string]json.RawMessage
	if json.Unmarshal(data, &object) != nil || object == nil {
		return nil, fmt.Errorf("invalid data object")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if _, err := d.Token(); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.WriteString(`{"data":{`)
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
		if dropFields[channel][name] {
			continue
		}
		if !keepFields[channel][name] {
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
	out.WriteString(`}}`)
	return out.Bytes(), nil
}

// SplitAllMarket splits a combined all-market message and filters its symbols.
func SplitAllMarket(message []byte, universe map[string]struct{}) []rawlog.Record {
	return (&Adapter{}).splitAllMarket(message, universe)
}

func (a *Adapter) splitAllMarket(message []byte, universe map[string]struct{}) []rawlog.Record {
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
		var symbol string
		if channel == "forceOrder_all" {
			// Exact lookup: encoding/json's case-insensitive struct matching lets
			// the side field "S" overwrite the symbol field "s".
			var fields map[string]json.RawMessage
			var order map[string]json.RawMessage
			if json.Unmarshal(element, &fields) != nil || json.Unmarshal(fields["o"], &order) != nil || json.Unmarshal(order["s"], &symbol) != nil {
				continue
			}
		} else {
			var fields struct {
				Symbol string `json:"s"`
			}
			if json.Unmarshal(element, &fields) != nil {
				continue
			}
			symbol = fields.Symbol
		}
		if _, ok := universe[symbol]; !ok || symbol == "" {
			continue
		}
		var payload []byte
		var err error
		if channel == "markPrice_all" {
			payload, err = a.filteredPayload(channel, element)
		} else {
			payload, err = json.Marshal(struct {
				Stream string          `json:"stream"`
				Data   json.RawMessage `json:"data"`
			}{envelope.Stream, element})
		}
		if err == nil {
			out = append(out, rawlog.Record{Channel: channel, Symbol: symbol, Payload: payload})
		}
	}
	return out
}
