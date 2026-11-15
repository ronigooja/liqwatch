package gate

import (
	"bytes"
	"strings"
	"unicode/utf8"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/venue"
)

// compactValue preserves string contents and removes JSON whitespace elsewhere.
func compactValue(dst, src []byte) []byte {
	quoted, escape := false, false
	for _, c := range src {
		if quoted {
			dst = append(dst, c)
			if escape {
				escape = false
			} else if c == '\\' {
				escape = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		if c == '"' {
			quoted = true
			dst = append(dst, c)
		} else if c != ' ' && c != '\n' && c != '\r' && c != '\t' {
			dst = append(dst, c)
		}
	}
	return dst
}

func (a *Adapter) scanSplit(sh venue.Shard, msg []byte) ([]rawlog.Record, bool) {
	s := jsonScanner{b: msg}
	root, ok := s.value(0, true)
	s.ws()
	if !ok || s.unsure || s.i != len(msg) || root.start >= len(msg) || msg[root.start] != '{' {
		return nil, false
	}
	// Avoid escaped or non-ASCII keys, case-insensitive struct matches, duplicate
	// envelope fields and unusual values; the reference handles these exactly.
	seen := map[string]bool{}
	var channel, event string
	var result *jsonNode
	for i := range root.children {
		n := &root.children[i]
		k := n.key
		if seen[strings.ToLower(k)] || !simpleKey(k) {
			return nil, false
		}
		seen[strings.ToLower(k)] = true
		switch strings.ToLower(k) {
		case "channel", "event":
			if k != strings.ToLower(k) || msg[n.start] != '"' {
				return nil, false
			}
			v := decodeString(msg[n.start:n.end])
			if k == "channel" {
				channel = v
			} else {
				event = v
			}
		case "result":
			if k != "result" {
				return nil, false
			}
			result = n
		case "error":
			return nil, false
		}
	}
	if event != "update" && event != "all" {
		return nil, false
	}
	ch := map[string]string{"futures.trades": "trades", "futures.tickers": "tickers", "futures.public_liquidates": "liquidations", "futures.contract_stats": "contract_stats", "futures.contract_info": "contract_info"}[channel]
	if ch == "" || result == nil {
		return nil, false
	}
	array := msg[result.start] == '['
	items := []jsonNode{*result}
	if array {
		items = result.children
	}
	// Validate every item before mutating counters or sampling.
	symbols := make([]string, len(items))
	for i, item := range items {
		if msg[item.start] != '{' {
			return nil, false
		}
		found := false
		for _, field := range item.children {
			if !simpleKey(field.key) {
				return nil, false
			}
			if strings.EqualFold(field.key, "contract") {
				if field.key != "contract" || found || msg[field.start] != '"' {
					return nil, false
				}
				found = true
				symbols[i] = decodeString(msg[field.start:field.end])
			}
		}
		if !found || symbols[i] == "" {
			return nil, false
		}
	}
	var out []rawlog.Record
	a.mu.RLock()
	defer a.mu.RUnlock()
	for i, item := range items {
		symbol := symbols[i]
		if _, ok := a.universe[symbol]; !ok {
			continue
		}
		filtered := []byte{'{'}
		for _, field := range item.children {
			k := field.key
			if policies[ch].drop[k] {
				continue
			}
			if !policies[ch].keep[k] {
				a.unknownFields.Add(1)
			}
			if len(filtered) > 1 {
				filtered = append(filtered, ',')
			}
			filtered = appendKey(filtered, k)
			filtered = append(filtered, ':')
			filtered = compactValue(filtered, msg[field.start:field.end])
		}
		filtered = append(filtered, '}')
		if array {
			filtered = append(append([]byte{'['}, filtered...), ']')
		}
		envelope := envelopeFor(ch)
		payload := []byte{'{'}
		for _, field := range root.children {
			k := field.key
			if envelope.drop[k] {
				continue
			}
			if !envelope.keep[k] {
				a.unknownFields.Add(1)
			}
			if len(payload) > 1 {
				payload = append(payload, ',')
			}
			payload = appendKey(payload, k)
			payload = append(payload, ':')
			if k == "result" {
				payload = append(payload, filtered...)
			} else {
				payload = compactValue(payload, msg[field.start:field.end])
			}
		}
		payload = append(payload, '}')
		out = append(out, rawlog.Record{Channel: ch, Symbol: symbol, Payload: payload})
	}
	if len(out) > 0 && sampleDue(sh.Name, ch) {
		payload := append(append([]byte(`{"sample":`), msg...), '}')
		out = append(out, rawlog.Record{Channel: ch, Symbol: out[0].Symbol, Payload: payload})
	}
	return out, true
}
func simpleKey(k string) bool {
	return utf8.ValidString(k) && !strings.ContainsAny(k, "\\\"<>&\u2028\u2029") && !bytes.ContainsAny([]byte(k), "\x00\n\r\t")
}
