package okx

import (
	"bytes"
	"encoding/json"
	"unicode/utf8"
)

// frameScanner accepts a deliberately narrow subset of JSON. Anything outside
// that subset goes through encoding/json, including malformed input. It validates
// the complete frame before Split makes any observable changes.
type frameScanner struct {
	b      []byte
	i      int
	unsure bool
}
type scannedItem struct {
	raw    []byte
	symbol string
	fields []scannedField
}
type scannedField struct {
	key           string
	rawKey, value []byte
}
type scannedFrame struct {
	channel, symbol string
	arg             []byte
	items           []scannedItem
}

func filterScannedItem(channel string, item scannedItem) []byte {
	out := make([]byte, 0, len(item.raw))
	out = append(out, '{')
	rule := fieldRules[channel]
	for _, field := range item.fields {
		if rule.drop[field.key] {
			continue
		}
		if !rule.keep[field.key] {
			unknownFields.Add(1)
		}
		if len(out) > 1 {
			out = append(out, ',')
		}
		if bytes.IndexAny(field.rawKey, "<>&\u0080") >= 0 || !isASCII(field.rawKey) {
			name, _ := json.Marshal(field.key)
			out = append(out, name...)
		} else {
			out = append(out, field.rawKey...)
		}
		out = append(out, ':')
		value := field.value
		if channel == "liquidation-orders" && field.key == "details" {
			value = filterDetails(value)
		}
		out = append(out, value...)
	}
	return append(out, '}')
}

func isASCII(b []byte) bool {
	for _, c := range b {
		if c >= 0x80 {
			return false
		}
	}
	return true
}

func (s *frameScanner) space() {
	for s.i < len(s.b) {
		switch s.b[s.i] {
		case ' ', '\t', '\n', '\r':
			s.i++
		default:
			return
		}
	}
}
func (s *frameScanner) take(c byte) bool {
	s.space()
	if s.i >= len(s.b) || s.b[s.i] != c {
		s.unsure = true
		return false
	}
	s.i++
	return true
}
func (s *frameScanner) str() string {
	s.space()
	if s.i >= len(s.b) || s.b[s.i] != '"' {
		s.unsure = true
		return ""
	}
	s.i++
	start := s.i
	for s.i < len(s.b) {
		c := s.b[s.i]
		if c == '"' {
			v := s.b[start:s.i]
			s.i++
			if !utf8.Valid(v) {
				s.unsure = true
			}
			return string(v)
		}
		if c < 0x20 || c == '\\' {
			s.unsure = true
			return ""
		}
		s.i++
	}
	s.unsure = true
	return ""
}
func (s *frameScanner) value(depth int) {
	if depth > 128 {
		s.unsure = true
		return
	}
	s.space()
	if s.i >= len(s.b) {
		s.unsure = true
		return
	}
	switch s.b[s.i] {
	case '"':
		s.str()
	case '{':
		s.i++
		s.space()
		if s.i < len(s.b) && s.b[s.i] == '}' {
			s.i++
			return
		}
		for !s.unsure {
			s.str()
			if !s.take(':') {
				return
			}
			s.value(depth + 1)
			s.space()
			if s.i < len(s.b) && s.b[s.i] == '}' {
				s.i++
				return
			}
			if !s.take(',') {
				return
			}
		}
	case '[':
		s.i++
		s.space()
		if s.i < len(s.b) && s.b[s.i] == ']' {
			s.i++
			return
		}
		for !s.unsure {
			s.value(depth + 1)
			s.space()
			if s.i < len(s.b) && s.b[s.i] == ']' {
				s.i++
				return
			}
			if !s.take(',') {
				return
			}
		}
	case 't', 'f', 'n':
		literal := "true"
		if s.b[s.i] == 'f' {
			literal = "false"
		}
		if s.b[s.i] == 'n' {
			literal = "null"
		}
		if !bytes.HasPrefix(s.b[s.i:], []byte(literal)) {
			s.unsure = true
			return
		}
		s.i += len(literal)
	default:
		start := s.i
		if s.b[s.i] == '-' {
			s.i++
		}
		if s.i >= len(s.b) {
			s.unsure = true
			return
		}
		if s.b[s.i] == '0' {
			s.i++
		} else if s.b[s.i] >= '1' && s.b[s.i] <= '9' {
			for s.i < len(s.b) && s.b[s.i] >= '0' && s.b[s.i] <= '9' {
				s.i++
			}
		} else {
			s.unsure = true
			return
		}
		if s.i < len(s.b) && s.b[s.i] == '.' {
			s.i++
			n := s.i
			for s.i < len(s.b) && s.b[s.i] >= '0' && s.b[s.i] <= '9' {
				s.i++
			}
			if n == s.i {
				s.unsure = true
				return
			}
		}
		if s.i < len(s.b) && (s.b[s.i] == 'e' || s.b[s.i] == 'E') {
			s.i++
			if s.i < len(s.b) && (s.b[s.i] == '+' || s.b[s.i] == '-') {
				s.i++
			}
			n := s.i
			for s.i < len(s.b) && s.b[s.i] >= '0' && s.b[s.i] <= '9' {
				s.i++
			}
			if n == s.i {
				s.unsure = true
				return
			}
		}
		if s.i == start {
			s.unsure = true
		}
	}
}
func (s *frameScanner) object(field func(string)) {
	if !s.take('{') {
		return
	}
	s.space()
	if s.i < len(s.b) && s.b[s.i] == '}' {
		s.i++
		return
	}
	for !s.unsure {
		key := s.str()
		if !s.take(':') {
			return
		}
		field(key)
		s.space()
		if s.i < len(s.b) && s.b[s.i] == '}' {
			s.i++
			return
		}
		if !s.take(',') {
			return
		}
	}
}
func scanFrame(b []byte) (f scannedFrame, ok bool) {
	s := frameScanner{b: b}
	var seenArg, seenData bool
	s.object(func(key string) {
		switch key {
		case "arg":
			if seenArg {
				s.unsure = true
				return
			}
			seenArg = true
			s.space()
			start := s.i
			var seenChannel, seenID, seenType bool
			s.object(func(k string) {
				switch k {
				case "channel":
					if seenChannel {
						s.unsure = true
						return
					}
					seenChannel = true
					f.channel = s.str()
				case "instId":
					if seenID {
						s.unsure = true
						return
					}
					seenID = true
					f.symbol = s.str()
				case "instType":
					if seenType {
						s.unsure = true
						return
					}
					seenType = true
					s.str()
				default:
					s.unsure = true
				}
			})
			if !s.unsure {
				f.arg = b[start:s.i]
			}
		case "data":
			if seenData {
				s.unsure = true
				return
			}
			seenData = true
			if !s.take('[') {
				return
			}
			s.space()
			if s.i < len(b) && b[s.i] == ']' {
				s.i++
				return
			}
			for !s.unsure {
				s.space()
				start := s.i
				item := scannedItem{}
				seenID := false
				if !s.take('{') {
					return
				}
				s.space()
				for !s.unsure && s.i < len(b) && b[s.i] != '}' {
					keyStart := s.i
					k := s.str()
					keyEnd := s.i
					if !s.take(':') {
						return
					}
					s.space()
					valueStart := s.i
					if k == "instId" {
						if seenID {
							s.unsure = true
							return
						}
						seenID = true
						item.symbol = s.str()
					} else { // Case variants can target the tagged field in encoding/json.
						if bytes.EqualFold([]byte(k), []byte("instId")) {
							s.unsure = true
							return
						}
						s.value(1)
					}
					if s.unsure {
						return
					}
					item.fields = append(item.fields, scannedField{k, b[keyStart:keyEnd], b[valueStart:s.i]})
					s.space()
					if s.i < len(b) && b[s.i] == '}' {
						break
					}
					if !s.take(',') {
						return
					}
					s.space()
					if s.i < len(b) && b[s.i] == '}' {
						s.unsure = true
						return
					}
				}
				if !s.take('}') {
					return
				}
				if s.unsure {
					return
				}
				item.raw = b[start:s.i]
				f.items = append(f.items, item)
				s.space()
				if s.i < len(b) && b[s.i] == ']' {
					s.i++
					return
				}
				if !s.take(',') {
					return
				}
			}
		default:
			s.unsure = true
		}
	})
	s.space()
	return f, !s.unsure && s.i == len(b) && seenArg && seenData
}
