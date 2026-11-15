package kucoin

import (
	"bytes"
	"strings"
	"unicode/utf8"
)

type frameScanner struct {
	b      []byte
	i      int
	unsure bool
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

type scanField struct {
	key           string
	rawKey, value []byte
	child         []scanField
}
type scanFrame struct {
	fields                                           []scanField
	typ, op, topic, subject, t, symbol, code, result string
	hasData                                          bool
}

func (s *frameScanner) fields(depth int) []scanField {
	if depth > 128 || !s.take('{') {
		s.unsure = true
		return nil
	}
	var fields []scanField
	s.space()
	if s.i < len(s.b) && s.b[s.i] == '}' {
		s.i++
		return fields
	}
	for !s.unsure {
		s.space()
		start := s.i
		key := s.str()
		end := s.i
		if !s.take(':') {
			return nil
		}
		s.space()
		vstart := s.i
		var child []scanField
		if (key == "data" || key == "d") && s.i < len(s.b) && s.b[s.i] == '{' {
			child = s.fields(depth + 1)
		} else {
			s.value(depth + 1)
		}
		if s.unsure {
			return nil
		}
		fields = append(fields, scanField{key: key, rawKey: s.b[start:end], value: s.b[vstart:s.i], child: child})
		s.space()
		if s.i < len(s.b) && s.b[s.i] == '}' {
			s.i++
			return fields
		}
		if !s.take(',') {
			return nil
		}
	}
	return nil
}
func scan(msg []byte) (scanFrame, bool) {
	s := frameScanner{b: msg}
	f := scanFrame{}
	f.fields = s.fields(0)
	s.space()
	if s.unsure || s.i != len(msg) {
		return f, false
	}
	seen := map[string]bool{}
	for _, x := range f.fields { // encoding/json matches struct tags case-insensitively.
		low := strings.ToLower(x.key)
		switch low {
		case "type", "op", "topic", "subject", "t", "d", "code", "result", "data":
			if seen[low] || (x.key != low && x.key != "T") {
				return f, false
			}
			seen[low] = true
		}
		switch x.key {
		case "type", "op", "topic", "subject", "T":
			if len(x.value) < 2 || x.value[0] != '"' {
				return f, false
			}
			v := string(x.value[1 : len(x.value)-1])
			switch x.key {
			case "type":
				f.typ = v
			case "op":
				f.op = v
			case "topic":
				f.topic = v
			case "subject":
				f.subject = v
			case "T":
				f.t = v
			}
		case "code", "result":
			return f, false
		case "data":
			f.hasData = true
		case "d":
			if len(x.value) == 0 || x.value[0] != '{' {
				return f, false
			}
			var found bool
			for _, v := range x.child {
				if strings.EqualFold(v.key, "s") {
					if found || v.key != "s" || len(v.value) < 2 || v.value[0] != '"' {
						return f, false
					}
					found = true
					f.symbol = string(v.value[1 : len(v.value)-1])
				}
			}
		}
	}
	return f, true
}
func scanFilter(fields []scanField, keep, drop, outerKeep map[string]bool) ([]byte, uint64, bool) {
	out := []byte{'{'}
	var unknown uint64
	for _, f := range fields {
		if drop[f.key] {
			continue
		}
		if !keep[f.key] && !outerKeep[f.key] {
			unknown++
		}
		value := f.value
		if f.key == "data" || f.key == "d" {
			if len(value) == 0 || value[0] != '{' {
				return nil, 0, false
			}
			var n uint64
			var ok bool
			value, n, ok = scanFilter(f.child, keep, drop, nil)
			if !ok {
				return nil, 0, false
			}
			unknown += n
		}
		if len(out) > 1 {
			out = append(out, ',')
		}
		out = append(out, f.rawKey...)
		out = append(out, ':')
		out = append(out, value...)
	}
	return append(out, '}'), unknown, true
}
