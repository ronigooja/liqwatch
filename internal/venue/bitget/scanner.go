package bitget

import (
	"bytes"
	"unicode/utf8"
)

type frameScanner struct {
	b      []byte
	i      int
	unsure bool
}
type field struct {
	key string
	raw []byte
}
type item struct {
	symbol string
	fields []field
}
type frame struct {
	event, topic, symbol string
	code, err            []byte
	fields               []field
	items                []item
	array                bool
	data                 bool
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
		for _, c := range key {
			if c >= 127 || c == '<' || c == '>' || c == '&' {
				s.unsure = true
				return
			}
		}
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

func (s *frameScanner) item() (it item) {
	seen := map[string]bool{}
	s.object(func(k string) {
		if seen[k] || (k != "symbol" && bytes.EqualFold([]byte(k), []byte("symbol"))) {
			s.unsure = true
			return
		}
		seen[k] = true
		s.space()
		start := s.i
		if k == "symbol" {
			it.symbol = s.str()
		} else {
			s.value(1)
		}
		if !s.unsure {
			it.fields = append(it.fields, field{k, s.b[start:s.i]})
		}
	})
	return
}
func scanFrame(b []byte) (f frame, ok bool) {
	s := frameScanner{b: b}
	seen := map[string]bool{}
	s.object(func(k string) {
		if seen[k] {
			s.unsure = true
			return
		}
		seen[k] = true
		for _, special := range []string{"event", "error", "code", "arg", "data"} {
			if k != special && bytes.EqualFold([]byte(k), []byte(special)) {
				s.unsure = true
				return
			}
		}
		s.space()
		start := s.i
		switch k {
		case "event":
			f.event = s.str()
		case "arg":
			seenArg := map[string]bool{}
			s.object(func(k string) {
				if seenArg[k] {
					s.unsure = true
					return
				}
				seenArg[k] = true
				switch k {
				case "topic":
					f.topic = s.str()
				case "symbol":
					f.symbol = s.str()
				default:
					if bytes.EqualFold([]byte(k), []byte("topic")) || bytes.EqualFold([]byte(k), []byte("symbol")) {
						s.unsure = true
						return
					}
					s.value(1)
				}
			})
		case "data":
			f.data = true
			if s.i < len(b) && b[s.i] == '[' {
				f.array = true
				s.i++
				s.space()
				if s.i < len(b) && b[s.i] == ']' {
					s.i++
				} else {
					for !s.unsure {
						f.items = append(f.items, s.item())
						s.space()
						if s.i < len(b) && b[s.i] == ']' {
							s.i++
							break
						}
						if !s.take(',') {
							break
						}
					}
				}
			} else {
				f.items = append(f.items, s.item())
			}
		default:
			s.value(1)
		}
		if !s.unsure {
			raw := b[start:s.i]
			f.fields = append(f.fields, field{k, raw})
			if k == "code" {
				f.code = raw
			}
			if k == "error" {
				f.err = raw
			}
		}
	})
	s.space()
	return f, !s.unsure && s.i == len(b)
}
