package mexc

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

type scanField struct {
	key           string
	rawKey, value []byte
}
type scannedFrame struct {
	channel, symbol string
	data            []byte
	fields          []scanField
	tradeFields     [][]scanField
}

func (s *frameScanner) fields() []scanField {
	var fields []scanField
	s.object(func(k string) {
		// encoding/json escapes HTML characters and invalid Unicode in object keys.
		if strings.ContainsAny(k, "<>&") || !isASCII(k) {
			s.unsure = true
			return
		}
		end := s.i
		// object consumed colon; find the original unescaped key immediately before it.
		j := end - 1
		for j >= 0 && (s.b[j] == ':' || s.b[j] == ' ' || s.b[j] == '\n' || s.b[j] == '\r' || s.b[j] == '\t') {
			j--
		}
		keyEnd := j + 1
		keyStart := keyEnd - len(k) - 2
		if keyStart < 0 {
			s.unsure = true
			return
		}
		s.space()
		start := s.i
		s.value(1)
		if !s.unsure {
			fields = append(fields, scanField{k, s.b[keyStart:keyEnd], s.b[start:s.i]})
		}
	})
	return fields
}
func isASCII(b string) bool {
	for i := 0; i < len(b); i++ {
		if b[i] >= 128 {
			return false
		}
	}
	return true
}
func scanFrame(b []byte) (f scannedFrame, ok bool) {
	s := frameScanner{b: b}
	seen := map[string]bool{}
	s.object(func(k string) {
		// Any case variant of a tagged key uses encoding/json's case folding.
		for _, x := range []string{"channel", "symbol", "data"} {
			if strings.EqualFold(k, x) && k != x {
				s.unsure = true
				return
			}
		}
		if seen[k] {
			s.unsure = true
			return
		}
		seen[k] = true
		switch k {
		case "channel":
			f.channel = s.str()
		case "symbol":
			f.symbol = s.str()
		case "data":
			s.space()
			start := s.i
			if s.i >= len(b) {
				s.unsure = true
				return
			}
			switch b[s.i] {
			case '{':
				f.fields = s.fields()
			case '[':
				s.i++
				s.space()
				if s.i < len(b) && b[s.i] == ']' {
					s.i++
					break
				}
				for !s.unsure {
					s.space()
					if s.i >= len(b) || b[s.i] != '{' {
						s.unsure = true
						return
					}
					f.tradeFields = append(f.tradeFields, s.fields())
					s.space()
					if s.i < len(b) && b[s.i] == ']' {
						s.i++
						break
					}
					if !s.take(',') {
						return
					}
				}
			default:
				s.value(1)
			}
			if !s.unsure {
				f.data = b[start:s.i]
			}
		default:
			s.value(1)
		}
	})
	s.space()
	return f, !s.unsure && s.i == len(b)
}
