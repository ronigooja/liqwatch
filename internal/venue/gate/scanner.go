package gate

import (
	"bytes"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

type jsonNode struct {
	start, end int
	key        string
	children   []jsonNode
}
type jsonScanner struct {
	b []byte
	i int
}

func (s *jsonScanner) ws() {
	for s.i < len(s.b) {
		switch s.b[s.i] {
		case ' ', '\n', '\r', '\t':
			s.i++
		default:
			return
		}
	}
}
func (s *jsonScanner) str() bool {
	if s.i >= len(s.b) || s.b[s.i] != '"' {
		return false
	}
	s.i++
	for s.i < len(s.b) {
		c := s.b[s.i]
		s.i++
		if c == '"' {
			return true
		}
		if c < 0x20 {
			return false
		}
		if c == '\\' {
			if s.i >= len(s.b) {
				return false
			}
			c = s.b[s.i]
			s.i++
			if c == 'u' {
				for j := 0; j < 4; j++ {
					if s.i >= len(s.b) || !isHex(s.b[s.i]) {
						return false
					}
					s.i++
				}
			} else if !strings.ContainsRune(`"\/bfnrt`, rune(c)) {
				return false
			}
		}
	}
	return false
}
func isHex(c byte) bool { return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }
func (s *jsonScanner) value(depth int, capture bool) (jsonNode, bool) {
	s.ws()
	n := jsonNode{start: s.i}
	if s.i >= len(s.b) || depth > 10000 {
		return n, false
	}
	c := s.b[s.i]
	switch c {
	case '"':
		if !s.str() {
			return n, false
		}
	case '{', '[':
		s.i++
		s.ws()
		end := byte('}')
		if c == '[' {
			end = ']'
		}
		if s.i < len(s.b) && s.b[s.i] == end {
			s.i++
			break
		}
		for {
			var key string
			if c == '{' {
				start := s.i
				if !s.str() {
					return n, false
				}
				if capture {
					key = decodeString(s.b[start:s.i])
				}
				s.ws()
				if s.i >= len(s.b) || s.b[s.i] != ':' {
					return n, false
				}
				s.i++
			}
			child, ok := s.value(depth+1, capture)
			if !ok {
				return n, false
			}
			if capture {
				child.key = key
				n.children = append(n.children, child)
			}
			s.ws()
			if s.i >= len(s.b) {
				return n, false
			}
			ch := s.b[s.i]
			s.i++
			if ch == end {
				break
			}
			if ch != ',' {
				return n, false
			}
			s.ws()
		}
	case 't', 'f', 'n':
		lit := "true"
		if c == 'f' {
			lit = "false"
		}
		if c == 'n' {
			lit = "null"
		}
		if !bytes.HasPrefix(s.b[s.i:], []byte(lit)) {
			return n, false
		}
		s.i += len(lit)
	default:
		if c == '-' {
			s.i++
			if s.i >= len(s.b) {
				return n, false
			}
		}
		if s.b[s.i] == '0' {
			s.i++
		} else if s.b[s.i] >= '1' && s.b[s.i] <= '9' {
			for s.i < len(s.b) && s.b[s.i] >= '0' && s.b[s.i] <= '9' {
				s.i++
			}
		} else {
			return n, false
		}
		if s.i < len(s.b) && s.b[s.i] == '.' {
			s.i++
			start := s.i
			for s.i < len(s.b) && s.b[s.i] >= '0' && s.b[s.i] <= '9' {
				s.i++
			}
			if s.i == start {
				return n, false
			}
		}
		if s.i < len(s.b) && (s.b[s.i] == 'e' || s.b[s.i] == 'E') {
			s.i++
			if s.i < len(s.b) && (s.b[s.i] == '+' || s.b[s.i] == '-') {
				s.i++
			}
			start := s.i
			for s.i < len(s.b) && s.b[s.i] >= '0' && s.b[s.i] <= '9' {
				s.i++
			}
			if s.i == start {
				return n, false
			}
		}
	}
	n.end = s.i
	return n, true
}
func decodeString(b []byte) string {
	b = b[1 : len(b)-1]
	if !bytes.ContainsRune(b, '\\') && utf8.Valid(b) {
		return string(b)
	}
	out := make([]byte, 0, len(b))
	for i := 0; i < len(b); {
		if b[i] == '\\' {
			i++
			c := b[i]
			i++
			switch c {
			case '"', '\\', '/':
				out = append(out, c)
			case 'b':
				out = append(out, '\b')
			case 'f':
				out = append(out, '\f')
			case 'n':
				out = append(out, '\n')
			case 'r':
				out = append(out, '\r')
			case 't':
				out = append(out, '\t')
			case 'u':
				v, _ := strconv.ParseUint(string(b[i:i+4]), 16, 16)
				i += 4
				r := rune(v)
				if utf16.IsSurrogate(r) {
					if i+6 <= len(b) && b[i] == '\\' && b[i+1] == 'u' {
						v2, e := strconv.ParseUint(string(b[i+2:i+6]), 16, 16)
						r2 := rune(v2)
						if e == nil && utf16.DecodeRune(r, r2) != utf8.RuneError {
							r = utf16.DecodeRune(r, r2)
							i += 6
						} else {
							r = utf8.RuneError
						}
					} else {
						r = utf8.RuneError
					}
				}
				out = utf8.AppendRune(out, r)
			}
			continue
		}
		r, n := utf8.DecodeRune(b[i:])
		out = utf8.AppendRune(out, r)
		i += n
	}
	return string(out)
}
func appendKey(out []byte, key string) []byte {
	out = append(out, '"')
	for _, r := range key {
		switch r {
		case '"', '\\':
			out = append(out, '\\', byte(r))
		case '\b':
			out = append(out, `\b`...)
		case '\f':
			out = append(out, `\f`...)
		case '\n':
			out = append(out, `\n`...)
		case '\r':
			out = append(out, `\r`...)
		case '\t':
			out = append(out, `\t`...)
		case '<', '>', '&', '\u2028', '\u2029':
			out = append(out, `\u`...)
			out = strconv.AppendInt(out, int64(r)>>12, 16)
			out = strconv.AppendInt(out, int64(r)>>8&15, 16)
			out = strconv.AppendInt(out, int64(r)>>4&15, 16)
			out = strconv.AppendInt(out, int64(r)&15, 16)
		default:
			if r < 0x20 {
				out = append(out, `\u00`...)
				out = strconv.AppendInt(out, int64(r)>>4, 16)
				out = strconv.AppendInt(out, int64(r)&15, 16)
			} else {
				out = utf8.AppendRune(out, r)
			}
		}
	}
	return append(out, '"')
}
