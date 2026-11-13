package bitget

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

// legacyFilter is the pre-optimization implementation, retained as a byte-level oracle.
func legacyFilter(a *Adapter, channel string, data []byte, noTime bool) ([]byte, bool, bool) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return nil, false, false
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	if _, err := dec.Token(); err != nil {
		return nil, false, false
	}
	var out bytes.Buffer
	out.WriteByte('{')
	meaningful := false
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, false, false
		}
		key := tok.(string)
		var value json.RawMessage
		if dec.Decode(&value) != nil {
			return nil, false, false
		}
		if dropFields[channel][key] || (noTime && (key == "ts" || key == "T")) {
			continue
		}
		if !keepFields[channel][key] {
			a.unknown.Add(1)
		}
		if channel == "tier" && key == "list" {
			var items []json.RawMessage
			if json.Unmarshal(value, &items) == nil {
				for i, item := range items {
					if v, _, ok := legacyFilter(a, channel, item, false); ok {
						items[i] = v
					}
				}
				value, _ = json.Marshal(items)
			}
		}
		if key != "symbol" && key != "ts" && key != "T" {
			meaningful = true
		}
		if out.Len() > 1 {
			out.WriteByte(',')
		}
		k, _ := json.Marshal(key)
		out.Write(k)
		out.WriteByte(':')
		out.Write(value)
	}
	out.WriteByte('}')
	return out.Bytes(), meaningful, true
}

func TestFilterDifferentialFixtures(t *testing.T) {
	for _, file := range []string{"testdata/contracts.jsonl", "testdata/frames.jsonl"} {
		f, err := os.Open(file)
		if err != nil {
			t.Fatal(err)
		}
		s := bufio.NewScanner(f)
		s.Buffer(make([]byte, 4096), 16<<20)
		for s.Scan() {
			var walk func(json.RawMessage)
			walk = func(raw json.RawMessage) {
				for _, channel := range []string{"trade", "ticker", "funding", "tier", "liquidation"} {
					for _, noTime := range []bool{false, true} {
						a, b := &Adapter{}, &Adapter{}
						want, wm, wok := legacyFilter(a, channel, raw, noTime)
						got, gm, err := b.filterObject(channel, raw, noTime)
						if wok != (err == nil) || wm != gm || !bytes.Equal(want, got) || a.UnknownFields() != b.UnknownFields() {
							t.Fatalf("%s %s %s: got %s (%v), want %s", file, channel, raw, got, err, want)
						}
					}
				}
				var obj map[string]json.RawMessage
				if json.Unmarshal(raw, &obj) == nil && obj != nil {
					for _, v := range obj {
						walk(v)
					}
					return
				}
				var arr []json.RawMessage
				if json.Unmarshal(raw, &arr) == nil {
					for _, v := range arr {
						walk(v)
					}
				}
			}
			walk(append([]byte(nil), s.Bytes()...))
		}
		if err := s.Err(); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
}
