package poller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Policy classifies members of a JSON object or positional array. Unclassified
// members are retained and counted. Nested paths are rooted at the payload:
// append "." and the field name for children (omit the dot at the root),
// and append [] for array members. [] denotes root array object members;
// for example, [].tiers[] denotes members of their tiers arrays.
// TimestampPath locates the object or array containing the history cursor;
// empty means the payload itself. TimestampKey or TimestampIndex identifies
// the member. A nonempty keep list at that level must include it: otherwise
// it would be retained but counted as unknown. DropChannel suppresses scheduled
// requests and output records; existing cursor state is preserved.
type Policy struct {
	Keep, Drop               []string
	KeepIndexes, DropIndexes []int
	Nested                   map[string]Policy
	DropChannel              bool
	TimestampKey             string
	TimestampIndex           *int
	TimestampPath            string
}

type policyKey struct{ venue, channel string }

var policies = struct {
	sync.RWMutex
	m map[policyKey]Policy
}{m: make(map[policyKey]Policy)}

// policiesDisabled is only changed by non-parallel tests.
var policiesDisabled bool

var unknownFields sync.Map // policyKey -> *atomic.Uint64

// RegisterPolicy installs a policy. Register at initialization, before polling;
// do not mutate its slices or maps after registration.
func RegisterPolicy(venue, channel string, p Policy) error {
	if venue == "" || channel == "" {
		return fmt.Errorf("policy: empty venue or channel")
	}
	var validate func(Policy) error
	validate = func(p Policy) error {
		for _, k := range p.Drop {
			if k == p.TimestampKey && k != "" {
				return fmt.Errorf("policy: timestamp key %q dropped", k)
			}
			if hasString(p.Keep, k) {
				return fmt.Errorf("policy: key %q both kept and dropped", k)
			}
		}
		for _, i := range p.DropIndexes {
			if i < 0 {
				return fmt.Errorf("policy: negative index")
			}
			if p.TimestampIndex != nil && i == *p.TimestampIndex {
				return fmt.Errorf("policy: timestamp index %d dropped", i)
			}
			if hasIndex(p.KeepIndexes, i) {
				return fmt.Errorf("policy: index %d both kept and dropped", i)
			}
		}
		for _, i := range p.KeepIndexes {
			if i < 0 {
				return fmt.Errorf("policy: negative index")
			}
		}
		if p.TimestampIndex != nil && *p.TimestampIndex < 0 {
			return fmt.Errorf("policy: negative timestamp index")
		}
		for _, n := range p.Nested {
			if err := validate(n); err != nil {
				return err
			}
		}
		return nil
	}
	if err := validate(p); err != nil {
		return err
	}
	if _, ok := p.Nested["[]"]; ok && (len(p.KeepIndexes) > 0 || len(p.DropIndexes) > 0) {
		return fmt.Errorf("policy: root array members conflict with positional indexes")
	}
	if p.TimestampKey != "" && p.TimestampIndex != nil {
		return fmt.Errorf("policy: timestamp cannot be both key and index")
	}
	if p.TimestampPath != "" && p.TimestampKey == "" && p.TimestampIndex == nil {
		return fmt.Errorf("policy: timestamp path without timestamp")
	}
	if p.TimestampKey != "" || p.TimestampIndex != nil {
		level := p
		path := ""
		if p.TimestampPath != "" {
			for _, part := range strings.Split(p.TimestampPath, ".") {
				key := strings.TrimSuffix(part, "[]")
				if (key == "" && !(path == "" && part == "[]")) || (key != "" && hasString(level.Drop, key)) {
					return fmt.Errorf("policy: timestamp ancestor %q dropped or invalid", key)
				}
				if path != "" {
					path += "."
				}
				path += part
				var ok bool
				level, ok = p.Nested[path]
				if !ok {
					return fmt.Errorf("policy: timestamp path %q missing policy", path)
				}
			}
		}
		if p.TimestampKey != "" && (hasString(level.Drop, p.TimestampKey) || len(level.Keep) > 0 && !hasString(level.Keep, p.TimestampKey)) {
			return fmt.Errorf("policy: timestamp key %q not kept at %q", p.TimestampKey, path)
		}
		if p.TimestampIndex != nil && (hasIndex(level.DropIndexes, *p.TimestampIndex) || len(level.KeepIndexes) > 0 && !hasIndex(level.KeepIndexes, *p.TimestampIndex)) {
			return fmt.Errorf("policy: timestamp index %d not kept at %q", *p.TimestampIndex, path)
		}
	}
	policies.Lock()
	defer policies.Unlock()
	k := policyKey{venue, channel}
	if _, ok := policies.m[k]; ok {
		return fmt.Errorf("policy: duplicate %s/%s", venue, channel)
	}
	policies.m[k] = p
	return nil
}
func hasString(a []string, s string) bool {
	for _, v := range a {
		if v == s {
			return true
		}
	}
	return false
}
func hasIndex(a []int, i int) bool {
	for _, v := range a {
		if v == i {
			return true
		}
	}
	return false
}

// UnknownFields returns the cumulative number of unknown member occurrences.
func UnknownFields(venue, channel string) uint64 {
	v, ok := unknownFields.Load(policyKey{venue, channel})
	if !ok {
		return 0
	}
	return v.(*atomic.Uint64).Load()
}

// channelDropped reports whether an explicit policy disables scheduled work.
// Channels without a policy remain enabled, just as in ApplyPolicy.
func channelDropped(venue, channel string) bool {
	if policiesDisabled {
		return false
	}
	policies.RLock()
	defer policies.RUnlock()
	return policies.m[policyKey{venue, channel}].DropChannel
}

// ApplyPolicy filters one JSON payload. Without a registered policy it returns
// the input unchanged. record is false for a dropped channel.
func ApplyPolicy(venue, channel string, raw []byte) (payload []byte, record bool, err error) {
	if policiesDisabled {
		return raw, true, nil
	}
	key := policyKey{venue, channel}
	policies.RLock()
	p, ok := policies.m[key]
	policies.RUnlock()
	if !ok {
		return raw, true, nil
	}
	if p.DropChannel {
		return nil, false, nil
	}
	if !json.Valid(raw) {
		return nil, false, fmt.Errorf("policy: invalid JSON")
	}
	var unknown uint64
	payload, err = filterPolicy(raw, p, p.Nested, "", &unknown)
	if err != nil {
		return nil, false, err
	}
	if unknown > 0 {
		v, _ := unknownFields.LoadOrStore(key, &atomic.Uint64{})
		v.(*atomic.Uint64).Add(unknown)
	}
	return payload, true, nil
}

func filterPolicy(raw []byte, p Policy, nested map[string]Policy, path string, unknown *uint64) ([]byte, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	tok, err := d.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return raw, nil
	}
	var out bytes.Buffer
	switch delim {
	case '{':
		out.WriteByte('{')
		first := true
		for d.More() {
			k, err := d.Token()
			if err != nil {
				return nil, err
			}
			key := k.(string)
			var value json.RawMessage
			if err = d.Decode(&value); err != nil {
				return nil, err
			}
			if hasString(p.Drop, key) {
				continue
			}
			if !hasString(p.Keep, key) {
				*unknown++
			}
			child := key
			if path != "" {
				child = path + "." + key
			}
			if np, ok := nested[child]; ok {
				value, err = filterPolicy(value, np, nested, child, unknown)
				if err != nil {
					return nil, err
				}
			} else if np, ok := nested[child+"[]"]; ok {
				value, err = filterArrayMembers(value, np, nested, child+"[]", unknown)
				if err != nil {
					return nil, err
				}
			}
			if !first {
				out.WriteByte(',')
			}
			first = false
			kb, _ := json.Marshal(key)
			out.Write(kb)
			out.WriteByte(':')
			out.Write(value)
		}
		out.WriteByte('}')
	case '[':
		if path == "" {
			if np, ok := nested["[]"]; ok {
				return filterRootArrayMembers(raw, np, nested, unknown)
			}
		}
		out.WriteByte('[')
		first := true
		i := 0
		for d.More() {
			var value json.RawMessage
			if err = d.Decode(&value); err != nil {
				return nil, err
			}
			idx := i
			i++
			if hasIndex(p.DropIndexes, idx) {
				continue
			}
			if !hasIndex(p.KeepIndexes, idx) {
				*unknown++
			}
			if !first {
				out.WriteByte(',')
			}
			first = false
			out.Write(value)
		}
		out.WriteByte(']')
	default:
		return nil, fmt.Errorf("policy: unexpected delimiter %s", strconv.Quote(string(delim)))
	}
	if _, err = d.Token(); err != nil && err != io.EOF {
		return nil, err
	}
	return out.Bytes(), nil
}

func filterRootArrayMembers(raw []byte, p Policy, nested map[string]Policy, unknown *uint64) ([]byte, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	if _, err := d.Token(); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.WriteByte('[')
	first := true
	for d.More() {
		var v json.RawMessage
		if err := d.Decode(&v); err != nil {
			return nil, err
		}
		if len(v) > 0 && v[0] == '{' {
			var err error
			v, err = filterPolicy(v, p, nested, "[]", unknown)
			if err != nil {
				return nil, err
			}
		}
		if !first {
			out.WriteByte(',')
		}
		first = false
		out.Write(v)
	}
	out.WriteByte(']')
	return out.Bytes(), nil
}

func filterArrayMembers(raw []byte, p Policy, nested map[string]Policy, path string, unknown *uint64) ([]byte, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	if t != json.Delim('[') {
		return nil, fmt.Errorf("policy: %s is not an array", path)
	}
	var out bytes.Buffer
	out.WriteByte('[')
	first := true
	for d.More() {
		var v json.RawMessage
		if err = d.Decode(&v); err != nil {
			return nil, err
		}
		v, err = filterPolicy(v, p, nested, path, unknown)
		if err != nil {
			return nil, err
		}
		if !first {
			out.WriteByte(',')
		}
		first = false
		out.Write(v)
	}
	out.WriteByte(']')
	return out.Bytes(), nil
}
