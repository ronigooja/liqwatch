// Package venuemap loads reviewed venue-to-Binance symbol mappings.
package venuemap

import (
	"fmt"
	"os"
	"sort"

	"github.com/pelletier/go-toml/v2"
)

// Pair is a proposed mapping. Exp10 is log10(venue price / Binance price).
type Pair struct {
	Binance string `toml:"binance"`
	Venue   string `toml:"venue"`
	Symbol  string `toml:"symbol"`
	Exp10   int    `toml:"exp10"`
	Status  string `toml:"status"`
	Note    string `toml:"note"`
}

// Map contains validated mapping proposals.
type Map struct {
	Pairs []Pair `toml:"pair"`
}

func usable(p Pair) bool { return p.Status == "auto" || p.Status == "reviewed" }

// Load reads and validates a TOML map.
func Load(path string) (*Map, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Map
	if err = toml.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("decode venue map: %w", err)
	}
	seen := map[string]bool{}
	accepted := map[string]bool{}
	for _, p := range m.Pairs {
		switch p.Venue {
		case "bybit", "okx", "bitget", "gate", "mexc", "kucoin":
		default:
			return nil, fmt.Errorf("unknown venue %q", p.Venue)
		}
		switch p.Status {
		case "auto", "reviewed", "review", "rejected":
		default:
			return nil, fmt.Errorf("unknown status %q", p.Status)
		}
		if p.Binance == "" || p.Symbol == "" {
			return nil, fmt.Errorf("empty symbol in venue map")
		}
		key := p.Venue + "\x00" + p.Symbol
		if seen[key] {
			return nil, fmt.Errorf("duplicate venue symbol %s/%s", p.Venue, p.Symbol)
		}
		seen[key] = true
		key = p.Venue + "\x00" + p.Binance
		if usable(p) && accepted[key] {
			return nil, fmt.Errorf("duplicate usable Binance symbol %s/%s", p.Venue, p.Binance)
		}
		if usable(p) {
			accepted[key] = true
		}
	}
	return &m, nil
}

// Symbols returns the sorted usable venue symbols.
func (m *Map) Symbols(venue string) []string {
	var out []string
	for _, p := range m.Pairs {
		if p.Venue == venue && usable(p) {
			out = append(out, p.Symbol)
		}
	}
	sort.Strings(out)
	return out
}

func (m *Map) find(venue, symbol string) (Pair, bool) {
	for _, p := range m.Pairs {
		if p.Venue == venue && p.Symbol == symbol && usable(p) {
			return p, true
		}
	}
	return Pair{}, false
}

// Binance returns the Binance symbol for a usable mapping.
func (m *Map) Binance(venue, symbol string) (string, bool) {
	p, ok := m.find(venue, symbol)
	return p.Binance, ok
}

// Exp10 returns the price exponent for a usable mapping.
func (m *Map) Exp10(venue, symbol string) (int, bool) {
	p, ok := m.find(venue, symbol)
	return p.Exp10, ok
}

// ByStatus returns entries in file order with the given status.
func (m *Map) ByStatus(status string) []Pair {
	var out []Pair
	for _, p := range m.Pairs {
		if p.Status == status {
			out = append(out, p)
		}
	}
	return out
}
