package venue

import (
	"log"

	"github.com/ronigooja/liqwatch/internal/venuemap"
)

// Filter keeps symbols with usable mappings, preserving venue symbol names and order.
func Filter(syms []Symbol, m *venuemap.Map, venueName string) []Symbol {
	if venueName == "binance" {
		return syms
	}
	var out []Symbol
	for _, s := range syms {
		if _, ok := m.Binance(venueName, s); ok {
			out = append(out, s)
		}
	}
	log.Printf("venue=%s mapping_dropped=%d", venueName, len(syms)-len(out))
	return out
}
