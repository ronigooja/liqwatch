package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"

	"github.com/ronigooja/liqwatch/internal/venuemap"
)

func normalize(s string) string {
	for _, p := range []string{"1000000", "100000", "10000", "1000", "1M", "1m", "k"} {
		if strings.HasPrefix(s, p) && len(s) > len(p) {
			c := rune(s[len(p)])
			if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' {
				s = s[len(p):]
				break
			}
		}
	}
	s = strings.ToUpper(s)
	if s == "XBT" {
		return "BTC"
	}
	return s
}
func power(r float64) (int, bool) {
	if r <= 0 || math.IsNaN(r) || math.IsInf(r, 0) {
		return 0, false
	}
	x := math.Log10(r)
	n := math.Round(x)
	return int(n), math.Abs(x-n) < 0.06
}

var pinyin = map[string][]string{
	"币安人生": {"BIANRENSHENG"}, "我踏马来了": {"WOTAMALAILE"}, "龙虾": {"LONGXIA"}, "牛来": {"NIULAI"}, "哈基米": {"HAJIMI"},
}

type candidate struct {
	p     venuemap.Pair
	ratio float64
	exact bool
}

func propose(bn, vs []instrument, venue string) []venuemap.Pair {
	bnNames := map[string]bool{}
	venueNames := map[string]int{}
	for _, b := range bn {
		bnNames[normalize(b.Base)] = true
	}
	for _, v := range vs {
		venueNames[normalize(v.Base)]++
	}
	var cs []candidate
	for _, b := range bn {
		n := normalize(b.Base)
		for _, v := range vs {
			vn := normalize(v.Base)
			ratio := 0.0
			if b.Price > 0 {
				ratio = v.Price / b.Price
			}
			if ratio <= 0 {
				continue
			}
			exact := n == vn
			chinese := false
			for _, alias := range pinyin[b.Base] {
				if vn == alias || vn == n {
					chinese = true
				}
			}
			if !exact && !chinese && !(len([]rune(n)) >= 3 && len([]rune(vn)) >= 3 && (strings.Contains(n, vn) || strings.Contains(vn, n))) {
				continue
			}
			p := venuemap.Pair{Binance: b.Symbol, Venue: venue, Symbol: v.Symbol}
			exp, near := power(ratio)
			p.Exp10 = exp
			switch {
			case chinese || containsHan(b.Base):
				if ratio < 0.97 || ratio > 1.03 {
					continue
				}
				p.Status = "review"
			case exact && near:
				p.Status = "auto"
			case exact:
				p.Status = "rejected"
			default:
				if ratio < 0.99 || ratio > 1.01 {
					continue
				}
				p.Status = "auto"
				if bnNames[vn] || venueNames[vn] > 1 || strings.Contains(n, "LUNA2") && vn == "LUNA" {
					p.Status = "review"
				}
			}
			p.Note = fmt.Sprintf("ratio=%.10g", ratio)
			cs = append(cs, candidate{p, ratio, exact})
		}
	}
	// A venue symbol can only have one proposal. Prefer exact names, then review ambiguity.
	bySymbol := map[string][]int{}
	for i, c := range cs {
		bySymbol[c.p.Symbol] = append(bySymbol[c.p.Symbol], i)
	}
	keep := make([]bool, len(cs))
	for _, ids := range bySymbol {
		best := ids
		var exact []int
		for _, i := range ids {
			if cs[i].exact {
				exact = append(exact, i)
			}
		}
		if len(exact) > 0 {
			best = exact
		}
		sort.Slice(best, func(i, j int) bool { return cs[best[i]].p.Binance < cs[best[j]].p.Binance })
		keep[best[0]] = true
		if len(best) > 1 || len(ids) > 1 {
			cs[best[0]].p.Status = "review"
		}
	}
	byBin := map[string][]int{}
	for i, c := range cs {
		if keep[i] && c.p.Status == "auto" {
			byBin[c.p.Binance] = append(byBin[c.p.Binance], i)
		}
	}
	for _, ids := range byBin {
		if len(ids) > 1 {
			for _, i := range ids {
				cs[i].p.Status = "review"
			}
		}
	}
	var out []venuemap.Pair
	for i, c := range cs {
		if keep[i] {
			out = append(out, c.p)
		}
	}
	return out
}
func containsHan(s string) bool {
	for _, r := range s {
		if unicode.Is(unicode.Han, r) {
			return true
		}
	}
	return false
}
