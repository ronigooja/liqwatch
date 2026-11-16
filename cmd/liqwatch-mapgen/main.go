// Command liqwatch-mapgen proposes venue mappings from live mark prices.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/ronigooja/liqwatch/internal/venuemap"
)

func key(p venuemap.Pair) string { return p.Venue + "\x00" + p.Symbol }
func merge(current, proposed []venuemap.Pair, active map[string]map[string]bool) ([]venuemap.Pair, []string) {
	old := map[string]venuemap.Pair{}
	fresh := map[string]venuemap.Pair{}
	for _, p := range current {
		old[key(p)] = p
	}
	for _, p := range proposed {
		fresh[key(p)] = p
	}
	var out []venuemap.Pair
	var diff []string
	for _, p := range proposed {
		k := key(p)
		if q, ok := old[k]; ok {
			if q.Status == "reviewed" || q.Status == "rejected" || q.Status == "review" {
				p = q
			}
			if !reflect.DeepEqual(q, p) {
				diff = append(diff, fmt.Sprintf("changed %s %s %s -> %s", p.Venue, p.Symbol, q.Status, p.Status))
			}
		} else {
			diff = append(diff, fmt.Sprintf("added %s %s %s", p.Venue, p.Symbol, p.Status))
		}
		out = append(out, p)
	}
	for _, p := range current {
		if _, ok := fresh[key(p)]; !ok {
			if symbols, selected := active[p.Venue]; selected && !symbols[p.Symbol] {
				diff = append(diff, fmt.Sprintf("removed %s %s", p.Venue, p.Symbol))
				continue
			}
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Venue != b.Venue {
			return a.Venue < b.Venue
		}
		if a.Binance != b.Binance {
			return a.Binance < b.Binance
		}
		return a.Symbol < b.Symbol
	})
	sort.Strings(diff)
	return out, diff
}
func run() error {
	out := flag.String("out", "config/venue-map.toml", "output TOML")
	existing := flag.String("existing", "config/venue-map.toml", "existing decisions")
	diff := flag.Bool("diff", false, "print changes and review entries")
	venues := flag.String("venues", "bybit,okx,bitget,gate,mexc,kucoin", "comma-separated venues")
	timeout := flag.Duration("http-timeout", 30*time.Second, "HTTP request timeout")
	flag.Parse()
	selected := map[string]bool{}
	for _, v := range strings.Split(*venues, ",") {
		switch v {
		case "bybit", "okx", "bitget", "gate", "mexc", "kucoin":
			selected[v] = true
		default:
			return fmt.Errorf("unknown venue %q", v)
		}
	}
	var current []venuemap.Pair
	m, err := venuemap.Load(*existing)
	if err == nil {
		current = m.Pairs
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("existing map: %w", err)
	}
	f := &httpFetcher{client: &http.Client{Timeout: *timeout}}
	bn, err := discover(f, "binance")
	if err != nil {
		return err
	}
	if len(bn) == 0 {
		return fmt.Errorf("Binance returned no instruments")
	}
	var proposals []venuemap.Pair
	active := map[string]map[string]bool{}
	for _, v := range []string{"bybit", "okx", "bitget", "gate", "mexc", "kucoin"} {
		if !selected[v] {
			continue
		}
		symbols, e := discover(f, v)
		if e != nil {
			return e
		}
		if len(symbols) == 0 {
			return fmt.Errorf("%s returned no instruments", v)
		}
		active[v] = map[string]bool{}
		for _, symbol := range symbols {
			active[v][symbol.Symbol] = true
		}
		proposals = append(proposals, propose(bn, symbols, v)...)
	}
	pairs, changes := merge(current, proposals, active)
	b, err := toml.Marshal(venuemap.Map{Pairs: pairs})
	if err != nil {
		return err
	}
	if err = os.WriteFile(*out, b, 0644); err != nil {
		return err
	}
	if _, err = venuemap.Load(*out); err != nil {
		return fmt.Errorf("generated map: %w", err)
	}
	if *diff {
		for _, s := range changes {
			fmt.Println(s)
		}
		for _, p := range pairs {
			if p.Status == "review" {
				fmt.Printf("review %s %s %s %s\n", p.Venue, p.Binance, p.Symbol, p.Note)
			}
		}
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
