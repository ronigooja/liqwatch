package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"

	"github.com/wicker/wicker/internal/pickdepth"
)

func main() {
	opts := pickdepth.DefaultOptions()
	flag.IntVar(&opts.Days, "days", opts.Days, "days of 1m candles")
	flag.Float64Var(&opts.WickPct, "wick-pct", opts.WickPct, "range percentage for the primary wick count")
	flag.Float64Var(&opts.MinQuoteVol, "min-quote-vol", opts.MinQuoteVol, "minimum mean daily quote volume")
	flag.IntVar(&opts.N, "n", opts.N, "number of selected symbols")
	flag.IntVar(&opts.Controls, "controls", opts.Controls, "number of high-volume controls")
	flag.IntVar(&opts.LimitSymbols, "limit-symbols", 0, "limit symbols for a small smoke run")
	flag.Parse()
	if opts.N <= 0 || opts.Controls < 0 || opts.Controls > opts.N || opts.Days <= 0 || opts.WickPct <= 0 {
		fmt.Fprintln(os.Stderr, "invalid flags")
		os.Exit(2)
	}
	metrics, err := pickdepth.Fetch(context.Background(), http.DefaultClient, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	selected, err := pickdepth.Select(metrics, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	printOutput(selected)
}

func printOutput(selected []pickdepth.Selection) {
	symbols := make([]string, 0, len(selected))
	for _, s := range selected {
		symbols = append(symbols, s.Symbol)
	}
	sorted := append([]string(nil), symbols...)
	sort.Strings(sorted)
	fmt.Println("depth_symbols = [")
	for _, symbol := range sorted {
		fmt.Printf("  %q,\n", symbol)
	}
	fmt.Println("]")
	fmt.Println()
	fmt.Println("rank symbol role mean_daily_quote_volume wick_count dominant_wick_count max_range_pct")
	for i, s := range selected {
		fmt.Printf("%4d %-12s %-7s %18.2f %10d %20d %14.4f\n", i+1, s.Symbol, s.Role, s.MeanDailyQuote, s.WickCount, s.DominantWickCount, s.MaxRangePct)
	}
}
