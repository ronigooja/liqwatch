package pickdepth

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const endpoint = "https://fapi.binance.com"

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type Symbol struct {
	Symbol            string   `json:"symbol"`
	ContractType      string   `json:"contractType"`
	Status            string   `json:"status"`
	QuoteAsset        string   `json:"quoteAsset"`
	UnderlyingType    string   `json:"underlyingType"`
	UnderlyingSubType []string `json:"underlyingSubType"`
}

type exchangeInfo struct {
	Symbols    []Symbol `json:"symbols"`
	RateLimits []struct {
		RateLimitType string `json:"rateLimitType"`
		Interval      string `json:"interval"`
		IntervalNum   int    `json:"intervalNum"`
		Limit         int    `json:"limit"`
	} `json:"rateLimits"`
}

type Candle struct {
	OpenTime time.Time
	Open     float64
	High     float64
	Low      float64
	Close    float64
	QuoteVol float64
}

type Metrics struct {
	Symbol            string
	MeanDailyQuote    float64
	WickCount         int
	DominantWickCount int
	SpikeCount        int // one-sided wick, large vs the symbol's own median range, price pulled back
	MedianRangePct    float64
	MaxRangePct       float64
}

// Spike definition (per 1m candle): the larger one-sided wick is at least spikeMinWickPct of the
// open and at least spikeMedianMult times the symbol's median 1m range, and the body is at most
// spikeMaxBodyFrac of the candle range (the move was largely reversed).
const (
	spikeMinWickPct  = 1.0
	spikeMedianMult  = 8.0
	spikeMaxBodyFrac = 0.3
)

type Selection struct {
	Metrics
	Role string
}

type Options struct {
	Days         int
	WickPct      float64
	MinQuoteVol  float64
	N            int
	Controls     int
	LimitSymbols int
	CacheDir     string // optional: per-symbol candle cache, reused across runs
	Now          time.Time
}

func DefaultOptions() Options {
	return Options{Days: 30, WickPct: 2, MinQuoteVol: 5_000_000, N: 15, Controls: 3}
}

func ComputeMetrics(symbol string, candles []Candle, wickPct float64) Metrics {
	m := Metrics{Symbol: symbol}
	if len(candles) == 0 {
		return m
	}
	daily := make(map[string]float64)
	ranges := make([]float64, 0, len(candles))
	for _, c := range candles {
		if c.Open > 0 {
			ranges = append(ranges, (c.High-c.Low)/c.Open*100)
		}
	}
	if len(ranges) > 0 {
		sort.Float64s(ranges)
		m.MedianRangePct = ranges[len(ranges)/2]
	}
	for _, c := range candles {
		day := c.OpenTime.UTC().Format("2006-01-02")
		daily[day] += c.QuoteVol
		if c.Open <= 0 {
			continue
		}
		rangePct := (c.High - c.Low) / c.Open * 100
		if rangePct > m.MaxRangePct {
			m.MaxRangePct = rangePct
		}
		if rangePct >= wickPct {
			m.WickCount++
		}
		span := c.High - c.Low
		if span > 0 {
			wick := math.Max(c.High-math.Max(c.Open, c.Close), math.Min(c.Open, c.Close)-c.Low) / c.Open * 100
			body := math.Abs(c.Close-c.Open) / span
			if wick >= spikeMinWickPct && wick >= spikeMedianMult*m.MedianRangePct && body <= spikeMaxBodyFrac {
				m.SpikeCount++
			}
		}
		if span > 0 && rangePct >= 1 && math.Max(c.High-c.Close, c.Close-c.Low)/span >= .7 {
			m.DominantWickCount++
		}
	}
	for _, v := range daily {
		m.MeanDailyQuote += v
	}
	m.MeanDailyQuote /= float64(len(daily))
	return m
}

func Select(metrics []Metrics, opts Options) ([]Selection, error) {
	if opts.N < 0 || opts.Controls < 0 || opts.Controls > opts.N {
		return nil, errors.New("require 0 <= controls <= n")
	}
	eligible := make([]Metrics, 0, len(metrics))
	for _, m := range metrics {
		if m.MeanDailyQuote >= opts.MinQuoteVol {
			eligible = append(eligible, m)
		}
	}
	byVolume := append([]Metrics(nil), eligible...)
	sort.SliceStable(byVolume, func(i, j int) bool {
		if byVolume[i].MeanDailyQuote != byVolume[j].MeanDailyQuote {
			return byVolume[i].MeanDailyQuote > byVolume[j].MeanDailyQuote
		}
		return byVolume[i].Symbol < byVolume[j].Symbol
	})
	if len(byVolume) < opts.Controls {
		return nil, fmt.Errorf("only %d symbols pass liquidity floor; need %d controls", len(byVolume), opts.Controls)
	}
	selected := make([]Selection, 0, opts.N)
	used := make(map[string]bool)
	for _, m := range byVolume[:opts.Controls] {
		selected = append(selected, Selection{Metrics: m, Role: "control"})
		used[m.Symbol] = true
	}
	spikes := append([]Metrics(nil), eligible...)
	sort.SliceStable(spikes, func(i, j int) bool {
		if spikes[i].SpikeCount != spikes[j].SpikeCount {
			return spikes[i].SpikeCount > spikes[j].SpikeCount
		}
		if spikes[i].MeanDailyQuote != spikes[j].MeanDailyQuote {
			return spikes[i].MeanDailyQuote > spikes[j].MeanDailyQuote
		}
		return spikes[i].Symbol < spikes[j].Symbol
	})
	for _, m := range spikes {
		if len(selected) == opts.N {
			break
		}
		if !used[m.Symbol] {
			selected = append(selected, Selection{Metrics: m, Role: "spike"})
			used[m.Symbol] = true
		}
	}
	return selected, nil
}

func Fetch(ctx context.Context, client HTTPDoer, opts Options) ([]Metrics, error) {
	if opts.Days <= 0 || opts.N < 0 || opts.Controls < 0 {
		return nil, errors.New("invalid options")
	}
	info, err := getExchangeInfo(ctx, client)
	if err != nil {
		return nil, err
	}
	symbols := eligibleSymbols(info.Symbols)
	if opts.LimitSymbols > 0 && len(symbols) > opts.LimitSymbols {
		symbols = symbols[:opts.LimitSymbols]
	}
	rate := requestPacer(info.RateLimits)
	end := opts.Now
	if end.IsZero() {
		end = time.Now()
	}
	start := end.Add(-time.Duration(opts.Days) * 24 * time.Hour)
	all := make([]Metrics, 0, len(symbols))
	for i, symbol := range symbols {
		fmt.Fprintf(os.Stderr, "[%d/%d] %s\n", i+1, len(symbols), symbol.Symbol)
		candles, ok := loadCache(opts.CacheDir, symbol.Symbol, opts.Days)
		if !ok {
			var err error
			candles, err = fetchKlines(ctx, client, rate, symbol.Symbol, start, end)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", symbol.Symbol, err)
			}
			saveCache(opts.CacheDir, symbol.Symbol, opts.Days, candles)
		}
		all = append(all, ComputeMetrics(symbol.Symbol, candles, opts.WickPct))
	}
	return all, nil
}

func eligibleSymbols(symbols []Symbol) []Symbol {
	out := make([]Symbol, 0, len(symbols))
	for _, s := range symbols {
		if s.ContractType != "PERPETUAL" || s.Status != "TRADING" || s.QuoteAsset != "USDT" || strings.EqualFold(s.UnderlyingType, "TRADIFI_PERPETUAL") {
			continue
		}
		excluded := false
		for _, subtype := range s.UnderlyingSubType {
			if strings.EqualFold(subtype, "TRADIFI_PERPETUAL") {
				excluded = true
			}
		}
		if !excluded {
			out = append(out, s)
		}
	}
	return out
}

func requestPacer(limits []struct {
	RateLimitType string `json:"rateLimitType"`
	Interval      string `json:"interval"`
	IntervalNum   int    `json:"intervalNum"`
	Limit         int    `json:"limit"`
}) func(context.Context, int) error {
	limit := 1200
	for _, l := range limits {
		if l.RateLimitType == "REQUEST_WEIGHT" && l.Interval == "MINUTE" && l.IntervalNum == 1 && l.Limit > 0 {
			limit = l.Limit
		}
	}
	// Twenty percent of the advertised minute budget, with ten-weight klines pages.
	interval := time.Minute * 10 / time.Duration(limit/5)
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	var last time.Time
	return func(ctx context.Context, weight int) error {
		if weight <= 0 {
			return nil
		}
		wait := time.Duration(weight) * interval / 10
		if !last.IsZero() {
			wait -= time.Since(last)
		}
		if wait > 0 {
			timer := time.NewTimer(wait)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
		last = time.Now()
		return nil
	}
}

func getExchangeInfo(ctx context.Context, client HTTPDoer) (exchangeInfo, error) {
	var info exchangeInfo
	if err := getJSON(ctx, client, endpoint+"/fapi/v1/exchangeInfo", nil, &info); err != nil {
		return info, err
	}
	return info, nil
}

func fetchKlines(ctx context.Context, client HTTPDoer, pace func(context.Context, int) error, symbol string, start, end time.Time) ([]Candle, error) {
	var out []Candle
	for cursor := start.UnixMilli(); cursor < end.UnixMilli(); {
		if err := pace(ctx, 10); err != nil {
			return nil, err
		}
		q := url.Values{"symbol": {symbol}, "interval": {"1m"}, "limit": {"1500"}, "startTime": {strconv.FormatInt(cursor, 10)}, "endTime": {strconv.FormatInt(end.UnixMilli(), 10)}}
		var raw [][]json.RawMessage
		if err := getJSON(ctx, client, endpoint+"/fapi/v1/klines", q, &raw); err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			break
		}
		for _, row := range raw {
			if len(row) < 8 {
				return nil, errors.New("malformed kline")
			}
			vals := make([]float64, 5)
			for i, field := range []int{1, 2, 3, 4, 7} {
				var s string
				if err := json.Unmarshal(row[field], &s); err != nil {
					return nil, err
				}
				v, err := strconv.ParseFloat(s, 64)
				if err != nil {
					return nil, err
				}
				vals[i] = v
			}
			var millis int64
			if err := json.Unmarshal(row[0], &millis); err != nil {
				return nil, err
			}
			out = append(out, Candle{OpenTime: time.UnixMilli(millis).UTC(), Open: vals[0], High: vals[1], Low: vals[2], Close: vals[3], QuoteVol: vals[4]})
		}
		last := raw[len(raw)-1]
		var millis int64
		if err := json.Unmarshal(last[0], &millis); err != nil {
			return nil, err
		}
		if millis < cursor || len(raw) < 1500 {
			break
		}
		cursor = millis + 60_000
	}
	return out, nil
}

func getJSON(ctx context.Context, client HTTPDoer, rawURL string, query url.Values, dst any) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	u.RawQuery = query.Encode()
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusTeapot {
			backoff := time.Duration(1<<attempt) * time.Second
			if v, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil && v > 0 {
				backoff = time.Duration(v) * time.Second
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("binance HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		if err := json.Unmarshal(body, dst); err != nil {
			return err
		}
		return nil
	}
	return errors.New("too many rate-limit retries")
}

func cachePath(dir, symbol string, days int) string {
	return filepath.Join(dir, fmt.Sprintf("%s-%dd.json.gz", symbol, days))
}

func loadCache(dir, symbol string, days int) ([]Candle, bool) {
	if dir == "" {
		return nil, false
	}
	f, err := os.Open(cachePath(dir, symbol, days))
	if err != nil {
		return nil, false
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, false
	}
	defer zr.Close()
	var c []Candle
	if json.NewDecoder(zr).Decode(&c) != nil || len(c) == 0 {
		return nil, false
	}
	return c, true
}

func saveCache(dir, symbol string, days int, c []Candle) {
	if dir == "" || len(c) == 0 {
		return
	}
	if os.MkdirAll(dir, 0o755) != nil {
		return
	}
	tmp := cachePath(dir, symbol, days) + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return
	}
	zw := gzip.NewWriter(f)
	err = json.NewEncoder(zw).Encode(c)
	if e := zw.Close(); err == nil {
		err = e
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		os.Remove(tmp)
		return
	}
	os.Rename(tmp, cachePath(dir, symbol, days))
}
