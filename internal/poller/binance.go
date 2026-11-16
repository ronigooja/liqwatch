package poller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

var monoOrigin = time.Now()

// PollConfig is the [poller] table. The top-level channel maps and URLs predate venues and apply to
// binance; every venue (binance included) can override them in [poller.venues.<name>].
type PollConfig struct {
	WeightPerMinute float64                `toml:"weight_per_minute"`
	Weights         map[string]float64     `toml:"weights"`
	RPS             map[string]float64     `toml:"rps"`
	Periods         map[string]string      `toml:"periods"`
	Overlap         string                 `toml:"overlap"`
	InitialLookback string                 `toml:"initial_lookback"` // deprecated: does not widen live recording scope
	BaseURL         string                 `toml:"base_url"`
	BracketsURL     string                 `toml:"brackets_url"`
	MapPath         string                 `toml:"map_path"` // venue map for the symbol universe; default /etc/liqwatch/venue-map.toml
	Venues          map[string]VenueConfig `toml:"venues"`
}

func init() { RegisterVenue("binance", binanceFactory, WithBaseURL("https://fapi.binance.com")) }

// binanceFactory adapts the Binance channels to Env: universe from exchangeInfo, config merged with
// [poller.venues.binance].
func binanceFactory(e *Env) []Channel {
	cfg := e.Config
	vc := cfg.Venues["binance"]
	merge := func(top, over map[string]float64) map[string]float64 {
		out := map[string]float64{}
		for k, v := range top {
			out[k] = v
		}
		for k, v := range over {
			out[k] = v
		}
		return out
	}
	cfg.RPS, cfg.Weights = merge(cfg.RPS, vc.ChannelRPS), merge(cfg.Weights, vc.Weights)
	periods := map[string]string{}
	for k, v := range cfg.Periods {
		periods[k] = v
	}
	for k, v := range vc.Periods {
		periods[k] = v
	}
	cfg.Periods = periods
	if vc.Overlap != "" {
		cfg.Overlap = vc.Overlap
	}
	if vc.InitialLookback != "" {
		cfg.InitialLookback = vc.InitialLookback
	}
	cfg.BaseURL = e.BaseURL
	bn := &Binance{Doer: e.Doer, Clock: e.Clock, Config: cfg, Selected: e.Selected}
	e.Universe = bn.Symbols
	e.OnBind(func(s *Scheduler) { bn.Scheduler = s })
	return BinanceChannels(bn)
}

type Binance struct {
	Doer                          HTTPDoer
	Clock                         Clock
	Config                        PollConfig
	Scheduler                     *Scheduler
	Selected                      []string
	HistoryRequests               atomic.Uint64
	HistoryResponsesWithoutWeight atomic.Uint64
}

func (b *Binance) Symbols() []string {
	if b.Scheduler == nil {
		return nil
	}
	all := b.Scheduler.Symbols()
	if len(b.Selected) == 0 {
		return all
	}
	selected := map[string]bool{}
	for _, s := range b.Selected {
		selected[s] = true
	}
	out := []string{}
	for _, s := range all {
		if selected[s] {
			out = append(out, s)
		}
	}
	return out
}
func (b *Binance) request(ctx context.Context, name, sym string) ([]Record, error) {
	if isHistory(name) || name == "fundingRate" {
		return b.history(ctx, name, sym)
	}
	return b.requestPage(ctx, name, sym, 0, 0)
}
func isHistory(name string) bool {
	switch name {
	case "openInterestHist", "topLongShortPositionRatio", "topLongShortAccountRatio", "globalLongShortAccountRatio", "takerlongshortRatio":
		return true
	}
	return false
}
func duration(s string, fallback time.Duration) time.Duration {
	d, e := time.ParseDuration(s)
	if e != nil || d <= 0 {
		return fallback
	}
	return d
}
func (b *Binance) history(ctx context.Context, name, sym string) ([]Record, error) {
	h := &History{Venue: "binance", Channel: name, Symbol: sym, Clock: b.Clock, Sched: b.Scheduler,
		Overlap: duration(b.Config.Overlap, 2*time.Hour), Lookback: duration(b.Config.InitialLookback, 0), MaxLookback: 29 * 24 * time.Hour,
		Fetch: func(ctx context.Context, start, end int64) ([]Row, error) {
			recs, err := b.requestPage(ctx, name, sym, start, end)
			if err != nil {
				return nil, err
			}
			rows := make([]Row, len(recs))
			for i, r := range recs {
				rows[i] = Row{TS: r.Meta["last_row_timestamp"].(int64), Raw: r.Payload, Meta: map[string]any{"status": r.Meta["status"], "used_weight_1m": r.Meta["used_weight_1m"]}}
			}
			return rows, nil
		}}
	if isHistory(name) {
		h.Retention, h.Step, h.Mode, h.Window = 29*24*time.Hour, 5*time.Minute, PageWindows, 400
	} else {
		h.Mode, h.PageSize, h.SkipNoData = PageForward, 1000, true
	}
	return h.Run(ctx)
}
func (b *Binance) requestPage(ctx context.Context, name, sym string, start, end int64) ([]Record, error) {
	base := b.Config.BaseURL
	if base == "" {
		base = "https://fapi.binance.com"
	}
	endpoint := base + "/fapi/v1/" + name
	method := "GET"
	q := url.Values{}
	if sym != "" {
		q.Set("symbol", sym)
	}
	series := name == "openInterestHist" || name == "topLongShortPositionRatio" || name == "topLongShortAccountRatio" || name == "globalLongShortAccountRatio" || name == "takerlongshortRatio"
	if series {
		endpoint = base + "/futures/data/" + name
		q.Set("period", "5m")
		q.Set("limit", "500")
	}
	if series || name == "fundingRate" {
		q.Set("startTime", strconv.FormatInt(start, 10))
		q.Set("endTime", strconv.FormatInt(end, 10))
		if !series {
			q.Set("limit", "1000")
		}
	}
	var body io.Reader
	if name == "brackets" {
		endpoint = b.Config.BracketsURL
		if endpoint == "" {
			endpoint = "https://www.binance.com/bapi/futures/v1/friendly/future/common/brackets"
		}
		method = "POST"
		body = bytes.NewBufferString("{}")
	}
	if len(q) > 0 {
		endpoint += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if series {
		b.HistoryRequests.Add(1)
	}
	started := b.Clock.Now()
	resp, err := b.Doer.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	received := b.Clock.Now()
	if series && resp.Header.Get("x-mbx-used-weight-1m") == "" {
		b.HistoryResponsesWithoutWeight.Add(1)
	}
	if resp.StatusCode == 429 || resp.StatusCode == 418 {
		return nil, rateLimitError(req, resp, started, received, b.Clock)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, raw)
	}
	if !json.Valid(raw) {
		if name == "brackets" {
			return nil, &PermanentError{fmt.Errorf("invalid brackets JSON"), raw}
		}
		return nil, fmt.Errorf("invalid JSON")
	}
	meta := map[string]any{"status": resp.StatusCode, "used_weight_1m": nil}
	if v := resp.Header.Get("x-mbx-used-weight-1m"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			meta["used_weight_1m"] = n
		}
	}
	recv := b.Clock.Now()
	rec := Record{Venue: "binance", Symbol: sym, Channel: name, RecvWall: recv.UnixNano(), RecvMono: time.Since(monoOrigin).Nanoseconds(), Meta: meta, Payload: raw}
	if name == "exchangeInfo" {
		var v struct {
			Symbols []struct{ Symbol, Status, ContractType, QuoteAsset string }
		}
		if err = json.Unmarshal(raw, &v); err != nil || v.Symbols == nil {
			return nil, fmt.Errorf("invalid exchangeInfo")
		}
		symbols := []string{}
		for _, x := range v.Symbols {
			if x.Status == "TRADING" && x.ContractType == "PERPETUAL" && x.QuoteAsset == "USDT" {
				symbols = append(symbols, x.Symbol)
			}
		}
		sort.Strings(symbols)
		if b.Scheduler != nil {
			b.Scheduler.SetSymbols(symbols)
		}
	}
	if name == "brackets" {
		entries, err := validateBrackets(raw)
		if err != nil {
			return nil, &PermanentError{err, raw}
		}
		sum := sha256.Sum256(raw)
		meta["response_sha256"] = fmt.Sprintf("%x", sum)
		out := make([]Record, 0, len(entries))
		var allowed map[string]bool
		if b.Scheduler != nil {
			allowed = snapshotScope(b.Symbols)
		}
		for _, entry := range entries {
			var x struct{ Symbol string }
			json.Unmarshal(entry, &x)
			if allowed != nil && !allowed[x.Symbol] {
				continue
			}
			r := rec
			r.Symbol = x.Symbol
			r.Payload = entry
			out = append(out, r)
		}
		return out, nil
	}
	if series || name == "fundingRate" {
		var rows []json.RawMessage
		if err = json.Unmarshal(raw, &rows); err != nil {
			return nil, err
		}
		field := "timestamp"
		if name == "fundingRate" {
			field = "fundingTime"
		}
		out := make([]Record, 0, len(rows))
		for _, row := range rows {
			var obj map[string]json.RawMessage
			var ts int64
			if json.Unmarshal(row, &obj) != nil || json.Unmarshal(obj[field], &ts) != nil || ts <= 0 {
				return nil, fmt.Errorf("missing row timestamp")
			}
			r := rec
			r.Payload = row
			r.Meta = map[string]any{"status": resp.StatusCode, "used_weight_1m": meta["used_weight_1m"], "last_row_timestamp": ts}
			out = append(out, r)
		}
		return out, nil
	}
	return []Record{rec}, nil
}
func validateBrackets(raw []byte) ([]json.RawMessage, error) {
	var root map[string]json.RawMessage
	if json.Unmarshal(raw, &root) != nil {
		return nil, fmt.Errorf("invalid envelope")
	}
	for _, k := range []string{"code", "message", "messageDetail", "data", "success"} {
		if _, ok := root[k]; !ok {
			return nil, fmt.Errorf("missing %s", k)
		}
	}
	var success bool
	if json.Unmarshal(root["success"], &success) != nil || !success {
		return nil, fmt.Errorf("unsuccessful envelope")
	}
	var data map[string]json.RawMessage
	if json.Unmarshal(root["data"], &data) != nil {
		return nil, fmt.Errorf("invalid data")
	}
	var entries []json.RawMessage
	if json.Unmarshal(data["brackets"], &entries) != nil || len(entries) == 0 {
		return nil, fmt.Errorf("invalid brackets")
	}
	seen := map[string]bool{}
	for _, entry := range entries {
		var x map[string]json.RawMessage
		if json.Unmarshal(entry, &x) != nil {
			return nil, fmt.Errorf("invalid entry")
		}
		for _, k := range []string{"symbol", "updateTime", "notionalLimit", "riskBrackets"} {
			v, ok := x[k]
			if !ok || (k != "updateTime" && string(v) == "null") {
				return nil, fmt.Errorf("invalid %s", k)
			}
		}
		var sym string
		if json.Unmarshal(x["symbol"], &sym) != nil || sym == "" || seen[sym] {
			return nil, fmt.Errorf("invalid symbol")
		}
		seen[sym] = true
		var tiers []map[string]json.RawMessage
		if json.Unmarshal(x["riskBrackets"], &tiers) != nil || len(tiers) == 0 {
			return nil, fmt.Errorf("invalid tiers")
		}
		for _, tier := range tiers {
			for _, k := range []string{"bracketSeq", "bracketNotionalFloor", "bracketNotionalCap", "bracketMaintenanceMarginRate", "cumFastMaintenanceAmount", "minOpenPosLeverage", "maxOpenPosLeverage"} {
				v, ok := tier[k]
				if !ok || string(v) == "null" {
					return nil, fmt.Errorf("invalid %s", k)
				}
			}
		}
	}
	return entries, nil
}
func BinanceChannels(b *Binance) []Channel {
	if b.Clock == nil {
		b.Clock = RealClock{}
	}
	if b.Doer == nil {
		b.Doer = &http.Client{Timeout: 30 * time.Second}
	}
	if _, ok := b.Doer.(uaDoer); !ok {
		b.Doer = uaDoer{b.Doer}
	}
	names := []string{"exchangeInfo", "brackets", "constituents", "topLongShortPositionRatio", "topLongShortAccountRatio", "globalLongShortAccountRatio", "takerlongshortRatio", "openInterestHist", "fundingRate"}
	out := make([]Channel, 0, len(names))
	for _, name := range names {
		n := name
		c := Channel{Venue: "binance", Name: n, PerSymbol: n != "exchangeInfo" && n != "brackets", Weight: 1, RPS: 1, Period: 24 * time.Hour, Fetch: func(ctx context.Context, s string) ([]Record, error) { return b.request(ctx, n, s) }}
		if n == "exchangeInfo" || n == "brackets" || n == "constituents" || n == "fundingRate" {
			c.Daily = true
		}
		c.History = isHistory(n) || n == "fundingRate"
		if isHistory(n) {
			c.Skip = func(sym string) bool { return b.Scheduler != nil && b.Scheduler.NoDataSkip("binance", n, sym) }
		}
		if n == "openInterest" {
			c.Period = 5 * time.Minute
			c.RPS = 2
		}
		if w := b.Config.Weights[n]; w > 0 {
			c.Weight = w
		}
		if r := b.Config.RPS[n]; r > 0 {
			c.RPS = r
		}
		if p := b.Config.Periods[n]; p != "" {
			if d, e := time.ParseDuration(p); e == nil && d > 0 {
				c.Period = d
				c.Daily = false
			}
		}
		out = append(out, c)
	}
	return out
}
