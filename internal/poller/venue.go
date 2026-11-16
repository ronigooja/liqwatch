package poller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/ronigooja/liqwatch/internal/venuemap"
)

// DefaultUserAgent is set on every request that does not carry its own User-Agent.
const DefaultUserAgent = "Mozilla/5.0"

const defaultMapPath = "/etc/liqwatch/venue-map.toml"

// VenueConfig is one [poller.venues.<name>] table. Zero values mean "use the default".
type VenueConfig struct {
	RPS             float64            `toml:"rps"`               // requests per second per endpoint, default 1
	WeightPerMinute float64            `toml:"weight_per_minute"` // request budget per minute, default 240
	BaseURL         string             `toml:"base_url"`          // replaces the venue default, e.g. a test server
	Periods         map[string]string  `toml:"periods"`           // channel -> poll period
	ChannelRPS      map[string]float64 `toml:"channel_rps"`       // channel -> rps
	Weights         map[string]float64 `toml:"weights"`           // channel -> request weight
	Overlap         string             `toml:"overlap"`           // falls back to [poller] overlap
	InitialLookback string             `toml:"initial_lookback"`  // falls back to [poller] initial_lookback
}

// VenueFactory declares the channels of a venue.
type VenueFactory func(*Env) []Channel

// VenueOption tunes a registration.
type VenueOption func(*venueEntry)

type venueEntry struct {
	factory VenueFactory
	baseURL string
}

var registry = map[string]*venueEntry{}

// WithBaseURL sets the production base URL, used when the config has no base_url for the venue.
func WithBaseURL(u string) VenueOption { return func(v *venueEntry) { v.baseURL = u } }

// RegisterVenue registers a venue. Call it from init() in internal/poller/<venue>.go.
func RegisterVenue(name string, factory VenueFactory, opts ...VenueOption) {
	v := &venueEntry{factory: factory}
	for _, o := range opts {
		o(v)
	}
	registry[name] = v
}

// Venues lists the registered venue names, sorted.
func Venues() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Deps are the process-level collaborators of a venue.
type Deps struct {
	Doer     HTTPDoer // nil: http.Client with a 30s timeout
	Clock    Clock    // nil: RealClock
	Sink     Sink
	Selected []string // optional symbol filter (-syms)
}

// Env is what a venue gets: configuration, HTTP, universe, sink and scheduler.
type Env struct {
	Venue string
	// Doer sets User-Agent "Mozilla/5.0" on every request that has none.
	Doer  HTTPDoer
	Clock Clock
	Sink  Sink
	// Config is the poller config as seen by this venue: for binance the [poller] legacy fields merged with
	// [poller.venues.binance]; for other venues the raw config.
	Config   PollConfig
	Selected []string
	// BaseURL is the config override, else the default given to RegisterVenue.
	BaseURL         string
	RPS             float64 // default requests per second per endpoint
	WeightPerMinute float64 // 0: scheduler default
	Overlap         time.Duration
	Lookback        time.Duration
	Periods         map[string]time.Duration
	ChannelRPS      map[string]float64
	Weights         map[string]float64
	// Universe lists the symbols of per-symbol channels. Default: usable symbols of this venue in the venue
	// map, filtered by Selected. A venue may replace it in its factory (binance uses exchangeInfo).
	Universe func() []string
	// Scheduler is nil until Bind; use it only inside callbacks.
	Scheduler *Scheduler
	onBind    []func(*Scheduler)
}

// OnBind registers a callback run when the scheduler exists.
func (e *Env) OnBind(f func(*Scheduler)) { e.onBind = append(e.onBind, f) }

// Bind attaches the scheduler and installs the universe.
func (e *Env) Bind(s *Scheduler) {
	e.Scheduler = s
	s.Universe = e.Universe
	for _, f := range e.onBind {
		f(s)
	}
}

type uaDoer struct{ inner HTTPDoer }

func (d uaDoer) Do(req *http.Request) (*http.Response, error) {
	if req.Header.Get("User-Agent") == "" {
		req = req.Clone(req.Context())
		req.Header.Set("User-Agent", DefaultUserAgent)
	}
	return d.inner.Do(req)
}

func positiveDuration(values ...string) time.Duration {
	for _, v := range values {
		if d := duration(v, 0); d > 0 {
			return d
		}
	}
	return 0
}

func initialLookback(values ...string) time.Duration {
	for _, v := range values {
		if v != "" {
			d, _ := time.ParseDuration(v)
			return max(d, 0)
		}
	}
	return 0
}

// NewVenue builds the Env and channels of a registered venue. Unknown venues are an error that lists the
// registered ones. Call Env.Bind with the scheduler built from the returned channels.
func NewVenue(name string, cfg PollConfig, d Deps) (*Env, []Channel, error) {
	entry := registry[name]
	if entry == nil {
		return nil, nil, fmt.Errorf("unknown venue %q (registered: %s)", name, strings.Join(Venues(), ", "))
	}
	vc := cfg.Venues[name]
	doer := d.Doer
	if doer == nil {
		doer = &http.Client{Timeout: 30 * time.Second}
	}
	clock := d.Clock
	if clock == nil {
		clock = RealClock{}
	}
	e := &Env{Venue: name, Doer: uaDoer{doer}, Clock: clock, Sink: d.Sink, Selected: d.Selected, Config: cfg,
		RPS: vc.RPS, WeightPerMinute: vc.WeightPerMinute, BaseURL: vc.BaseURL,
		Overlap:  positiveDuration(vc.Overlap, cfg.Overlap),
		Lookback: initialLookback(vc.InitialLookback, cfg.InitialLookback),
		Periods:  map[string]time.Duration{}, ChannelRPS: map[string]float64{}, Weights: map[string]float64{}}
	if e.RPS <= 0 {
		e.RPS = 1
	}
	if e.WeightPerMinute <= 0 && name == "binance" {
		e.WeightPerMinute = cfg.WeightPerMinute
	}
	if e.Overlap <= 0 {
		e.Overlap = 2 * time.Hour
	}
	// The top-level channel maps predate venues and belong to binance.
	if name == "binance" {
		for k, v := range cfg.Periods {
			e.Periods[k] = duration(v, 0)
		}
		for k, v := range cfg.RPS {
			e.ChannelRPS[k] = v
		}
		for k, v := range cfg.Weights {
			e.Weights[k] = v
		}
		if e.BaseURL == "" {
			e.BaseURL = cfg.BaseURL
		}
	}
	for k, v := range vc.Periods {
		e.Periods[k] = duration(v, 0)
	}
	for k, v := range vc.ChannelRPS {
		e.ChannelRPS[k] = v
	}
	for k, v := range vc.Weights {
		e.Weights[k] = v
	}
	if e.BaseURL == "" {
		e.BaseURL = entry.baseURL
	}
	e.BaseURL = strings.TrimRight(e.BaseURL, "/")
	channels := entry.factory(e)
	if e.Universe == nil {
		path := cfg.MapPath
		if path == "" {
			path = defaultMapPath
		}
		m, err := venuemap.Load(path)
		if err != nil {
			return nil, nil, fmt.Errorf("venue map for %s: %w", name, err)
		}
		e.Universe = filterSymbols(m.Symbols(name), d.Selected)
	}
	return e, channels, nil
}

func filterSymbols(all, selected []string) func() []string {
	return func() []string {
		if len(selected) == 0 {
			return append([]string(nil), all...)
		}
		want := map[string]bool{}
		for _, s := range selected {
			want[s] = true
		}
		out := []string{}
		for _, s := range all {
			if want[s] {
				out = append(out, s)
			}
		}
		return out
	}
}

// Response is a successful (2xx, valid JSON) HTTP response.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// Get requests BaseURL+path with the query. See Request.
func (e *Env) Get(ctx context.Context, path string, q url.Values) (*Response, error) {
	return e.Request(ctx, "GET", path, q, nil, nil)
}

// Request performs one HTTP request. path is appended to BaseURL unless it is an absolute URL. HTTP 429
// and 418 become a RetryError honouring Retry-After (default 60s); other non-2xx statuses and invalid JSON
// bodies are errors. Venues that report errors inside a 200 body check them on Response.Body.
func (e *Env) Request(ctx context.Context, method, path string, q url.Values, body []byte, header http.Header) (*Response, error) {
	endpoint := path
	if !strings.HasPrefix(path, "http://") && !strings.HasPrefix(path, "https://") {
		endpoint = e.BaseURL + path
	}
	if len(q) > 0 {
		sep := "?"
		if strings.Contains(endpoint, "?") {
			sep = "&"
		}
		endpoint += sep + q.Encode()
	}
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, rd)
	if err != nil {
		return nil, err
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if body != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	started := e.Clock.Now()
	resp, err := e.Doer.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 429 || resp.StatusCode == 418 {
		return nil, rateLimitError(req, resp, started, e.Clock.Now(), e.Clock)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d: %.200s", resp.StatusCode, raw)
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("invalid JSON")
	}
	return &Response{Status: resp.StatusCode, Header: resp.Header, Body: raw}, nil
}

// ChannelSpec describes a custom channel for Env.Channel.
type ChannelSpec struct {
	PerSymbol bool
	Daily     bool          // once per UTC day after 00:05
	Period    time.Duration // when not Daily; default 24h
	Weight    float64       // request weight, default 1
	RPS       float64       // default Env.RPS
	Fetch     func(ctx context.Context, symbol string) ([]Record, error)
	Skip      func(symbol string) bool
}

// Channel builds a scheduler channel and applies the venue config: periods[name] replaces Period/Daily,
// weights[name] and channel_rps[name] replace Weight and RPS.
func (e *Env) Channel(name string, s ChannelSpec) Channel {
	c := Channel{Venue: e.Venue, Name: name, PerSymbol: s.PerSymbol, Daily: s.Daily, Period: s.Period, Weight: s.Weight, RPS: s.RPS, Fetch: s.Fetch, Skip: s.Skip}
	if c.Period <= 0 {
		c.Period = 24 * time.Hour
	}
	if c.Weight <= 0 {
		c.Weight = 1
	}
	if c.RPS <= 0 {
		c.RPS = e.RPS
	}
	if w := e.Weights[name]; w > 0 {
		c.Weight = w
	}
	if r := e.ChannelRPS[name]; r > 0 {
		c.RPS = r
	}
	if p := e.Periods[name]; p > 0 {
		c.Period, c.Daily = p, false
	}
	return c
}

// HistorySpec describes a per-symbol history channel; see the package comment.
type HistorySpec struct {
	Step        time.Duration // row granularity (5m, 1m); 0 disables source_gap
	Grace       time.Duration // finalization delay; zero defaults to Step
	Retention   time.Duration // oldest data the venue serves; 0: unlimited
	MaxLookback time.Duration
	Mode        PageMode
	Window      int // PageWindows: rows per window
	PageSize    int // PageForward, PageBackward: rows of a full page
	SkipNoData  bool
	Period      time.Duration // default 24h
	Weight, RPS float64
	// Fetch does one request for [start,end] (ms, inclusive) and returns its rows.
	Fetch func(ctx context.Context, symbol string, start, end int64) ([]Row, error)
}

// HistoryChannel builds a per-symbol history channel on top of History.
func (e *Env) HistoryChannel(name string, s HistorySpec) Channel {
	skip := func(sym string) bool {
		return !s.SkipNoData && e.Scheduler != nil && e.Scheduler.NoDataSkip(e.Venue, name, sym)
	}
	c := e.Channel(name, ChannelSpec{PerSymbol: true, Period: s.Period, Weight: s.Weight, RPS: s.RPS, Skip: skip,
		Fetch: func(ctx context.Context, sym string) ([]Record, error) {
			h := &History{Venue: e.Venue, Channel: name, Symbol: sym, Clock: e.Clock, Sched: e.Scheduler,
				Overlap: e.Overlap, Lookback: e.Lookback, Retention: s.Retention, MaxLookback: s.MaxLookback,
				Step: s.Step, Grace: s.Grace, Mode: s.Mode, Window: s.Window, PageSize: s.PageSize, SkipNoData: s.SkipNoData,
				Fetch: func(ctx context.Context, start, end int64) ([]Row, error) { return s.Fetch(ctx, sym, start, end) }}
			return h.Run(ctx)
		}})
	c.History = true
	return c
}

// SnapshotChannel builds a daily snapshot channel (no dedup); see SnapshotMode.
func (e *Env) SnapshotChannel(name string, s SnapshotSpec) Channel {
	return e.Channel(name, ChannelSpec{PerSymbol: s.Mode == SnapshotPerSymbol, Daily: true,
		Fetch: func(ctx context.Context, symbol string) ([]Record, error) {
			// Resolve after factory construction: NewVenue installs map-based scope
			// after the factory returns; nil means discovery/configuration is pending.
			spec := s
			if spec.Mode == SnapshotSplit {
				spec.Universe = e.Universe
			}
			return SnapshotFetch(e.Venue, name, e.Clock, spec)(ctx, symbol)
		}})
}
