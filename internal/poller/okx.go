package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

func init() { RegisterVenue("okx", okxFactory, WithBaseURL("https://www.okx.com")) }

func okxCurrency(s string) string { return strings.SplitN(s, "-", 2)[0] }
func okxFamily(s string) string   { return strings.TrimSuffix(s, "-SWAP") }

type okxEnvelope struct {
	Code string          `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type okxError struct{ code, message string }

func (err *okxError) Error() string { return fmt.Sprintf("OKX code %s: %s", err.code, err.message) }

func okxGet(ctx context.Context, e *Env, path string, q url.Values) ([]byte, error) {
	r, err := e.Get(ctx, path, q)
	if err != nil {
		return nil, err
	}
	var v okxEnvelope
	if err = json.Unmarshal(r.Body, &v); err != nil {
		return nil, err
	}
	if v.Code != "0" {
		return nil, &okxError{v.Code, v.Msg}
	}
	if len(v.Data) == 0 || string(v.Data) == "null" {
		return nil, fmt.Errorf("OKX missing data")
	}
	return r.Body, nil
}
func okxRows(ctx context.Context, e *Env, path, symbol string, start, end int64, currency bool) ([]Row, error) {
	q := url.Values{"period": {"5m"}, "begin": {strconv.FormatInt(start, 10)}, "end": {strconv.FormatInt(end, 10)}, "limit": {"100"}}
	if currency {
		q.Set("ccy", symbol)
	} else {
		q.Set("instId", symbol)
		q.Set("end", strconv.FormatInt(end+1, 10))
	}
	raw, err := okxGet(ctx, e, path, q)
	if err != nil {
		return nil, err
	}
	var v okxEnvelope
	if err = json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	var data []json.RawMessage
	if err = json.Unmarshal(v.Data, &data); err != nil {
		return nil, err
	}
	rows := make([]Row, 0, len(data))
	for _, r := range data {
		var fields []json.RawMessage
		var ts string
		if json.Unmarshal(r, &fields) != nil || len(fields) == 0 || json.Unmarshal(fields[0], &ts) != nil {
			return nil, fmt.Errorf("OKX invalid timestamp")
		}
		n, err := strconv.ParseInt(ts, 10, 64)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("OKX invalid timestamp %q", ts)
		}
		if n >= start && n <= end {
			rows = append(rows, Row{TS: n, Raw: r})
		}
	}
	return rows, nil
}
func okxFactory(e *Env) []Channel {
	if e.Config.Venues["okx"].RPS <= 0 {
		e.RPS = 2
	}
	paths := []struct{ name, path string }{
		{"longShortAccountRatioContract", "/api/v5/rubik/stat/contracts/long-short-account-ratio-contract"},
		{"longShortAccountRatioTopTrader", "/api/v5/rubik/stat/contracts/long-short-account-ratio-contract-top-trader"},
		{"longShortPositionRatioTopTrader", "/api/v5/rubik/stat/contracts/long-short-position-ratio-contract-top-trader"},
		{"takerVolumeContract", "/api/v5/rubik/stat/taker-volume-contract"},
	}
	out := []Channel{}
	for _, p := range paths {
		p := p
		out = append(out, e.HistoryChannel(p.name, HistorySpec{Step: 5 * time.Minute, Retention: 5 * 24 * time.Hour, Mode: PageBackward, PageSize: 100, Period: 24 * time.Hour, Fetch: func(ctx context.Context, s string, a, b int64) ([]Row, error) {
			return okxRows(ctx, e, p.path, s, a, b, false)
		}}))
	}
	const name = "longShortAccountRatio"
	c := e.HistoryChannel(name, HistorySpec{Step: 5 * time.Minute, Retention: 2 * 24 * time.Hour, Mode: PageWindows, Window: 288, Period: 24 * time.Hour, Fetch: func(ctx context.Context, s string, a, b int64) ([]Row, error) {
		rows, err := okxRows(ctx, e, "/api/v5/rubik/stat/contracts/long-short-account-ratio", okxCurrency(s), a, b, true)
		// Only this currency endpoint's explicit unknown-token response is unsupported.
		// Keep the instrument in all other channels and recheck after the persisted cooldown.
		if apiErr, ok := err.(*okxError); ok && apiErr.code == "51012" && e.Scheduler != nil {
			now := e.Clock.Now()
			if e.Scheduler.Sink != nil {
				payload, _ := json.Marshal(map[string]any{"event": "unsupported", "channel": name, "symbol": s, "ccy": okxCurrency(s), "code": apiErr.code, "err": apiErr.Error()})
				e.Scheduler.Sink.Write(Record{Venue: e.Venue, Channel: "gap", Symbol: s, RecvWall: now.UnixNano(), RecvMono: time.Since(monoOrigin).Nanoseconds(), Payload: payload})
			}
			e.Scheduler.MarkNoData(e.Venue, name, s, now)
			return nil, nil
		}
		if apiErr, ok := err.(*okxError); ok && apiErr.code == "50030" {
			if e.Scheduler != nil {
				e.Scheduler.MarkNoData(e.Venue, name, s, e.Clock.Now())
			}
			return nil, nil
		}
		return rows, err
	}})
	origSkip, origFetch := c.Skip, c.Fetch
	c.Skip = func(s string) bool {
		if e.Scheduler != nil {
			at := e.Scheduler.NoDataAt(e.Venue, name, s)
			if at > 0 && e.Clock.Now().Sub(time.Unix(0, at)) < 24*time.Hour {
				return true
			}
		}
		if origSkip != nil && origSkip(s) {
			return true
		}
		for _, v := range e.Universe() {
			if okxCurrency(v) == okxCurrency(s) && v < s {
				return true
			}
		}
		return false
	}
	c.Fetch = func(ctx context.Context, s string) ([]Record, error) {
		r, err := origFetch(ctx, s)
		for i := range r {
			r[i].Symbol = okxCurrency(s)
		}
		return r, err
	}
	out = append(out, c)
	out = append(out, e.SnapshotChannel("instruments", SnapshotSpec{Mode: SnapshotBulk, Fetch: func(ctx context.Context, _ string) ([]byte, error) {
		return okxGet(ctx, e, "/api/v5/public/instruments", url.Values{"instType": {"SWAP"}})
	}}))
	out = append(out, e.Channel("positionTiers", ChannelSpec{Daily: true, Fetch: func(ctx context.Context, _ string) ([]Record, error) {
		set := map[string]bool{}
		for _, s := range e.Universe() {
			set[okxFamily(s)] = true
		}
		families := make([]string, 0, len(set))
		for s := range set {
			families = append(families, s)
		}
		sort.Strings(families)
		records := []Record{}
		first := true
		for _, mode := range []string{"cross", "isolated"} {
			for i := 0; i < len(families); i += 5 {
				if !first && e.Scheduler != nil {
					if err := e.Scheduler.waitRequest(ctx, e.Venue, "positionTiers"); err != nil {
						return nil, err
					}
				}
				first = false
				j := min(i+5, len(families))
				q := url.Values{"instType": {"SWAP"}, "tdMode": {mode}, "instFamily": {strings.Join(families[i:j], ",")}}
				raw, err := okxGet(ctx, e, "/api/v5/public/position-tiers", q)
				if err != nil {
					return nil, err
				}
				records = append(records, Record{Venue: e.Venue, Channel: "positionTiers", RecvWall: e.Clock.Now().UnixNano(), RecvMono: time.Since(monoOrigin).Nanoseconds(), Meta: map[string]any{"snapshot": true, "tdMode": mode, "instFamily": families[i:j]}, Payload: raw})
			}
		}
		return records, nil
	}}))
	return out
}
