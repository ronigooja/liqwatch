// Package binance implements the Binance futures venue adapter.
package binance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ronigooja/liqwatch/internal/rawlog"
	"github.com/ronigooja/liqwatch/internal/symbols"
	"github.com/ronigooja/liqwatch/internal/venue"
)

const WSMarket = "wss://fstream.binance.com/market/stream?streams="

// Adapter carries Binance configuration and the discovered universe.
type Adapter struct {
	StateDir          string
	MaxStreamsPerWS   int
	WeightPerMinLimit int
	universe          map[string]struct{}
	unknown           atomic.Uint64
}

// UnknownFields returns the number of unrecognized data fields retained.
func (a *Adapter) UnknownFields() uint64 { return a.unknown.Load() }

func init() { venue.Register("binance", func() venue.Adapter { return &Adapter{} }) }

func (*Adapter) Name() string { return "binance" }

func (a *Adapter) Symbols(ctx context.Context) ([]venue.Symbol, error) {
	u, err := symbols.Load(ctx, a.StateDir)
	if err != nil {
		return nil, err
	}
	a.WeightPerMinLimit = u.WeightPerMinLimit
	return u.All, nil
}

func (a *Adapter) Shards(_ context.Context, syms []venue.Symbol) ([]venue.Shard, error) {
	a.universe = make(map[string]struct{}, len(syms))
	var agg []string
	for _, s := range syms {
		a.universe[s] = struct{}{}
		agg = append(agg, strings.ToLower(s)+"@aggTrade")
	}
	perWS := a.MaxStreamsPerWS
	if perWS <= 0 {
		perWS = 100
	}
	var out []venue.Shard
	for i := 0; i*perWS < len(agg); i++ {
		streams := agg[i*perWS : min((i+1)*perWS, len(agg))]
		out = append(out, venue.Shard{Name: fmt.Sprintf("trade%02d", i), URL: WSMarket + strings.Join(streams, "/"), Streams: streams, Stall: 60 * time.Second})
	}
	streams := []string{"!markPrice@arr@1s", "!forceOrder@arr"}
	out = append(out, venue.Shard{Name: "misc", URL: WSMarket + strings.Join(streams, "/"), Streams: streams, Stall: 30 * time.Second})
	return out, nil
}

func (*Adapter) Subscribe(venue.Shard) [][]byte                              { return nil }
func (*Adapter) Heartbeat(venue.Shard) (time.Duration, func() (int, []byte)) { return 0, nil }

func (a *Adapter) Split(_ venue.Shard, msg []byte) []rawlog.Record {
	ch, sym := classify(msg)
	if ch == "markPrice_all" || ch == "forceOrder_all" {
		return a.splitAllMarket(msg, a.universe)
	}
	if ch == "unknown" || sym == "" {
		return unrecognized(msg)
	}
	if ch == "aggTrade" {
		var frame map[string]json.RawMessage
		if json.Unmarshal(msg, &frame) != nil {
			return unrecognized(msg)
		}
		payload, err := a.filteredPayload(ch, frame["data"])
		if err != nil {
			return unrecognized(msg)
		}
		delete(frame, "stream")
		var filtered map[string]json.RawMessage
		if json.Unmarshal(payload, &filtered) != nil {
			return unrecognized(msg)
		}
		frame["data"] = filtered["data"]
		payload, err = json.Marshal(frame)
		if err != nil {
			return unrecognized(msg)
		}
		return []rawlog.Record{{Channel: ch, Symbol: sym, Payload: payload}}
	}
	return []rawlog.Record{{Channel: ch, Symbol: sym, Payload: msg}}
}

// unrecognized keeps a trace of a data frame this adapter cannot classify, so a new stream type is
// not lost silently. Control frames (subscribe acknowledgements, errors without data) return nil.
func unrecognized(msg []byte) []rawlog.Record {
	if !bytes.Contains(msg, []byte(`"data"`)) {
		return nil
	}
	p, _ := json.Marshal(map[string]any{"event": "unrecognized_frame", "bytes": len(msg), "head": string(msg[:min(len(msg), 200)])})
	return []rawlog.Record{{Channel: "gap", Payload: p}}
}

func (*Adapter) Classes() func(string) (string, bool) {
	return func(ch string) (string, bool) {
		switch ch {
		case "markPrice_all":
			return "mark", true
		case "aggTrade":
			return "trade", true
		case "bookTicker":
			return "quote", true
		}
		return "", false
	}
}

func (*Adapter) FrameChannel(msg []byte) string {
	return frameChannel(streamName(msg))
}

func (*Adapter) FrameChannels(sh venue.Shard) []string {
	var channels []string
	for _, stream := range sh.Streams {
		channels = append(channels, frameChannel(stream))
	}
	return channels
}

func frameChannel(stream string) string {
	switch stream {
	case "!markPrice@arr@1s":
		return "markPrice_all"
	case "!forceOrder@arr":
		return "forceOrder_all"
	}
	if _, channel, ok := strings.Cut(stream, "@"); ok && channel == "aggTrade" {
		return "aggTrade"
	}
	return "unclassified"
}

func classify(data []byte) (ch, sym string) {
	name := streamName(data)
	if name == "" {
		return "unknown", ""
	}
	k := strings.IndexByte(name, '@')
	if k < 0 {
		return name, ""
	}
	ch = name[k+1:]
	if m := strings.IndexByte(ch, '@'); m >= 0 {
		ch = ch[:m]
	}
	if k > 0 && name[0] == '!' {
		return name[1:k] + "_all", ""
	}
	return ch, strings.ToUpper(name[:k])
}

func streamName(data []byte) string {
	i := bytes.Index(data, []byte(`"stream":"`))
	if i < 0 {
		return ""
	}
	rest := data[i+10:]
	j := bytes.IndexByte(rest, '"')
	if j < 0 {
		return ""
	}
	return string(rest[:j])
}
