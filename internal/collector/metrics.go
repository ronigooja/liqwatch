package collector

import (
	"sync"
	"sync/atomic"

	"github.com/ronigooja/liqwatch/internal/venue"
)

// ChannelCount reports frames received before Split and records returned by Split,
// before thinning or writing. Connections belonging to the same shard share counters.
type ChannelCount struct {
	Shard, Channel  string
	Frames, Records uint64
}

type channelCounters struct {
	frames, records atomic.Uint64
}

type shardCounters struct {
	channels sync.Map // channel -> *channelCounters
}

func (s *shardCounters) channel(name string) *channelCounters {
	if counter, ok := s.channels.Load(name); ok {
		return counter.(*channelCounters)
	}
	counter, _ := s.channels.LoadOrStore(name, new(channelCounters))
	return counter.(*channelCounters)
}

func newShardCounters(shards []Shard, classifier venue.FrameClassifier) map[string]*shardCounters {
	out := make(map[string]*shardCounters, len(shards))
	for _, sh := range shards {
		s := new(shardCounters)
		out[sh.Name] = s
		s.channel("unclassified")
		if classifier != nil {
			for _, channel := range classifier.FrameChannels(sh) {
				s.channel(channel)
			}
		}
	}
	return out
}

// ChannelCounts returns a snapshot of per-shard counters. Adapters without a
// FrameClassifier report received frames as unclassified.
func (c *Collector) ChannelCounts() []ChannelCount {
	var out []ChannelCount
	for shard, s := range c.shardCounts {
		s.channels.Range(func(channel, value any) bool {
			counter := value.(*channelCounters)
			out = append(out, ChannelCount{shard, channel.(string), counter.frames.Load(), counter.records.Load()})
			return true
		})
	}
	return out
}
