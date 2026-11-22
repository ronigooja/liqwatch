package checks

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
)

type channelRule struct {
	name   string
	maxAge time.Duration
	poll   bool
	bulk   bool // _all only; all other rules search across symbol directories
}

const (
	tradeAge       = 2 * time.Minute
	quoteAge       = 2 * time.Minute
	fundingAge     = 10 * time.Minute
	liquidationAge = 6 * time.Hour
	dailyAge       = 36 * time.Hour
	hourlyAge      = 3 * time.Hour
)

// Disk names and scopes follow the venue adapters and the non-dropped poller
// policies. See docs/storage-design.md for the required sources and cadence.
var requiredChannels = map[string][]channelRule{
	"binance": {
		{"aggTrade", tradeAge, false, false}, {"markPrice_all", quoteAge, false, false}, {"forceOrder_all", liquidationAge, false, false},
		{"brackets", dailyAge, true, false}, {"exchangeInfo", dailyAge, true, true}, {"fundingRate", dailyAge, true, false},
		{"openInterestHist", dailyAge, true, false}, {"globalLongShortAccountRatio", dailyAge, true, false},
		{"topLongShortAccountRatio", dailyAge, true, false}, {"topLongShortPositionRatio", dailyAge, true, false},
	},
	"bybit": {
		{"trade", tradeAge, false, false}, {"mark", quoteAge, false, false}, {"liquidation", liquidationAge, false, false},
		{"accountRatio", dailyAge, true, false}, {"riskLimit", dailyAge, true, true}, {"instruments", dailyAge, true, true},
	},
	"okx": {
		{"trade", tradeAge, false, false}, {"mark", quoteAge, false, false}, {"index", quoteAge, false, false},
		{"funding", fundingAge, false, false}, {"oi", quoteAge, false, false}, {"liquidation", liquidationAge, false, false},
		{"longShortAccountRatio", dailyAge, true, false}, {"longShortAccountRatioContract", dailyAge, true, false},
		{"longShortAccountRatioTopTrader", dailyAge, true, false}, {"longShortPositionRatioTopTrader", dailyAge, true, false},
		{"positionTiers", dailyAge, true, true}, {"instruments", dailyAge, true, true},
	},
	"bitget": {
		{"trade", tradeAge, false, false}, {"ticker", quoteAge, false, false}, {"funding", fundingAge, false, false},
		{"liquidation", liquidationAge, false, false}, {"tier", dailyAge, false, false}, {"positionTiers", dailyAge, true, false},
		{"contracts", dailyAge, true, true}, {"longShort", hourlyAge, true, false}, {"accountLongShort", hourlyAge, true, false},
		{"positionLongShort", hourlyAge, true, false},
	},
	"gate": {
		{"trades", tradeAge, false, false}, {"tickers", quoteAge, false, false}, {"liquidations", liquidationAge, false, false},
		{"contract_stats", fundingAge, false, false}, {"contractStats", dailyAge, true, false}, {"riskLimitTiers", dailyAge, true, false},
		{"contracts", dailyAge, true, true},
	},
	"mexc": {
		{"trade", tradeAge, false, false}, {"mark", quoteAge, false, false}, {"index", quoteAge, false, false},
		{"ticker", quoteAge, false, false}, {"funding", fundingAge, false, false}, {"contractDetail", dailyAge, true, false},
	},
	"kucoin": {
		{"trade", tradeAge, false, false}, {"mark", quoteAge, false, false}, {"funding", fundingAge, false, false},
		{"utamark", quoteAge, false, false}, {"contracts", dailyAge, true, true}, {"riskLimit", dailyAge, true, false},
	},
}

func requiredChannel(venue, channel string) bool {
	for _, rule := range requiredChannels[venue] {
		if rule.name == channel {
			return true
		}
	}
	return false
}

func checkVenues(c *config.Config) []string {
	venues := slices.Clone(c.Checks.Venues)
	if venues == nil {
		for venue := range requiredChannels {
			venues = append(venues, venue)
		}
	}
	slices.Sort(venues)
	return venues
}

func channelFindings(c *config.Config, now time.Time, starts map[string]time.Time, active map[string]bool) ([]finding, error) {
	today := now.UTC().Truncate(24 * time.Hour)
	venues := checkVenues(c)
	ages := make(map[string]map[string]time.Duration, len(venues))
	raw := make(map[string]map[string]time.Time, len(venues))
	var archived map[string]time.Time
	var out []finding
	for _, venue := range venues {
		rules := requiredChannels[venue]
		maxAges := make(map[string]time.Duration, len(rules))
		var horizon time.Duration
		for _, r := range rules {
			age := r.maxAge
			if !r.poll && age < dailyAge {
				age = max(age, 2*c.Record.FlushEvery(5*time.Second))
			}
			if value := c.Checks.ChannelMaxAge[venue][r.name]; value != "" {
				age, _ = time.ParseDuration(value) // validated by config.Load
			}
			maxAges[r.name] = age
			horizon = max(horizon, age)
		}
		newest, err := latestWrites(filepath.Join(c.DataDir, "raw", venue), rules, now, horizon)
		if err != nil {
			return nil, err
		}
		ages[venue], raw[venue] = maxAges, newest
	}
	// Read archive evidence after scanning all venues: removal intent is saved
	// before archival can make any of these raw directories disappear.
	for _, venue := range venues {
		var pollStart time.Time
		var readFloor bool
		for _, r := range requiredChannels[venue] {
			key := "data.stale_write:" + venue + ":" + r.name
			at, limit := raw[venue][r.name], ages[venue][r.name]
			if !at.IsZero() && now.Sub(at) <= limit {
				continue
			}
			if r.poll && !readFloor {
				pollStart = pollRecordingStart(c.DataDir, venue)
				readFloor = true
			}
			if c.Archive.Mode == "local" && c.Archive.DeleteSources && now.Add(-limit).Before(today) {
				if archived == nil {
					var err error
					archived, err = archivedWrites(c, now)
					if err != nil {
						return nil, err
					}
				}
				if prev := archived[key]; prev.After(at) && (!r.poll || !prev.Before(pollStart)) {
					at = prev
				}
			}
			age, missing := now.Sub(at), at.IsZero()
			if (!missing && age <= limit) || (!active[key] && starting(c, venue, r, now, starts[venue], pollStart, missing, limit)) {
				continue
			}
			details := map[string]any{"venue": venue, "channel": r.name, "missing": missing, "max_age_seconds": int(limit.Seconds())}
			problem := "missing"
			if !missing {
				problem = "stale"
				details["age_seconds"] = int(age.Seconds())
			}
			out = append(out, finding{key, "critical", "data.stale_write",
				fmt.Sprintf("%s %s: raw channel is %s", venue, r.name, problem), details})
		}
	}
	return out, nil
}

func archivedWrites(c *config.Config, now time.Time) (map[string]time.Time, error) {
	// Include the same UTC lookback as the raw scanner; only verified original
	// member mtimes count, not pack timestamps or empty successful polls.
	horizon := dailyAge
	for _, overrides := range c.Checks.ChannelMaxAge {
		for _, value := range overrides {
			d, _ := time.ParseDuration(value)
			horizon = max(horizon, d)
		}
	}
	since := now.Add(-horizon).UTC()
	day := since.Year()*10000 + int(since.Month())*100 + since.Day()
	members, err := manifest.LocalDeletionWrites(c.ManifestPath(), day)
	if err != nil {
		return nil, err
	}
	writes := make(map[string]time.Time)
	for _, m := range members {
		name, isPoll, ok := diskChannel(m.Name)
		if !ok {
			continue
		}
		for _, r := range requiredChannels[m.Venue] {
			if matchesChannel(r, m.Symbol, name, isPoll) {
				key := "data.stale_write:" + m.Venue + ":" + r.name
				if at := time.Unix(0, m.MtimeNS); at.After(writes[key]) {
					writes[key] = at
				}
				break
			}
		}
	}
	return writes, nil
}

func diskChannel(name string) (channel string, isPoll, ok bool) {
	stem, ok := strings.CutSuffix(strings.TrimSuffix(name, ".zst"), ".jsonl")
	channel, slot, _ := strings.Cut(stem, ".")
	return channel, slot == "p", ok
}

func matchesChannel(r channelRule, symbol, name string, isPoll bool) bool {
	return r.name == name && symbol != "gap" && r.bulk == (symbol == "_all") && r.poll == isPoll
}

func latestWrites(root string, rules []channelRule, now time.Time, horizon time.Duration) (map[string]time.Time, error) {
	newest := make(map[string]time.Time, len(rules))
	symbols, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return newest, nil
	}
	if err != nil {
		return nil, err
	}
	byName := make(map[string]channelRule, len(rules))
	for _, r := range rules {
		byName[r.name] = r
	}
	today := now.UTC().Truncate(24 * time.Hour)
	for day := today; day.Add(24 * time.Hour).After(now.Add(-horizon)); day = day.AddDate(0, 0, -1) {
		str := day.Format("20060102")
		for _, sym := range symbols {
			if !sym.IsDir() || sym.Name() == "gap" {
				continue
			}
			files, err := os.ReadDir(filepath.Join(root, sym.Name(), str))
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			for _, file := range files {
				name, isPoll, ok := diskChannel(file.Name())
				if !ok || !file.Type().IsRegular() {
					continue
				}
				r, ok := byName[name]
				if !ok || !matchesChannel(r, sym.Name(), name, isPoll) {
					continue
				}
				info, err := file.Info()
				if os.IsNotExist(err) {
					continue
				}
				if err != nil {
					return nil, err
				}
				if info.Size() > 0 && info.ModTime().After(newest[name]) {
					newest[name] = info.ModTime()
				}
			}
		}
	}
	return newest, nil
}
