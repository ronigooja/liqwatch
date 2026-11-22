package checks

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/ronigooja/liqwatch/internal/config"
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

func channelFindings(c *config.Config, now time.Time, daily map[string]time.Time, dailyChanged *bool) ([]finding, error) {
	venues := slices.Clone(c.Checks.Venues)
	if venues == nil {
		for venue := range requiredChannels {
			venues = append(venues, venue)
		}
	}
	slices.Sort(venues)
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
		for _, r := range rules {
			key := "data.stale_write:" + venue + ":" + r.name
			at, limit := newest[r.name], maxAges[r.name]
			// Local archival removes yesterday's raw files before some daily jobs
			// write today's files. Carry forward observed writes, never pack mtimes.
			if c.Archive.Mode == "local" && c.Archive.DeleteSources && r.poll && r.maxAge == dailyAge && daily != nil {
				if prev := daily[key]; at.After(prev) {
					daily[key] = at
					*dailyChanged = true
				} else if prev.Before(now.UTC().Truncate(24 * time.Hour)) {
					at = prev
				}
			}
			age, missing := now.Sub(at), at.IsZero()
			if !missing && age <= limit {
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
				stem, ok := strings.CutSuffix(strings.TrimSuffix(file.Name(), ".zst"), ".jsonl")
				if !ok || !file.Type().IsRegular() {
					continue
				}
				name, slot, _ := strings.Cut(stem, ".")
				r, ok := byName[name]
				if !ok || r.bulk != (sym.Name() == "_all") || r.poll != (slot == "p") {
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
