package checks

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/poller"
)

const recorderGrace = 5 * time.Minute

// recorderStarts uses systemd's per-venue process start, not the time the check
// first ran. Non-systemd installs without this evidence check immediately.
func recorderStarts(venues []string) map[string]time.Time {
	if len(venues) == 0 {
		return nil
	}
	args := []string{"show", "--timestamp=unix", "-p", "Id", "-p", "ExecMainStartTimestamp"}
	for _, venue := range venues {
		args = append(args, "liqwatch-recorder@"+venue+".service")
	}
	// systemctl may return nonzero for a missing instance alongside valid ones.
	cmd := exec.Command("systemctl", args...)
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	out, _ := cmd.Output()
	return parseRecorderStarts(string(out))
}

func parseRecorderStarts(output string) map[string]time.Time {
	starts := make(map[string]time.Time)
	for _, unit := range strings.Split(output, "\n\n") {
		var venue string
		var sec int64
		for _, line := range strings.Split(unit, "\n") {
			if id, ok := strings.CutPrefix(line, "Id=liqwatch-recorder@"); ok {
				venue, _ = strings.CutSuffix(id, ".service")
			}
			if ts, ok := strings.CutPrefix(line, "ExecMainStartTimestamp=@"); ok {
				sec, _ = strconv.ParseInt(ts, 10, 64)
			}
		}
		if venue != "" && sec > 0 {
			starts[venue] = time.Unix(sec, 0)
		}
	}
	return starts
}

func pollRecordingStart(dataDir, venue string) time.Time {
	var state struct {
		RecordingStartMS int64 `json:"recording_start_ms"`
	}
	b, err := os.ReadFile(poller.StatePath(filepath.Join(dataDir, "raw"), venue))
	if err != nil || json.Unmarshal(b, &state) != nil || state.RecordingStartMS <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(state.RecordingStartMS)
}

func starting(c *config.Config, venue string, r channelRule, now, recorderStart, pollStart time.Time, missing bool, limit time.Duration) bool {
	if !r.poll {
		grace := max(recorderGrace, 2*c.Record.FlushEvery(5*time.Second))
		if missing && r.maxAge == liquidationAge {
			grace = max(grace, limit) // a healthy event stream need not write at startup
		}
		return inStartup(now, recorderStart, grace)
	}
	if !missing {
		return false // Once a channel has written, enforce its normal age limit.
	}
	if pollStart.IsZero() {
		return inStartup(now, recorderStart, recorderGrace) // allow the poller to create its state
	}
	return inStartup(now, pollStart, pollStartupWindow(c, venue, r))
}

func inStartup(now, start time.Time, grace time.Duration) bool {
	return !start.IsZero() && !now.Before(start) && now.Before(start.Add(grace))
}

// The aggregate metrics alert waits while any checked recorder is starting.
// Use the realtime-channel window, including the configured flush allowance.
func recordersStarting(c *config.Config, now time.Time, starts map[string]time.Time) bool {
	for _, venue := range checkVenues(c) {
		if starting(c, venue, channelRule{}, now, starts[venue], time.Time{}, true, 0) {
			return true
		}
	}
	return false
}

func pollStartupWindow(c *config.Config, venue string, r channelRule) time.Duration {
	period := 24 * time.Hour
	if r.maxAge == hourlyAge {
		period = time.Hour
	}
	value := c.Poller.Venues[venue].Periods[r.name]
	if venue == "binance" && value == "" {
		value = c.Poller.Periods[r.name]
	}
	if d, err := time.ParseDuration(value); err == nil && d > 0 {
		period = d
	}
	// First snapshots are immediately due but share the venue request budget.
	// Histories can wait a full phase, and early polls may find no finalized
	// bucket after the new recording floor.
	switch venue + "/" + r.name {
	case "binance/fundingRate":
		return period + 12*time.Hour // allow sparse settlements plus pacing
	case "bitget/accountLongShort", "bitget/longShort", "bitget/positionLongShort":
		return period + 30*time.Minute
	case "binance/openInterestHist", "binance/globalLongShortAccountRatio", "binance/topLongShortAccountRatio", "binance/topLongShortPositionRatio",
		"bybit/accountRatio", "gate/contractStats", "okx/longShortAccountRatio", "okx/longShortAccountRatioContract",
		"okx/longShortAccountRatioTopTrader", "okx/longShortPositionRatioTopTrader":
		return period + 2*time.Hour
	default:
		return 15 * time.Minute
	}
}
