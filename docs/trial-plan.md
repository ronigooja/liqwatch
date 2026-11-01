# Trial Recording Plan

Purpose: measure load and data quality on the recorder host before fixing tier sizes, compression, and queue settings. Host: [infrastructure.md](infrastructure.md). Design under test: [p0-core-design.md](p0-core-design.md), [p0-support-design.md](p0-support-design.md). Not the production collector.

## Questions the trial answers

1. Message rate per channel and per symbol group, in calm and busy periods.
2. CPU, memory, disk write rate, and compressed bytes per hour on 2 vCPU / 4 GB.
3. Which tier A quote option fits: see "Tier A channel choice" in [p0-support-design.md](p0-support-design.md).
4. Depth stream update speed that fits: 100 ms or 250/500 ms.
5. Latency `recv_time − event_time` P50/P99 per channel, and its stability.
6. Disconnect behavior: reconnect count, time to recover, forced 24 h cut.
7. Whether depth can be rebuilt and matches REST snapshots (a rough check; the real validator comes later).
8. Real `exchangeInfo` values for `contractType`, `underlyingType`, and the count of USDT perpetuals.

## Tool

`cmd/trialrec` (Go, one binary). It connects, timestamps on read, writes the raw log envelope from [p0-core-design.md](p0-core-design.md) to disk, and prints periodic statistics. It does no validation, uploads, or alerts. Its own CPU and memory are part of what is measured.

## Runs

| Run | Config | Duration | Compared on |
|---|---|---|---|
| R0 smoke | 3 symbols, all channels | 10 min | It works; envelope correct; files readable |
| R1 option 1 | `@aggTrade` all symbols + `!markPrice@arr@1s`; depth 100 ms for 10 symbols | 2 h | CPU, memory, bytes, rates |
| R2 option 2 | R1 + `@bookTicker` for top 50 symbols by volume | 2 h | Cost of per-symbol quotes |
| R3 option 3 | R1 + `@bookTicker` for all symbols | 2 h | Whether it fits |
| R4 depth speed | Same symbols at 100 ms vs 250 ms, side by side | 1 h each | CPU and bytes difference |
| R5 soak | Best fitting option, 20 depth symbols | 24–48 h | Stability, reconnects, disk growth, forced 24 h cut |

Runs are sequential, one at a time. Short runs repeat in a busy period if the first one was calm; the busy period is judged from price movement in the data, not from the clock.

## Measurements

- Process: CPU %, resident memory, open file descriptors, goroutines, queue depth (sampled every 10 s).
- Host: load average, memory available, disk used, disk write bytes (from `/proc`).
- Per channel: messages per second (mean and peak per second), bytes per second raw and compressed.
- Latency: `recv_wall − event_time` histogram per channel, P50/P99/max. Clock offset from `chronyc tracking` is recorded at start and end.
- Connection events: opens, closes, reason, duration; messages missed (stall detection).
- REST: weight used (from the response header) during snapshots.

Statistics are written every 60 s to `stats.jsonl` next to the data, so results survive a crash.

## Pass criteria for each run

Numbers are proposals, set before the runs and not tuned afterwards.

| Item | Criterion |
|---|---|
| Collector CPU | Mean under 50% of one core; peak second under 150% of one core (of 200% total) |
| Host memory | Available memory stays above 1.5 GB |
| Queue | Never above 50% full; zero drops |
| Disk | Projected growth for 7 days fits within free space minus 10 GB, or the upload plan covers it |
| Latency | P99 `recv − event` below 500 ms in calm periods; report busy-period figures without a fixed bar |
| Reconnects | Each recovers within 10 s and is logged as a gap |

An option that fails a row is rejected for the 2 vCPU host or its scope is reduced (fewer symbols). The results table goes into this file under "Results".

## Safety

- Trial data lives in `/var/lib/wicker/trial/`; it is not part of the production archive and is deleted after analysis, or uploaded to the storage account if the owner wants to keep it.
- Disk: the tool stops writing at 80% disk use and logs it.
- REST: one snapshot per depth symbol at start, then at most one per symbol per 10 minutes. Backs off on HTTP 429 and never retries in a tight loop.
- WebSocket: connections opened gradually; at most 100 streams per connection; control messages well below 10 per second.
- No order or account endpoints are used; no API keys are needed.

## Results

Not run yet.
