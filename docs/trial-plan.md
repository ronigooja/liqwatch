# Trial Recording Plan

Historical trial and results: quote tests do not establish a current recording requirement; see [decision 0011](decisions/0011-no-alert-narrower-recording.md). Recorder status: [p0-rollout.md](p0-rollout.md).

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
| R4 depth speed | Depth only, 10 symbols, 100 ms vs 250 ms, side by side | 5 min | CPU and bytes difference (250 ms run was invalid, see Results) |
| R5 soak | Dropped as a separate run; merged into the production collector's first two days ([decision 0007](decisions/0007-trial-outcome.md)) | n/a | Stability, reconnects, disk growth, forced 24 h cut |

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

Host: 2 vCPU / 4 GB, Japan. Trial binary built on GitHub Actions. R1–R3: 2 h each, 522 symbols `aggTrade`, `!markPrice@arr@1s`, `!forceOrder@arr`, 10 symbols depth at 100 ms (symbols are trial picks, not the final set). R0 smoke test passed.

| Item | R1 (no quotes) | R2 (`bookTicker` top 50) | R3 (`bookTicker` all 522) |
|---|---|---|---|
| CPU mean / peak (% of one core) | 4.1 / 15.3 | 18.3 / 53.0 | 27.5 / 52.6 |
| Process RSS | 187 MB | 282 MB | 273 MB |
| Min host available memory | 3331 MB | 3197 MB | 3223 MB |
| Drops / queue peak | 0 / 198 | 0 / 1100 | 0 / 1061 |
| Reconnects | 0 | 1 | 2 |
| `bookTicker` peak msgs/s | n/a | 24089 | 68717 |
| `bookTicker` latency P99 worst / max | n/a | 65 / 87 ms | 340 / 493 ms |
| `aggTrade` peak msgs/s | 5954 | 5489 | 9849 |
| Raw / compressed bytes | 2.6 GB / ~330 MB on disk | 14.3 GB / 1.5 GB | 18.3 GB / 2.0 GB |
| Compression ratio | ~8x | 9.3x | 8.9x |
| Disk per day (projected) | ~4 GB | ~18 GB | ~24 GB |
| REST weight peak per minute | 241 | 241 | 201 |

- All three runs pass every row of the pass criteria. R3 `bookTicker` latency is within the 500 ms bar only narrowly (max 493 ms in a busy period; no fixed bar was set for busy periods).
- `aggTrade`, depth, `forceOrder` P99 latency stayed at or below 27 ms in all runs; `markPrice` shows 69–100 ms because the stream stamps events inside the second.
- The "44x" compression figure in the first R1 stats was a counter bug (only open files were counted), fixed in commit `2e63197`. The R1 stats file keeps the wrong figure; the on-disk size is the correct one.
- Reconnects: R2 one at startup after 32 s (`pub00`); R3 two on `pub02` after 18 min and 1 h 23 min. Each reconnected within 1 s and wrote a `gap` record.
- R4: the 100 ms run (10 symbols, 5 min) used 1.6% CPU, 66 MB RSS, 0 drops, P99 6 ms, 34 MB raw and 8.8 MB compressed, so roughly 100 MB per day compressed. The 250 ms run received nothing: the stream name `@depth@250ms` that the tool built likely is not a valid name (the documented default is `@depth` without suffix; not yet verified). Depth is small next to quotes, so the speed choice is made on precision, not cost (100 ms).
- Unmeasured: forced 24 h disconnect, multi-day stability, daily upload and cleanup. These move into the production collector's observation period.
