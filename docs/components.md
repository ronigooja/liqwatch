# Components

Per-phase component list. Phase definitions: [roadmap.md](roadmap.md). Recording behavior: [data-recording.md](data-recording.md). Hosts: [infrastructure.md](infrastructure.md).

## P0 Data recording (design now)

| # | Component | Responsibility | Design points to settle |
|---|---|---|---|
| 1 | Symbol Manager | Daily USDT-M perpetual list; per-tier subscription lists; log delistings and tier promotions | List format, change log, promotion rule |
| 2 | Stream Collector | Sharded WebSocket per channel and symbol group; reconnect and forced-disconnect handling; receive timestamps | Shard rule, reconnect and de-duplication, backpressure |
| 3 | Snapshot Scheduler | REST snapshots for depth tier: startup sync, resync after gap, periodic verification points | Frequency, rate-limit budget |
| 4 | Raw Log Writer | Bounded queue, append-only hourly files, compression | Record format, file naming, drop priority when full |
| 5 | Book Validator | Side-path book rebuild, gap detection, mismatch vs verification snapshot, unavailable intervals | Rebuild algorithm, mismatch definition, interval format |
| 6 | Archiver | Daily Parquet conversion, upload, verify, then clean raw logs | Partitioning, retention, checksums |
| 7 | Manifest | Per-file start/end, message count, checksum, reconnect count | Table schema |
| 8 | Replay Engine | Replay by receive time; "data received up to t" interface | Interface shape (basis of leakage control) |
| 9 | Monitoring and Alerting | Latency, disconnects, rates, disk, mismatch rate, clock offset; alert push | Metric list, thresholds, channel |
| 10 | Daily Quality Report | Automatic data-quality summary | Report fields |
| 11 | Deployment and Ops | Process supervision, config, secrets, logs, clock sync | Deployment method, config layout |

Flow: 1→2→4, 3→4, 4→5, 4→6→7, 6→8, {2,3,4,5}→9→10.

## P1 Event library and baseline (design in parallel with P0)

| # | Component | Responsibility |
|---|---|---|
| 12 | Volatility Estimator | σ_t from past data only |
| 13 | Event Labeler | k·σ events; ambiguity rule ([labels-and-evaluation.md](labels-and-evaluation.md)) |
| 14 | Event Catalog | Event store with per-tier and per-liquidity-band statistics |
| 15 | Baseline Evaluator | Conditional-frequency baseline and evaluation framework |
| 16 | Fill Simulator | Book-walk fills, latency, limit-order queue, fees ([trading-and-risk.md](trading-and-risk.md)) |

## P2–P4 (design after P1 results)

Feature Engine (one code path for offline and online), Training and Calibration Pipeline, Model Registry and Prediction Log, Online Inference Service, Strategy Engine, Risk Engine (independent of strategy), Execution Gateway (orders, exchange-side stops, reconciliation), Shadow Runner, Dashboard.

Detailed design of components 2, 3, 4, 5, 8: [p0-core-design.md](p0-core-design.md).

## Design order

1. First: 2, 3, 4, 5, 8. They decide whether the data is usable.
2. Next: 1, 6, 7, 9, 11. Needed for stable unattended recording.
3. After recording starts: 10, 12–15.
4. Start early: 16. Its needs (which book detail to keep) can change the record format of component 4.

## Process model (decision)

Collector, writer, and validator are separate processes communicating through a local queue or files, so a validator fault cannot stop recording. Single host first; redundancy later.
