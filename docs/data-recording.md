# Data Recording (P0)

Goal: a complete, auditable, replayable raw market record. No models or features in this phase.

## Why self-record

Binance provides no full historical incremental L2. Public archives at data.binance.vision give klines and trades; depth there is reported as aggregated only (unverified, see [open-questions.md](open-questions.md)). Purchasable alternatives exist (e.g. Tardis.dev) and may be added later to extend history.

## Recording tiers

| Tier | Symbols | Channels |
|---|---|---|
| A light | All USDT-M perpetuals (refresh list daily) | aggregate trades, mark price; best bid/ask per [p0-support-design.md](p0-support-design.md) (all-market book ticker is only 5 s) |
| B depth | 10–20 symbols per [universe.md](universe.md) (expand only after cost-adjusted profit is shown) | incremental depth (update speed 100 ms; 250 ms and 500 ms are the alternatives) plus periodic REST snapshots |
| C events | All | liquidation orders |
| D spot | Spot pairs of tier B | aggregate trades, best bid/ask; depth optional |

Tier A exists so that any spike can be reviewed afterwards, including on symbols not preselected. Channel names and parameters must be checked against current official docs before implementation.

## Pipeline

```
WebSocket shards -> bounded write queue -> append-only raw log
REST snapshot scheduler -> same queue
Side-path validator: rebuild book, detect gaps, trigger resync
Metrics: latency, reconnects, gaps, queue depth
```

- Shard connections by channel and symbol group; keep per-connection stream counts well below the exchange limit.
- Take wall-clock and monotonic receive time immediately on read, before parsing.
- Handle forced reconnects by opening the replacement connection early and de-duplicating by sequence.
- Never repair data inside the recording path. Record gaps as unavailable intervals.

## Record fields

venue, symbol, channel, connection id, local sequence, `recv_wall_time`, `recv_mono_time`, exchange event time (if present), raw payload, parser version.

## Order book integrity

- Follow the exchange's documented local-book procedure (snapshot plus sequenced diffs).
- On a sequence break: log it, fetch a new snapshot, mark the interval unavailable.
- Fetch a verification snapshot every 5–10 minutes and compare to the rebuilt book. Mismatch rate is the core quality metric.

## Storage

- Hot: hourly append-only files by channel/date/symbol, zstd.
- Archive: the recorder uploads the closed hourly `jsonl.zst` files in chunks and verifies them; Parquet conversion happens on the research side, not on the recorder ([p0-support-design.md](p0-support-design.md)). Keep raw files locally until verified, and at least 3 days.
- Manifest table (DuckDB or SQLite): per file start/end time, message count, checksum, reconnect count.
- Size capacity from a 1–2 day trial; do not extrapolate from symbol count.

## Clock and region

- Chrony/NTP on all hosts; monitor offset; flag records when offset exceeds threshold.
- Region: Japan (Tokyo), where the exchange matching infrastructure is located. See [decision 0005](decisions/0005-japan-region.md).
- No multi-region comparison. During the 1–2 day trial, measure `recv_time − event_time` P50/P99, jitter, and disconnects as a baseline for later monitoring.

## Monitoring

Per-channel message rate; `recv_time − event_time` P50/P99; reconnects; sequence breaks; snapshot refetches; unavailable-interval total; queue backlog; disk usage; book mismatch rate; clock offset. Alert on stream stall, disk nearly full, mismatch spike.

## Risks

| Risk | Mitigation |
|---|---|
| Rate or connection limits change | Check current docs; shard; keep headroom |
| Forced disconnects | Early replacement connection |
| Message bursts in volatility | Decouple write from validator; prioritize writing; load test |
| Disk full or write stall | Bounded queue, alerts, drop low-priority channels first and log it |
| Symbol delist or rename | Daily list refresh, logged |
| Terms of use or regional access | Confirm before running |

## Acceptance (P0 gate)

1. 7 consecutive days without manual intervention.
2. Valid-data coverage at or above the target set after the trial (proposed ≥ 99%, excluding marked gaps).
3. Replayed book matches verification snapshots on random samples.
4. A known large move can be fully reconstructed: trades, book, liquidations.
5. Replay API returns only data received up to a given time t, ordered by receive time.
6. Automatic daily data-quality report.
