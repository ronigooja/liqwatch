# 0007 Trial outcome: tier A quotes and soak test

Date: 2026-11-03. Evidence: Results in [trial-plan.md](../trial-plan.md).

## Decision

1. Tier A records `@aggTrade` for all symbols, `!markPrice@arr@1s`, `!forceOrder@arr`, plus per-symbol `@bookTicker` for the top 50 symbols by volume (option 2 in [p0-support-design.md](../p0-support-design.md)).
2. Tier B depth uses the 100 ms diff stream.
3. The separate 24–48 h soak run (R5) is dropped. The production collector's first two days are the observation period for the forced 24 h disconnect, memory and descriptor growth, and the daily upload and cleanup.

## Why

- Option 3 (all symbols) also fit the host but costs ~24 GB/day against ~18 GB/day, with `bookTicker` P99 up to 340 ms versus 65 ms.
- Local free disk is ~21 GB, so daily upload and cleanup is mandatory either way.
- Depth cost is small, so speed is chosen for precision.
- A separate soak adds little over a monitored production start; the cost is that early problems appear in real data with gaps, which the owner accepted.

## Revisit

Widen `bookTicker` beyond 50 only if research shows a need and disk and upload capacity allow.
