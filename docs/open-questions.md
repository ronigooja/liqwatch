# Open Questions and Unverified Facts

Move an item to "Verified" with source and date once checked.

## Inputs needed from the owner

| Question | Affects |
|---|---|
| Identify what already uses ~1.5 GB RAM on the recorder host | [infrastructure](infrastructure.md) |
| Why WebDAV upload is slow and a 500 MB PUT stalls; measure chunked upload throughput over several days | [infrastructure](infrastructure.md) |
| Plan for more storage capacity (storage gateway has ~6 GB free) | [infrastructure](infrastructure.md) |
| Monthly budget for compute and storage | Recording scale |
| Preferred symbols for the depth set (size fixed at 10–20) | [universe](universe.md) |
| Record spot (tier D)? | [data-recording](data-recording.md) |
| Typical position size and holding period | [trading-and-risk](trading-and-risk.md) cost and latency |
| Account fee tier (maker/taker) | [trading-and-risk](trading-and-risk.md) |
| Can the execution host be placed near the exchange? | [trading-and-risk](trading-and-risk.md) latency |

## Unverified facts (check against official sources)

- Current Binance WebSocket connection and stream limits; forced-disconnect interval.
- Futures depth stream update intervals and the exact local-book procedure and field names.
- Contents of data.binance.vision futures depth (reported as aggregated only).
- Tardis.dev current pricing, plans, and free-sample policy.
- Legal and terms-of-use status of API access and automated trading in the owner's jurisdiction.

## Verified

| Fact | Source | Date |
|---|---|---|
| Binance servers are in Japan; deploy in Japan | Owner investigation (details not recorded) | 2026-11-01 |
