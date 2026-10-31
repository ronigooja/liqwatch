# Open Questions and Unverified Facts

Move an item to "Verified" with source and date once checked.

## Inputs needed from the owner

| Question | Affects |
|---|---|
| Cloud provider, candidate regions, existing servers | [data-recording](data-recording.md) region selection |
| Monthly budget for compute and storage | Recording scale |
| Preferred symbols for the depth set | [universe](universe.md) |
| Record spot (tier D)? | [data-recording](data-recording.md) |
| Typical position size and holding period | [trading-and-risk](trading-and-risk.md) cost and latency |
| Account fee tier (maker/taker) | [trading-and-risk](trading-and-risk.md) |
| Can the execution host be placed near the exchange? | [trading-and-risk](trading-and-risk.md) latency |

## Unverified facts (check against official sources)

- Current Binance WebSocket connection and stream limits; forced-disconnect interval.
- Futures depth stream update intervals and the exact local-book procedure and field names.
- Contents of data.binance.vision futures depth (reported as aggregated only).
- Tardis.dev current pricing, plans, and free-sample policy.
- Network latency from candidate cloud regions to the exchange.
- Legal and terms-of-use status of API access and automated trading in the owner's jurisdiction.

## Verified

None yet.
