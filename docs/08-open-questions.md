# Open Questions and Unverified Facts

Move an item to "Verified" with source and date once checked.

## Inputs needed from the owner

| Question | Affects |
|---|---|
| Cloud provider, candidate regions, existing servers | [02](02-data-recording.md) region selection |
| Monthly budget for compute and storage | Recording scale |
| Preferred symbols for the depth set | [03](03-universe.md) |
| Record spot (tier D)? | [02](02-data-recording.md) |
| Typical position size and holding period | [06](06-trading-and-risk.md) cost and latency |
| Account fee tier (maker/taker) | [06](06-trading-and-risk.md) |
| Can the execution host be placed near the exchange? | [06](06-trading-and-risk.md) latency |

## Unverified facts (check against official sources)

- Current Binance WebSocket connection and stream limits; forced-disconnect interval.
- Futures depth stream update intervals and the exact local-book procedure and field names.
- Contents of data.binance.vision futures depth (reported as aggregated only).
- Tardis.dev current pricing, plans, and free-sample policy.
- Network latency from candidate cloud regions to the exchange.
- Legal and terms-of-use status of API access and automated trading in the owner's jurisdiction.

## Verified

None yet.
