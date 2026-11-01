# Open Questions and Unverified Facts

Move an item to "Verified" with source and date once checked.

## Inputs needed from the owner

| Question | Affects |
|---|---|
| Identify what already uses ~1.5 GB RAM on the recorder host | [infrastructure](infrastructure.md) |
| Measure sustained chunked upload throughput over several days; re-test a 500 MB single PUT; check nginx temp-file disk use during large uploads | [infrastructure](infrastructure.md) |
| Plan for more storage capacity (storage gateway has ~6 GB free) | [infrastructure](infrastructure.md) |
| Monthly budget for compute and storage | Recording scale |
| Preferred symbols for the depth set (size fixed at 10–20) | [universe](universe.md) |
| Record spot (tier D)? | [data-recording](data-recording.md) |
| Typical position size and holding period | [trading-and-risk](trading-and-risk.md) cost and latency |
| Account fee tier (maker/taker) | [trading-and-risk](trading-and-risk.md) |
| Can the execution host be placed near the exchange? | [trading-and-risk](trading-and-risk.md) latency |

## Unverified facts (check against official sources)

- Still unverified from official futures docs: new-connection rate limit; total REST IP weight limit and ban behavior; default depth-stream update interval when no `@100ms`/`@500ms` suffix is given; `forceOrder` details (Tardis reports the latest liquidation per symbol per 1000 ms); futures `serverShutdown` behavior. The last fetch attempt (general-info and liquidation pages) was declined by the permission system and not retried.
- The spot-doc limits (1024 streams, 5 control messages/s, 300 connections per 5 min per IP, 24 h connection life) are spot only.

- Current Binance WebSocket connection and stream limits; forced-disconnect interval.
- Futures depth stream update intervals and the exact local-book procedure and field names.
- Contents of data.binance.vision futures depth (reported as aggregated only).
- Tardis.dev current pricing, plans, and free-sample policy.
- Legal and terms-of-use status of API access and automated trading in the owner's jurisdiction.

## Verified

| Fact | Source | Date |
|---|---|---|
| Binance servers are in Japan; deploy in Japan | Owner investigation (details not recorded) | 2026-11-01 |
| Futures local-book integrity uses `pu` and `u`; WebSocket has no initial book, snapshot fetched from REST (top 1000 levels stored) | docs.tardis.dev/historical-data-details/binance-futures (secondary source) | 2026-11-01 |
| `forceOrder` publishes only the latest liquidation per symbol per 1000 ms window | same | 2026-11-01 |
| Futures endpoints split into `/public/stream` and `/market/stream` on 2026-03-18 | same | 2026-11-01 |
| Spot local-book procedure and spot limits | github.com/binance/binance-spot-api-docs web-socket-streams.md (spot only) | 2026-11-01 |
| Futures WebSocket: `/public`, `/market`, `/private` routed endpoints; public = bookTicker and depth (`@100ms`/`@500ms` supported); market = aggTrade, markPrice (`@1s`), forceOrder; legacy paths to be removed after 2026-04-23 | developers.binance.com Important-WebSocket-Change-Notice (via Scrapling) | 2026-11-01 |
| Futures connection: 1024 streams max, 10 incoming messages/s, 24 h life, ping every 3 min with 10 min pong deadline | developers.binance.com websocket-market-streams/Connect | 2026-11-01 |
| Futures local-book procedure (U/u/pu rules, snapshot `limit=1000` from `/fapi/v1/depth`) | developers.binance.com How-to-manage-a-local-order-book-correctly | 2026-11-01 |
| REST depth weight: limit 5-50 = 2, 100 = 5, 500 = 10, 1000 = 20; valid limits 5,10,20,50,100,500,1000 | developers.binance.com USDⓈ-M REST market-data (Order Book) | 2026-11-01 |
