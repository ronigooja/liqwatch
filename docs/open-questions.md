# Open Questions and Unverified Facts

Move an item to "Verified" with source and date once checked.

## Inputs needed from the owner

| Question | Affects |
|---|---|
| Address and access for the new 2C4G recorder host (owner provisioning); then re-measure latency to Binance | [infrastructure](infrastructure.md) |
| Measure sustained chunked upload throughput over several days; re-test a 500 MB single PUT; check nginx temp-file disk use during large uploads | [infrastructure](infrastructure.md) |
| Plan for more storage capacity (storage gateway has ~6 GB free) | [infrastructure](infrastructure.md) |
| Monthly budget for compute and storage | Recording scale |
| Preferred symbols for the depth set (size fixed at 10–20) | [universe](universe.md) |
| Record spot (tier D)? | [data-recording](data-recording.md) |
| Typical position size and holding period | [trading-and-risk](trading-and-risk.md) cost and latency |
| Account fee tier (maker/taker) | [trading-and-risk](trading-and-risk.md) |
| Can the execution host be placed near the exchange? | [trading-and-risk](trading-and-risk.md) latency |

## Unverified facts (check against official sources)

- Still unverified: new-connection rate limit for futures WebSocket; the numeric REST IP weight limit (read from `exchangeInfo` `rateLimits` at runtime); load per depth update speed on the 2 vCPU host (measure in the trial).
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
| Spot local-book procedure and spot limits | github.com/binance/binance-spot-api-docs web-socket-streams.md (spot only) | 2026-11-01 |
| Futures WebSocket: `/public`, `/market`, `/private` routed endpoints; public = bookTicker and depth (`@100ms`/`@500ms` supported); market = aggTrade, markPrice (`@1s`), forceOrder; legacy paths to be removed after 2026-04-23 | developers.binance.com Important-WebSocket-Change-Notice (via Scrapling) | 2026-11-01 |
| Futures connection: 1024 streams max, 10 incoming messages/s, 24 h life, ping every 3 min with 10 min pong deadline | developers.binance.com websocket-market-streams/Connect | 2026-11-01 |
| Futures local-book procedure (U/u/pu rules, snapshot `limit=1000` from `/fapi/v1/depth`) | developers.binance.com How-to-manage-a-local-order-book-correctly | 2026-11-01 |
| REST depth weight: limit 5-50 = 2, 100 = 5, 500 = 10, 1000 = 20; valid limits 5,10,20,50,100,500,1000 | developers.binance.com USDⓈ-M REST market-data (Order Book) | 2026-11-01 |
| Futures diff depth update speeds: 250 ms (default), 500 ms, 100 ms | developers.binance.com ws-streams/public (Diff. Book Depth Streams) | 2026-11-01 |
| Liquidations: `!forceOrder@arr` on `/market`, per symbol only the latest liquidation within 1000 ms, nothing pushed if none | developers.binance.com ws-streams/market | 2026-11-01 |
| REST limits are per IP; 429 = back off; continued requests cause 418 IP ban from 2 minutes to 3 days; numeric limits are in `exchangeInfo` `rateLimits` | developers.binance.com futures general-info | 2026-11-01 |
