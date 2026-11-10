# Open Questions and Unverified Facts

Move an item to "Verified" with source and date once checked.

## Inputs needed from the owner

| Question | Affects |
|---|---|
| Measure sustained chunked upload throughput over several days; re-test a 500 MB single PUT; check nginx temp-file disk use during large uploads | [infrastructure](infrastructure.md) |
| Plan for more storage capacity (storage gateway has ~6 GB free) | [infrastructure](infrastructure.md) |
| Monthly budget for compute and storage | Recording scale |
| Preferred symbols for the depth set (size fixed at 10–20) | [universe](universe.md) |
| Record spot (tier D)? | [data-recording](data-recording.md) |
| First consumer of the alert interface: none yet; build the generic signed webhook and pull API, add adapters when a consumer exists | [p0-support-design](p0-support-design.md) |
| Typical position size and holding period | [trading-and-risk](trading-and-risk.md) cost and latency |
| Account fee tier (maker/taker) | [trading-and-risk](trading-and-risk.md) |
| Can the execution host be placed near the exchange? | [trading-and-risk](trading-and-risk.md) latency |

## Unverified facts (check against official sources)

- Still unverified: whether the futures WebSocket has an undocumented new-connection or per-IP connection limit (none is documented); which message types count toward the 10 messages/s limit; whether `@depth@250ms` is a valid stream name (the R4 250 ms run received nothing; documented default is `@depth`); OpenList/OneDrive quota for ~18 GB/day of uploads; forced 24 h disconnect behavior (observed in the production collector's first two days).
- The spot-doc limits (1024 streams, 5 control messages/s, 300 connections per 5 min per IP, 24 h connection life) are spot only.

- Other venues (Bybit, OKX, Bitget, Gate, MEXC, KuCoin, Coinbase): whether each publishes a liquidation feed (and whether it is sampled), open interest, leverage brackets, mark-price formula, rate limits; Binance's share of each symbol's total open interest. None checked. See [liquidation-model.md](liquidation-model.md) Multiple venues.
- Liquidation mechanics still unverified: the definition of "Contract Price" in the mark-price median (page text was truncated); whether the median actually bounds mark price during contract-only pushes (testable on recorded `mark` and `trades`); maintenance margin tiers and leverage brackets (`/fapi/v1/leverageBracket` returns 401 without a signed key); the liquidation clearance fee rate; ADL ranking details. See [liquidation-model.md](liquidation-model.md).

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
| All-market `!bookTicker` update speed is 5 s | developers.binance.com ws-streams/public (All Book Tickers Stream) | 2026-11-01 |
| `exchangeInfo` `symbols[]` fields: `symbol`, `pair`, `status`, `contractType`, `baseAsset`, `quoteAsset`, `marginAsset`, `onboardDate`, `deliveryDate`, `underlyingType`; `rateLimits` entries have `rateLimitType` (`REQUEST_WEIGHT`, `ORDERS`), `interval`, `intervalNum`, `limit` (docs example shows 6000 per minute; read the real value at runtime) | developers.binance.com USDⓈ-M REST market-data (Exchange Information) and OpenAPI schema, via subagent | 2026-11-01 |
| Live `exchangeInfo`: `contractType` counts PERPETUAL 703, TRADIFI_PERPETUAL 235, CURRENT_QUARTER 2, NEXT_QUARTER 2; USDT + PERPETUAL + TRADING = 522 symbols; `rateLimits` REQUEST_WEIGHT 2400/min, ORDERS 1200/min and 300/10 s; `markPrice` stream includes non-crypto symbols (e.g. XAUUSDT, TSLAUSDT) | Trial R0, `/fapi/v1/exchangeInfo` on the recorder | 2026-11-01 |
| Load on the 2 vCPU / 4 GB host with 522 `aggTrade` + `markPrice` + `forceOrder` + 10 depth symbols + `bookTicker` top 50: CPU mean 18%, peak 53% of one core, 0 drops | [trial-plan](trial-plan.md) Results | 2026-11-02 |
| No new-connection rate limit or per-IP concurrent-connection limit is stated in the futures WebSocket Connect or general-info pages; only 1024 streams per connection and 10 incoming messages/s | developers.binance.com futures websocket-market-streams/Connect.md, general-info.md, via subagent | 2026-11-01 |
| Futures WebSocket: no documented per-IP connection cap or connection-attempt rate; 1024 streams and 10 incoming messages/s per connection are documented; IP bans scale from 2 minutes to 3 days; the docs give no guidance on overlap when replacing a connection | developers.binance.com futures websocket-market-streams and general-info, via subagent | 2026-11-04 |
| USDⓈ-M perpetual mark price = Median(Price 1, Price 2, Contract Price); Price 1 = Price Index × (1 + Last Funding Rate × Time Until Next Funding / Funding Period); Price 2 = Price Index + Moving Average (1-minute basis), 60 points, one per second, basis = (Bid1 + Ask1)/2 − Price Index; the 1-minute basis replaced the 2.5-minute basis from 2025-05-05 09:00 UTC | binance.com support FAQ 360033525071 and announcement of 2025-05-02, via search excerpts | 2026-11-10 |
| Liquidation is triggered when mark price reaches the liquidation price; the executed price can differ when mark price jumps; liquidation condition is collateral (initial + realized + unrealized PnL) below maintenance margin | binance.com support FAQ 360033525271 (Futures Liquidation Protocols), via search excerpts | 2026-11-10 |
| Insurance funds cover only the gap between bankruptcy price and execution price; when a fund is insufficient, ADL ranks opposing positions by leverage and profitability; BTC/ETH/BNB share one USDT fund, a second group (DOT, LINK, XMR, ADA, BCH, EOS, ETC, LTC, TRX, XLM, XRP) shares another, other USDT contracts use other pools | binance.com support FAQ 360033525371 (Introduction to Insurance Funds), via search excerpts | 2026-11-10 |
| `GET /fapi/v1/openInterest?symbol=` is public and returns `openInterest` (base units) and `time`; `GET /futures/data/openInterestHist` is public and returns `sumOpenInterest`, `sumOpenInterestValue` per period (5m tested) | Live calls from the research Mac | 2026-11-10 |
| `GET /fapi/v1/premiumIndex?symbol=` is public and returns `markPrice`, `indexPrice`, `estimatedSettlePrice`, `lastFundingRate`, `nextFundingTime`; `GET /fapi/v1/constituents?symbol=` is public and returns each index venue with price and weight | Live calls from the research Mac | 2026-11-10 |
| Index composition differs by symbol: BTCUSDT has 8 venues, Binance spot weight 0.43; FLOCKUSDT has 4 venues (gateio, mexc, bitget, bybit), no Binance; XANUSDT has 6 including coinbase and `binance_alpha`. Weights are fixed fractions that change over time | Live `constituents` calls | 2026-11-10 |
