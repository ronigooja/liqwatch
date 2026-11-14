# Venue APIs

Keyless public API surveys on 2026-11-12 and 2026-11-14, and live adapter captures on 2026-11-13. Governing records: [0013](decisions/0013-no-dex-recording.md) and [0014](decisions/0014-daily-pack-storage.md).

Status labels: **measured** = live observation with stated date; **doc** = stated in official documentation, not necessarily tested live; **unverified** = not established. Live WebSocket captures are listed below. Counts are response-filter counts, not tradable-volume counts.

## REST history and snapshot endpoints (2026-11-14)

**Live** = measured keyless request on 2026-11-14, generally BTC; **Doc** = survey's documentation reading, not independently verified against official sources; **Unknown** = not established. Source: user-provided REST polling survey, 2026-11-14; request logs and exact parameters are not available, so measurements are not independently reproducible. Times describe returned coverage, not guarantees.

### Open interest

| Venue | Endpoint, depth and behaviour |
|---|---|
| Binance | Live: `/fapi/v1/openInterest` snapshot, weight 1; `/futures/data/openInterestHist` per symbol, 5m, 1,000 rows (83h20m); 29-day start accepted, 31-day rejected; start without end returns newest 1,000. Doc: max 500 (41h40m), weight 0, 1,000 requests/5m/IP. |
| Bybit | Live: `/v5/market/tickers` bulk snapshot; `/v5/market/open-interest` 5m, 200 rows (16h40m), per symbol with cursor, BTC from Jan 2021. Doc: back to launch; general 600 requests/5s/IP. |
| OKX | Live: `/api/v5/public/open-interest` bulk snapshot, 543 swaps; `/api/v5/rubik/stat/contracts/open-interest-history` 5m, 100 rows/page (8h20m) even when requesting 1,440; 4-day-old returned, 6-day-old empty. Doc: latest 1,440 observations (~5 days); snapshot 20 requests/2s, history 10/2s. |
| Bitget | Live: `/api/v3/market/open-interest` bulk snapshot, 902 entries; `/api/v3/market/open-interest-history`, available since Oct 2026, 5m, 30 rows (2h30m); old startTime/endTime/cursor/limits ignored. Doc: history 10/s/IP. |
| Gate | Live: `/futures/usdt/contracts` `position_size` snapshot; `/futures/usdt/contract_stats` 1m, max 2,000 (33h20m nominal), 179-day-old returned, 180-day limit error for 2020; 2,000 accepted, 2,001 rejected. Includes OI, USD OI, ratios, liquidation totals and mark; a source minute was missing. |
| MEXC | Live: `/api/v1/contract/ticker` `holdVol`, bulk 1,163 tickers. Doc: 10 requests/2s. Unknown: OI history endpoint, none found. |
| KuCoin | Live: `/api/ua/v2/market/open-interest` snapshot all symbols or up to 10; `interval=5min` 200 rows (16h40m), 6-day-old returned, 7–8-day-old empty, 200 accepted and 201 rejected. Doc: weight 10, 7 days at 5m. |

Live: Binance bounded 24h OI with `limit=5` returned the newest five rows; with `limit=500` it returned all 288 buckets. Live: `exchangeInfo` and `constituents` each had weight 1; `exchangeInfo` reported 2,400/min. Live: the five history responses lacked `X-MBX-USED-WEIGHT-1M`. Source: user-provided survey, 2026-11-14; request logs unavailable.

### Long/short ratios and taker flow

| Venue | Endpoint, depth and behaviour |
|---|---|
| Binance | Live: `/futures/data/` `globalLongShortAccountRatio`, `topLongShortAccountRatio`, `topLongShortPositionRatio`, `takerlongshortRatio`: 5m, 1,000 rows, 29-day start accepted, 31-day rejected. Doc: max 500, weight 0; OI timestamps at period end, taker volume at period start. |
| Bybit | Live: `/v5/market/account-ratio`, 5m, 500 rows (41h40m), from 2020-07-20. |
| OKX | Live: `/api/v5/rubik/stat/contracts/` `long-short-account-ratio-contract`, `long-short-account-ratio-contract-top-trader`, `long-short-position-ratio-contract-top-trader`, and `taker-volume-contract`: 5m, 100 rows; ratios 4-day-old returned, 6-day-old empty; currency `long-short-account-ratio`: 576 rows (48h). Doc: contract ratio depth 1,440. |
| Bitget | Live: `/api/v2/mix/market/` `long-short`, `account-long-short`, `position-long-short`, `taker-buy-sell`: 5m, 30 rows (2h30m); old parameters ignored. Doc: 1/s/IP. |
| Gate | Live: `contract_stats` carries ratio variants at 1m; [coverage](#open-interest). |
| MEXC | Unknown: no public ratio endpoint found. |
| KuCoin | Unknown: no public ratio endpoint found. |

### Funding history

| Venue | Endpoint, depth and behaviour |
|---|---|
| Binance | Live: `/fapi/v1/fundingRate` max 1,000 rows, BTC from 2020; without symbol, 1,000 rows across symbols, and one settlement-time range returned 811 symbols. Doc: shared 500 requests/5m/IP with `fundingInfo`. |
| Bybit | Live: `/v5/market/funding/history`, 200 rows. |
| OKX | Live: `/api/v5/public/funding-rate-history` max 400, BTC 288 (96 days). Doc: 3 months. |
| Bitget | Live: `/api/v2/mix/market/history-fund-rate`, 100 rows; page 3 ended around 90 days, page 4 empty. |
| Gate | Live: `/futures/usdt/funding_rate` max 1,000, 179-day-old returned, older rejected. |
| MEXC | Live: `/api/v1/contract/funding_rate/history`, 1,000 rows; BTC oldest 2025-05-23. |
| KuCoin | Live: `/api/ua/v2/market/funding-rate-history` or `/api/v1/contract/funding-rates`, 100 rows observed; BTC from Jan 2021. |

### Risk tiers and metadata

All entries are Live current snapshots, not historical tier series; counts are captured entries, not necessarily active markets.

| Venue | Endpoint and observed behaviour |
|---|---|
| Binance | `POST https://www.binance.com/bapi/futures/v1/friendly/future/common/brackets`: undocumented website endpoint, 1,083 symbols; `exchangeInfo` 946 entries, weight 1. Two captures 600s apart: zero changed entries in either. Doc: tier changes announced; no fixed frequency. |
| Bybit | `/v5/market/risk-limit`: 15 symbols/page, bulk cursor; `/v5/market/instruments-info`: 929 instruments. |
| OKX | `/api/v5/public/position-tiers`: 99 BTC tiers, up to five families/call, separate margin modes; `/api/v5/public/instruments`: 543 swaps. |
| Bitget | `/api/v2/mix/market/query-position-lever` per symbol, 12 BTC tiers; `/api/v2/mix/market/contracts`: 885 entries. V3 `/api/v3/market/position-tier` returned BTC tiers but error 40019 without symbol although Doc says optional. |
| Gate | `/futures/usdt/risk_limit_tiers`: 100 markets/page by default; limit=1000 still returned 100; `/futures/usdt/contracts`: 1,051 entries. |
| MEXC | `/api/v1/contract/detail`: 1,154 entries including risk tiers. |
| KuCoin | `/api/v1/contracts/risk-limit/{symbol}` per symbol, 12 BTC tiers, classic isolated margin; `/api/v1/contracts/active`: 727 entries; UTA position-tier attempts returned 404. |

### Insurance funds

| Venue | Endpoint and observed behaviour |
|---|---|
| Binance | Live: `/fapi/v1/insuranceBalance` snapshot, 124 pools. |
| Bybit | Live: `/v5/market/insurance` snapshot, 76 rows; Doc: isolated pools update each minute, shared daily. |
| OKX | Live: `/api/v5/public/insurance-fund`, 100 minute-spaced entries. |
| Bitget | Live: `/api/v3/market/risk-reserve-hour` 30 hourly rows, `risk-reserve` 30 daily rows, `risk-reserve-all` 45 pools. |
| Gate | Live: `/futures/usdt/insurance`, 180 daily rows. |
| MEXC | Live: `/api/v1/contract/risk_reverse/history`, 42 four-hourly rows over 7 days. |
| KuCoin | Unknown: no endpoint found. |

### Polling cost per item (inferred)

Inferred from the survey's illustrative intervals for 522 symbols, requests/day without retries. No poll schedule is decided by this table; current implementation: [poller](poller.md).

| Venue | Item and illustrative interval | Requests/day |
|---|---|---:|
| Binance | OI snapshot 5m | 150,336 |
| Binance | OI history and four ratios, each 20h | 3,132 |
| Binance | Funding, constituents, bulk tiers and metadata daily | 1,046 |
| Bybit | OI history 8h; account ratio 20h; funding daily; tiers; metadata | 1,566; 626.4; 522; 35; 1 |
| OKX | OI history, three ratios and taker volume, each 4h; currency ratio daily; funding daily; tiers; metadata | 15,660; 522; 522; 210; 1 |
| Bitget | OI history, three ratios and taker volume, each 1h; funding and tiers daily | 62,640; 1,045 |
| Gate | `contract_stats` 12h; funding daily; tiers; metadata | 1,044; 522; 6; 1 |
| MEXC | Bulk OI snapshot 5m; funding; metadata and tiers | 288; 522; 1 |
| KuCoin | OI history 8h; funding; tiers; metadata | 1,566; 522; 522; 1 |

## Live adapter captures

Measured on 2026-11-13; fixture captures demonstrate observed delivery, not completeness. Official links and detailed counts are in each cited README.

| Venue | Observation and source |
|---|---|
| Bybit | [Capture](../internal/venue/bybit/testdata/README.txt): 9m59s, six symbols; ticker saved-delta intervals 99–300 ms; deltas omit unchanged fields; liquidation completeness unestablished. |
| OKX | [Capture](../internal/venue/okx/testdata/README.txt): 10 min, six swaps; 77,613 trade, 17,844 mark, 8,655 index, 366 OI, 66 funding, 69 liquidation frames; index IDs are underlying IDs. Official docs say liquidation feed is incomplete ([survey](#okx)). |
| Bitget | [Capture](../internal/venue/bitget/testdata/README.txt): V3 trade, ticker, funding, position-tier and global liquidation delivered; position-tier data was an object, unlike the documented array example. Official docs specify 100 connections/IP and ticker on-change cadence; see capture links. |
| Gate | [Capture](../internal/venue/gate/testdata/README.txt): identical ETH trade size was `0.1` with decimal header and `0` without it; stats symbol batching rejected; three-minute stats probe had no updates. |
| MEXC | [Capture](../internal/venue/mexc/testdata/README.txt): 610 s, four symbols; ticker 246–247 frames/symbol, about 2.45 s average versus documented 1 s on trades; 100 ticker subscriptions received data in 90 s. |
| KuCoin | [Capture](../internal/venue/kucoin/testdata/README.txt): 610 s, eight symbols; mark/index 37–378 updates/symbol, funding 10/symbol, executions 81–566/symbol. Active-contract response contained no non-ASCII symbol; Chinese-named symbol not captured. |

## Binance index constituents (measured)

Source: `GET /fapi/v1/constituents?symbol=` for all 522 TRADING USDT PERPETUAL symbols on 2026-11-12. 519 succeeded; `ALLUSDT`, `BTCDOMUSDT`, `PREDICTUSDT` returned HTTP 400 (-1121). Entry fields: `exchange`, `symbol`, `price`, `weight`; no field marks spot or futures.
Constituents per symbol: min 2, median 5, max 9; none empty. Weights are percentages of the returned entries.

| Source name | Symbols | Mean weight % | Max weight % | Pair format |
|---|---:|---:|---:|---|
| `mexc` | 412 | 11.2 | 32.6 | `BASEUSDT` |
| `gateio` | 405 | 10.2 | 43.5 | `BASE_USDT` |
| `binance` | 363 | 60.0 | 87.0 | `BASEUSDT` |
| `bitget` | 298 | 11.9 | 46.2 | `BASEUSDT` |
| `kucoin` | 295 | 11.8 | 46.2 | `BASE-USDT` |
| `bybit` | 255 | 12.2 | 50.0 | `BASEUSDT` |
| `okex` | 217 | 17.4 | 42.9 | `BASE-USDT` |
| `binance_cross2` | 102 | 4.7 | 10.5 | `BASETRY/USDTTRY`, other crosses |
| `binance_future` | 95 | 20.1 | 50.0 | `BASEUSDT` |
| `coinbase` | 89 | 17.7 | 54.5 | `BASE-USD*uindex(USDUSDT)`, some USDT, USDC |
| `binance_alpha` | 81 | 33.2 | 76.9 | `BASEUSDT`, some USDC via `uindex` |
| `pancakeswapV3` | 32 | 19.8 | 53.8 | USDT, WBNB, USDC, USD1 pair expressions |
| `binance_cross3` | 9 | 1.5 | 1.8 | `BASETRY/USDTTRY` or stable cross |
| `binance_cross` | 5 | 13.6 | 14.6 | e.g. `ARBBTC*BTCUSDT` |
| `pancakeswapV2` | 3 | 16.6 | 20.0 | WBNB pairs with `uindex(BNBUSDT)` |
| `uniswapV2` | 3 | 5.5 | 9.5 | WETH or USDS pairs |
| `uniswapV3` | 2 | 17.5 | 30.0 | WETH or USD1 pairs |
| `aster` | 1 | 33.3 | 33.3 | `BASEUSDT` |
| `bitfinex` | 1 | 20.0 | 20.0 | `XMRUST` |

195 symbols have at least one `binance_alpha` or `binance_cross*` entry. Some symbols carry multipliers in the pair expression. Other venues' prices enter the Binance mark price only through these entries ([open-questions.md](open-questions.md) mark-price formula).

## Coverage matrix

Y = documented keyless public source; N = none found; ? = unverified. Live = WebSocket or equivalent; Hist = REST history beyond the latest page.

| Data | Bybit | OKX | Bitget | Gate | MEXC | KuCoin | Coinbase |
|---|---|---|---|---|---|---|---|
| Perp trades live | Y | Y | Y | Y | Y | Y | Global derivatives Y |
| Perp trades hist | last 1,000 only | 3 months | 90 days | `from`/`to`, retention unknown | last 100 only | last 100 only | time range, retention unknown |
| Spot trades live | Y | Y | Y | Y | Y (protobuf) | Y | Y |
| Mark price live | Y, 100 ms | Y, 200 ms | Y, 200 ms on change | tickers, cadence unspecified | `sub.fair.price` on change | Y, 1 s | ticker, interval-dependent |
| Mark hist | 1 m candles | 1 m candles | 1 m candles | 10 s candles | 1 m candles | current only | ? |
| Index price live | Y, 100 ms | Y, 100 ms | Y, 200 ms on change | tickers, cadence unspecified | `sub.index.price` on change | Y, 1 s | Y |
| Liquidations live | Y, all, 500 ms batches | Y, **incomplete** | Y, **sampled** | Y, batched, not sampled per doc | N | N | `liquidation` field in trades |
| Liquidations hist | N | N | N | REST `liq_orders`, 3,600 s per query | N | N | via trade history |
| Open interest | Y, hist from 5 m | Y, 3 s; hist: [depth and page](#open-interest) | [history](#open-interest) | Y, hist | snapshot only (`holdVol`) | Y, hist 7 d | book summary |
| Funding | Y | Y | Y | Y | Y | Y | Y |
| Risk tiers | Y, current | Y, current | Y, current | Y, current | Y, current | Y, current | no public complete table |
| Long/short ratio | Y, from 2020-07-20, 5 m | Y, 5 m | Y, [history](#longshort-ratios-and-taker-flow) | via `contract_stats` | N | N | ? |

No venue documents historical risk-tier snapshots, so tiers must be recorded as snapshots.

## Recordable data per venue (liquidation model inputs)

Scope: seven centralized venues are in scope ([0014](decisions/0014-daily-pack-storage.md)); DEXes are not ([0013](decisions/0013-no-dex-recording.md)). This table describes recordable sources, not all implemented channels; see [adapter captures](#live-adapter-captures) and [implementation](venue-adapters.md).

| Data | Binance | Bybit | OKX | Bitget | Gate | MEXC | KuCoin |
|---|---|---|---|---|---|---|---|
| Trades | WS `aggTrade` | WS `publicTrade` | WS `trades-all` (business endpoint) | WS `publicTrade` (V3) | WS `futures.trades` | WS `sub.deal` | WS `execution` (public token) |
| Open/close flag | none | not found | not found | not found | not found | `O` field (below) | not found |
| Liquidations | `!forceOrder@arr`, one per symbol per second (lower bound) | `allLiquidation`, doc says all, 500 ms batches | `liquidation-orders`, OKX says not all | `liquidation`, sampled, two per pair per second | `public_liquidates` (`!all`), doc says batched not sampled | none | none |
| Mark price | `markPrice@1s` | `tickers`, 100 ms | `mark-price`, 200 ms | `ticker`, 200 ms on change | `tickers`, cadence unspecified | `sub.fair.price`, on change | `instrument`, 1 s |
| Index price | same stream | `tickers` | `index-tickers`, 100 ms | `ticker` | `tickers` | `sub.index.price` | `instrument`, 1 s |
| Funding rate | same stream | `tickers` | `funding-rate`, 30 to 90 s | `funding-rate` | `tickers` | `sub.funding.rate` | `instrument`, 1 min |
| Open interest | REST poll, 5 min | `tickers`, 100 ms | `open-interest`, 3 s | `ticker` field | `contract_stats`, 1 min | `sub.ticker` `holdVol` | UTA `mark-price` `oi`, 1 s, or REST |
| Leverage tiers | website internal endpoint (may change); [poller schedule](poller.md) | REST `risk-limit` | REST `position-tiers` | WS `position-tier` | REST `risk_limit_tiers` | REST `contract/detail` | REST `risk-limit` per symbol |
| Long/short ratios | REST: top-trader position, top-trader account, global account, taker volume; 5 min buckets, [poller schedule](poller.md) | REST `account-ratio`, 5 min | REST contract and top-trader ratios, 5 min | REST ratios and taker flow: [survey](#longshort-ratios-and-taker-flow) | `contract_stats`, 1 min | none | none |
| Index constituents | `constituents`, daily | n/a | n/a | n/a | n/a | n/a | n/a |

Constraints that shape recording:

| Constraint | Detail |
|---|---|
| Trade history is short | Public trade history is the latest 1,000 (Bybit), 100 (MEXC), 100 (KuCoin); Bitget 90 days, OKX 3 months, Gate unknown. Record trades live on all venues except Binance, whose daily public files can fill gaps. |
| Liquidation history | Only Gate has REST history (3,600 s per query, retention unknown). Others must be recorded live. |
| MEXC `O` | Record the exchange timestamps `t` and `cts` with `T`, `O`, `M`; alignment by receipt time drops the check from correlation 0.9976 to 0.52. |
| Open interest units | Bybit docs say both sides, Gate `total_size` is twice `position_size`, MEXC side undocumented, KuCoin unit (contracts or lots) confirmed only for XBT. Store raw fields and the unit metadata, convert later. |
| Gate | Futures WS needs header `X-Gate-Size-Decimal: 1` or fractional sizes are truncated. `lsr_taker` and `lsr_account` labels differ between REST and WS docs; check live. |
| Symbol mapping | Scale prefixes (`1000PEPE`, `10000CAT`, `1MBABYDOGE`) and aliases (`XBT` for `BTC`); build a reviewed mapping from each venue's instrument list and Binance `exchangeInfo`, store the raw symbol. |
| Not obtainable anywhere | Trader leverage distribution, share of crossed positions that liquidate, account-level positions. |

## Per venue

### Bybit
| Item | Value | Status |
|---|---|---|
| Endpoints | `/v5/public/linear`, `/v5/public/spot`; topics `publicTrade`, `tickers` (mark, index, OI, funding), `allLiquidation`; REST `/v5/market/*` | doc; REST measured |
| Liquidation | `allLiquidation.{symbol}` pushes all liquidations, 500 ms batches, price is bankruptcy price; the older `liquidation` topic is sampled at one per second per symbol and deprecated | doc |
| REST limits | 600 requests per 5 s per IP; HTTP 403 means stop at least 10 minutes | doc |
| WS limits | 500 new connections per 5 min; 1,000 market-data connections per IP; spot 10 args per request; 21,000 characters per args array; ping every 20 s; 100-args-per-connection value marked "TBD" | doc |
| Universe | 822 linear USDT perpetuals Trading; 521 spot Trading, 389 USDT-quoted; `1000PEPEUSDT` has base `1000PEPE` | measured |

### OKX
| Item | Value | Status |
|---|---|---|
| Endpoints | WS `/ws/v5/public` (`mark-price`, `index-tickers`, `liquidation-orders`, `open-interest`, `funding-rate`), `/ws/v5/business` (`trades-all`); REST `/api/v5/*` | doc |
| Liquidation | `liquidation-orders` by `instType=SWAP`; OKX states it does not represent all liquidations; no REST history found | doc |
| History | trades 3 months, 100 per page; OI: [depth and page](#open-interest); funding 3 months, 400 per page; mark and index 1 m candles | doc |
| Limits | REST history 10 to 20 requests per 2 s per IP; WS 3 connection attempts per s per IP, 480 subscribe requests per connection per hour, 64 KB args; 30 s idle close; connections per IP and forced reconnect not found | doc |
| Units | swap sizes are in contracts, spot in base currency | doc |
| Live access | Initial instrument-list calls returned HTTP 403; later REST and WebSocket capture succeeded ([capture](#live-adapter-captures)) | measured; original 403 cause unverified |

### Bitget
| Item | Value | Status |
|---|---|---|
| Endpoints | V3 WS `publicTrade`, `ticker` (mark, index, OI), `liquidation`, `funding-rate`, `position-tier`; V2 REST `/api/v2/mix/market/*` | doc; REST measured |
| Liquidation | once per second, at most two records per pair per second (largest long and largest short); sampled; no REST history | doc |
| History | trades 90 days, 7-day window, 1,000 per page (spot timestamps second-level); candles 1 m, 90-day range; OI history: [survey](#open-interest) | doc; Live |
| Limits | V3 docs specify 300 connection requests per 5 min per IP, 100 connections per IP, under 50 channels recommended, ping every 30 s; V3 trade snapshot on subscribe is removed on 2026-11-17 ([source](../internal/venue/bitget/testdata/README.txt)) | doc |
| Universe | 885 USDT-futures entries (not filtered by `symbolType`); 3,669 spot pairs, 3,662 online | measured |

### Gate
| Item | Value | Status |
|---|---|---|
| Endpoints | futures WS `wss://fx-ws.gateio.ws/v4/ws/usdt` (`futures.trades`, `futures.tickers`, `futures.public_liquidates` with `!all`, `futures.contract_stats`, `futures.contract_info`); spot WS `wss://api.gateio.ws/ws/v4/`; REST `https://api.gateio.ws/api/v4` | doc; REST measured |
| Liquidation | batched per contract, 1 s or 20 orders; changelog says batching replaced dropping orders; completeness not independently proven | doc |
| Limits | REST 200 requests per 10 s per endpoint per IP; 300 WS connections per IP; subscriptions per connection and forced reconnect not found | doc |
| Caveats | send `X-Gate-Size-Decimal: 1` on futures WS or fractional sizes are truncated; REST `create_time_ms` field was seconds-scale in the sample; `lsr_taker` and `lsr_account` labels differ between REST and WS docs | doc, measured |
| Universe | 1,086 futures contracts, none in delisting; 2,156 spot pairs, 2,155 tradable, 2,006 USDT-quoted; `PEPE_USDT` multiplier 10,000,000 | measured |

### MEXC
| Item | Value | Status |
|---|---|---|
| Endpoints | futures WS `sub.deal`, `sub.fair.price`, `sub.index.price`, `sub.ticker` (`holdVol`), `sub.funding.rate`; spot WS `spot@public.aggre.deals.v3.api.pb@(10ms|100ms)@<SYMBOL>`; REST `/api/v1/contract/*`, `/api/v3/*` | doc; some REST measured |
| Missing | no public liquidation feed and no long/short ratio found | unverified |
| Limits | futures REST 20 requests per 2 s; spot REST weight 5, 500 weight per 10 s per IP-endpoint; spot WS protobuf, 30 streams per connection, 24 h life; futures ping every 10 to 20 s; connections per IP not found | doc |
| Open interest | current snapshot only; `holdVol` semantics unconfirmed | measured, doc |
| Live access | `/api/v1/contract/detail/country` returned HTTP 403; stopped; cause unverified. Later checks: `contract.mexc.com` and `api.mexc.com` both served keyless tickers; the only 403 seen was on a website analytics asset | measured |
| Liquidation (rechecked) | No public market-wide feed in either official reference or the changelog; `risk_reverse` insurance fund balance keyless, history only 4-hourly snapshots (42 from 2026-11-05 to 11-12), no events; website inspection partial (45 of 108 scripts); Tardis lists no liquidation channel | measured, doc |
| Deal fields | `T` 1 buy, 2 sell; `M` 1 self-trade, 2 not; `O` 1 new position, 2 reduce, 3 unchanged. REST doc: `O` = combined maker and taker action (1 both open, 2 both not open, 3 other); WS doc does not say whose position. `O=3` does not say which side opened. Parsed by Tardis (`NewPosition`, `ReducePosition`, `Unchanged`), not by CCXT | doc |
| `O` check | 22,566 records, 23 contracts, about 15.7 min of stream time over two captures (first connection dropped after about 7.4 min; TON gave no records). BTC, 166 intervals of about 5 s: sum of `v` with `O=1` minus `v` with `O=2` vs change in `holdVol`: correlation 0.9976 and same sign in 162 of 166 when aligned by exchange timestamp; 0.52 and 118 of 166 when aligned by local receipt time. Share of `O=3`: BTC about 52%, ETH about 39%; share of `O=1`: SOL 36%, BTC 22%. Consequence: record the exchange timestamp (`t`, `cts`) with `T`, `O`, `M`. Not established: completeness of the public stream, effect of reversals, special executions and self-trades | measured |
| Universe | 1,104 enabled USDT contracts (perpetual type not filtered); spot 1,790 symbols, 1,412 USDT-quoted; `PEPE_USDT` contract size 10,000,000 | measured |

### KuCoin
| Item | Value | Status |
|---|---|---|
| Endpoints | classic futures WS `/contractMarket/execution:{symbol}`, `/contract/instrument:{symbol}` (mark, index, funding); spot WS `/market/match:{symbol}`; token from `POST /api/v1/bullet-public` (keyless, measured); UTA V2 `mark-price` channel includes `oi` | doc; token and REST measured |
| Missing | no public liquidation feed and no long/short ratio found | unverified |
| History | trades last 100 only; OI intervals from 5 m, 7 days (daily 70 days), data begins no earlier than 2025-12-29; funding history keyless: [survey](#funding-history) | doc |
| Limits | 800 public connections per IP; 100 messages per 10 s per connection; futures topics unlimited per connection; REST pool 2,000 weight per 30 s; error-code page still says 50 sessions and 300 subscriptions (documentation conflict) | doc |
| Universe | 712 contracts (USDT, `FFWCSX`, Open); 958 spot symbols, 957 enabled; `XBTUSDTM` base XBT; `PEPEUSDTM` multiplier 520,000 | measured |

### Coinbase
| Item | Value | Status |
|---|---|---|
| Spot | Exchange WS `matches`, `level2` (derive best bid and ask locally), `level2_batch` 50 ms; Advanced Trade WS `market_trades` (250 ms batches), `level2` | doc |
| Perpetuals | Coinbase International Exchange became read-only on 2026-10-01 and migrated to a Deribit-based Global Derivatives gateway (`wss://streams.drb.coinbase.com/ws/api/v2`, `https://drb.coinbase.com/api/v2`). Do not build a collector for the old exchange | doc |
| Perp data | trades with a `liquidation` field, `quote`, `book`, `deribit_price_index`, `ticker`, `perpetual` (funding); no public complete risk-tier table; OI via `get_book_summary_by_instrument` | doc |
| Limits | Exchange REST 10 requests per s per IP, burst 15; WS 8 requests per s per IP, conflicting message limits; Advanced Trade 8 connections per s per IP on one page and 750 on another (conflict); idle channels close after 60 to 90 s | doc |
| Symbols | spot `BTC-USD`; derivatives `{BASE}_USDC-PERPETUAL`; USD, USDT, USDC are distinct quotes, and most `-USDC` public ids alias `-USD` data | doc |
| Live access | one Exchange product call succeeded, the next returned HTTP 403; stopped; spot count and overlap not computed. Global derivatives instrument list: 219 instruments, 148 `PERPETUAL`, includes equity and commodity products | measured |

## Binance share of open interest (measured, 2026-11-12)

Question: how much of a coin's open interest across seven centralized venues sits on Binance. Answers the "Binance's share of each symbol's total open interest" gap in [liquidation-model.md](liquidation-model.md) Multiple venues.
Method: one REST snapshot per venue at about 13:52 UTC (see per-venue endpoints above), open interest converted to USD with the venue's mark price (USDT treated as USD), summed by underlying base asset (scale prefix `1000`, `10000`, `100000`, `1000000`, `1M` stripped, `XBT` mapped to `BTC`), restricted to the 522 Binance USDT perpetual bases. One snapshot, so no time spread; token identity by name only; Binance's own TradFi contracts are not removed. Raw per-venue files were kept outside the repository and are not reproducible from the repository alone.

| Venue | Open interest on Binance bases ($bn) | Binance bases covered |
|---|---:|---:|
| Binance | 18.41 | 522 |
| Bybit | 10.15 | 462 |
| MEXC | 8.79 | 501 |
| Bitget | 6.53 | 450 |
| OKX | 5.53 | 267 |
| Gate | 5.15 | 498 |
| KuCoin | 2.41 | 477 |
| All seven | 57.0 | |

| Result | Value |
|---|---|
| Binance share, open-interest weighted | 32.3% |
| Binance share per coin, median | 43.2% |
| Coins on one venue only / on all seven | 5 / 242 (six venues: 170) |
| Top 10, 50, 100 coins by Binance open interest: Binance share | 31.9%, 31.6%, 31.9% |
| Top 10, 50, 100 coins: share of Binance's own open interest | 80.0%, 90.2%, 93.7% |

| Binance share per coin | Coins | Binance open interest ($bn) |
|---|---:|---:|
| below 20% | 20 | 0.1 |
| 20% to 35% | 129 | 10.77 |
| 35% to 50% | 194 | 6.10 |
| 50% to 70% | 121 | 0.93 |
| 70% to 90% | 45 | 0.40 |
| 90% and above | 13 | 0.11 |

Largest coins, Binance share: BTC 28%, ETH 39%, SOL 28%, ZEC 37%, HYPE 31%, BNB 52%, XRP 25%, NEAR 33%, WLFI 75%, DOGE 28%. For BTC the other venues hold: Bybit 20%, MEXC 18%, Bitget 11%, OKX 10%, Gate 9%, KuCoin 3%.

Unit convention is not settled. Bybit's documentation calls its open interest both-sides; Gate's `total_size` ticker field was exactly twice `position_size` for BTC; MEXC's `holdVol` side convention is undocumented (BTC $4.56bn here vs a third-party $4.66bn and $2.41bn at unspecified times). Binance defines open interest per contract once. BTC converted to coin units is the same order on all venues (Binance 100,450; Bybit 71,057; MEXC 62,939; Bitget 39,674; OKX 36,121; Gate 32,089; KuCoin 10,884), so no venue is off by a clear factor of two. Sensitivity of the Binance share: Bybit halved 35.5%; Gate doubled 29.6%; Bybit halved, Gate doubled, MEXC halved 35.0%. Gate (490 contracts) and others include non-crypto contracts; those fall outside the 522 Binance bases unless Binance lists the same TradFi base.

## Perpetual DEXes (partial survey, 2026-11-12)

Keyless market-list calls succeeded for Hyperliquid (default perp DEX), dYdX, Aster and Lighter. Variational returned HTTP 429 on the first call and was not called again. This first pass connected no WebSocket; the later liquidation captures are in the next subsection. Volume is DefiLlama's reported volume (not normalized). Binance on 2026-11-12 (measured): USDT perpetual 24 h volume about $53.3bn (`/fapi/v1/ticker/24hr`, 758 USDT symbols), open interest about $18.4bn (`/fapi/v1/openInterest` times price for 516 of 522 trading perpetuals; 6 failed, so slightly low). Lighter (both DefiLlama entries summed, about $0.69bn) is about 5.0% of volume and 3.8% of open interest, including non-crypto markets; dYdX v4 about 0.03% and 0.2%. DEXes are not recorded: [decision 0013](decisions/0013-no-dex-recording.md).

| Venue | 24h volume | Open interest | Perp markets | Binance base matches |
|---|---:|---:|---:|---:|
| Hyperliquid | $9.84bn | $6.62bn | 179 (default DEX, excludes builder-deployed DEXes) | 174 |
| Aster | $1.75bn | $1.32bn | 594 contracts, all quote assets | 405 distinct bases |
| Variational | $2.32bn | $812m | unknown (429) | unknown |
| Lighter | $2.69bn | $693m | 209 | 105 |
| GRVT | $515m | $381m | not measured | not measured |
| Antarctic | $723m | $330m | not measured | not measured |
| GMX | $1.42bn | $221m | not measured | not measured |
| edgeX | $389m | $153m | not measured | not measured |
| Jupiter | $207m | $44.5m | not measured | not measured |
| dYdX | $15.7m | $38.3m | 57 ACTIVE | 57 |
| Paradex | $9.6m | $13.0m | not measured | not measured |
| Drift | $0.12m | $0.20m | not measured | not measured |

Matching is exact normalized ticker equality after stripping `1000`, `1000000`, `1M` prefixes (Hyperliquid also `k`); token identity and multipliers are not verified. Non-identical matches (measured): Hyperliquid `kPEPE`, `kSHIB`, `kBONK`, `kLUNC`, `kFLOKI` to the `1000*` symbols and `kNEIRO` to `NEIROUSDT`; dYdX `PEPE`, `BONK` to `1000PEPEUSDT`, `1000BONKUSDT`; Lighter `1000NOT` to `NOTUSDT`.

| Venue | Feed facts | Status |
|---|---|---|
| Hyperliquid | WS `trades`, `activeAssetCtx`, `allDexsAssetCtxs` (mark, oracle, OI, funding); POST `/info` `metaAndAssetCtxs`; limits 1,200 REST weight per min per IP, 10 WS connections and 1,000 subscriptions per IP. Liquidations only through `userEvents` or `userFills` for a given address; no market-wide feed established | doc; metadata measured |
| dYdX v4 | `/v4/trades/perpetualMarket/{ticker}`, WS `v4_trades`, `/v4/perpetualMarkets`, `/v4/historicalFunding/{ticker}`; mark, margin tiers and long/short not identified; limits unknown | doc; market list measured |
| Aster | Binance-style `/fapi/v1/exchangeInfo`, mark stream advertised as `@markPrice@1s`, example limit 2,400 weight per min; liquidations, tiers, long/short not established | doc; exchange info measured |
| Lighter | WS `trade/{market_index}` with `liquidation_trades`, `market_stats/all`, REST `recentTrades`, `orderBookDetails`, `fundings`; REST 60 requests per min, WS 255 connections, 500 subscriptions per connection, 5,000 per IP; liquidation completeness unverified | doc; market list measured |
| Variational | `GET /metadata/v2/stats` (mark, OI by side, funding, volume), 10 requests per 10 s per IP; current snapshot only | doc; live 429 |
| GRVT | WS `v1.trade`, ticker (mark, index, OI, funding), POST `/full/v1/trade`; public trade history retained three months; limits, liquidation feed and tiers not established | doc |
| Antarctic, GMX | no verified public developer API found or not investigated | unverified |

### Perp DEX liquidation records (live capture, 2026-11-12)

Replaces the earlier "no market-wide feed established" wording for these four venues. Two of the four captures were cut short by the subagent's own stop-on-403 rule, which was too strict; those parts are marked not captured. Completeness of every source below is unverified.

| Venue | Public market-wide liquidation record | Sample | History | Status |
|---|---|---|---|---|
| dYdX v4 | Yes. `GET https://indexer.dydx.trade/v4/trades/perpetualMarket/{ticker}`, trade `type` is `LIQUIDATED` or `DELEVERAGED`; WS `wss://indexer.dydx.trade/v4/ws` channel `v4_trades` per ticker | latest 100 trades for each of the 20 highest-volume markets: 2,000 trades, 3 `LIQUIDATED` (ZEC 1, BNB 2), 0 `DELEVERAGED` | `limit=1000` max, cursor `createdBeforeOrAtHeight` (inclusive, deduplicate); BTC reached 2024-03-04; a July 2025 page held one liquidation; earliest not established | measured; WS not captured |
| Lighter | Yes by REST. `GET /api/v1/trades?sort_by=timestamp&limit=100&type=liquidation` (and `type=deleverage`); WS `wss://mainnet.zklighter.elliot.ai/stream` `trade/{market_id}` per market, no market-wide liquidation channel | 400 liquidation records from 2026-11-12 10:24 to 2026-11-11 20:55 UTC; WS 10 minutes, 10 largest markets: 4,553 trade updates, 0 liquidations; the global REST query returned 7 liquidations in the same window, none in those 10 markets | deleverage records back to 2026-10-15 on the first page; public S3 `aws-public-blockchain/v1.1/lighter/core/mainnet/trades/date=YYYY-MM-DD/trades.parquet` exists for 2026-11-11 (anonymous), docs claim daily data from 2025-01-17; fields not inspected | measured |
| Hyperliquid | Only per user. `POST /info` `userFills` and `userFillsByTime` carry `liquidation: {liquidatedUser, markPx, method}`, also on the counterparty's fill. No hosted market-wide liquidation feed found; WS `trades` carries buyer and seller addresses | 13 marked fills in 2,000 returned for one address; 48 fills for a 2-second query around the liquidated account | `userFills` 2,000 most recent, `userFillsByTime` 10,000 most recent; S3 `hl-mainnet-node-data` (`node_fills_by_block`) anonymous listing returned HTTP 403 (requester-pays); docs describe a self-run non-validator node with `--write-fills` as the route to all fills | measured; WS 24-coin capture not completed |
| Aster | Documented only. `!forceOrder@arr` and `<symbol>@forceOrder` on `wss://fstream.asterdex.com/stream`, latest liquidation per symbol per 1,000 ms (sampled by design) | subscription acknowledged; 26.5 minutes, 0 liquidation messages; control streams delivered: `!markPrice@arr@1s` 1,429 messages, aggTrade 177, 481 and 142 for three symbols; the capture ended in an abnormal disconnect | `allForceOrders` v1 HTTP 400 ("out of maintenance"), v3 HTTP 404; no stream replay found | measured subscription; payload never observed |

Lighter: the three REST examples were GRAM liquidations (a small market), all with ask account 713043; the record alone was not verified to say which account was liquidated. Lighter trade records carry both account indices, and `GET /api/v1/account?by=index&value=` returns positions with `liquidation_price` (current state only). `/api/v1/liquidations` requires authorization.
dYdX: a public full node `block_results` (e.g. `https://dydx-rpc.publicnode.com/block_results?height=113505171`) showed a `match` event with `is_liquidation=true`, both subaccounts and quantities, matching the sampled BNB trade. `/v4/fills` for the liquidated account returned HTTP 403 (cause unknown), so its positions were not read. Indexer rate-limit header `ratelimit-limit: 100`.

Aster facts from the same capture: quote assets USDT, USD1, USDC, U (644 symbols, 594 TRADING: 562 USDT, 27 USD1, 3 USDC, 2 U); `exchangeInfo.rateLimits` 2,400 weight per minute; 24 h connection life, 200 streams per connection per docs; REST keyless and working: `exchangeInfo`, `premiumIndex` (799 entries), `openInterest` (snapshot), `fundingRate` (from 2021-08-27), `aggTrades` (V3 from 2026-01-01), `historicalTrades` (from 2021-08-29), `klines`; `leverageBracket` signed (V1 401, V3 400); long/short ratio path 404.

### Account-level positions (the Binance input gap)

Binance does not publish per-account leverage or liquidation prices ([liquidation-model.md](liquidation-model.md) Known input gaps). Per-address reads exist on some DEXes, but no venue was shown to allow keyless enumeration of all accounts.

| Venue | Known address | Enumerate all accounts |
|---|---|---|
| Hyperliquid | `clearinghouseState` public; WS state includes positions and margin | not established; trade messages carry buyer and seller addresses, deriving the full account set from them is unverified |
| dYdX v4 | `/v4/perpetualPositions?address=&subaccountNumber=` public (example account returned 200 with zero positions) | not established; liquidation trades and chain `match` events expose subaccounts that were liquidated |
| Lighter | `/api/v1/account?by=index&value=` public (measured: account 713043 returned 16 positions, 12 nonzero, each with `liquidation_price`) | not established; trade records carry both account indices, so accounts seen in trades can be read, completeness unverified |
| Aster, GRVT | account endpoints signed | not established |
| Variational, Antarctic, GMX | not established | not established |

## AMM DEXes in the index (2026-11-12)

Binance's index methodology names PancakeSwap on BNB Chain and Uniswap on Ethereum as sources ([FAQ 360033525071](https://www.binance.com/en-IA/support/faq/detail/360033525071), doc). Base and Arbitrum use was not established. The constituent response carries no pool address or V3 fee tier, so which pool Binance uses is unverified. Live RPC calls and pool-address matching were not done; no swap counts or data volumes were measured.

| Item | Fact | Status |
|---|---|---|
| Measured constituents | `BANANAS31USDT` `pancakeswapV3` `BANANAS31-WBNB*uindex(BNBUSDT)`; `SIRENUSDT` `pancakeswapV3` `SIREN-WBNB*uindex(BNBUSDT)`; `MYXUSDT` `pancakeswapV3` `uindex(BNBUSDT)/WBNB-MYX`. No example found for Uniswap or PancakeSwap V2 | measured |
| `uindex()` | Precise definition, timing, pool choice, smoothing and spot vs trade vs TWAP not found; Binance documents protective caps or smoothing | unverified |
| BNB Chain RPC | `https://bsc-dataseed.bnbchain.org`, 10,000 requests per 5 min, about 0.45 s blocks, `eth_getLogs` disabled on listed public endpoints, so log backfill needs another provider | doc |
| Ethereum RPC | provider-dependent; no free endpoint or quota verified; about 12 s slots | doc, unverified provider |
| V3 price | `slot0().sqrtPriceX96`, price token1/token0 = `(sqrtPriceX96 / 2^96)^2 × 10^(decimals0 − decimals1)`; `Swap` logs per swap; pool identity is token pair plus fee | doc |
| V2 price | `getReserves()` decimal-adjusted ratio is a mid price, not an execution price; record `Sync` as well as `Swap` | doc |
| Oracles | V2 cumulative price gives arithmetic TWAP; V3 mean tick gives geometric mean, observations at most once per block | doc |
| Subgraphs | The Graph gateway needs an API key, 100,000 free queries per month; suitable PancakeSwap deployments and history not verified | doc |
| Official DEX APIs | none established for these pools | unverified |

Recording design if needed (inferred): pool metadata and initial state (chain, contract, token addresses, decimals, ordering, V3 fee), then ordered swaps with block number and hash, transaction hash and index, log index, block timestamp and local receipt time; backfill disconnects and handle reorgs. Per-block state gives a block-time-weighted price but not intra-block trades. Also record Binance's constituent `price`, `weight`, response timestamp and the conversion index alongside chain data, to compare against on-chain reconstruction. Distinct pool count for the roughly 40 affected symbols is unverified (sets overlap, V3 fee tiers create several candidates).

## Open points

| Point | Note |
|---|---|
| Symbol mapping | String equality is unsafe; reviewed map and unresolved entries: [venue map](venue-map.md). |
| 403 responses | Initial OKX, MEXC (`/country`) and Coinbase Exchange calls returned HTTP 403; cause unknown. Later OKX and MEXC captures succeeded ([captures](#live-adapter-captures)). |
| Liquidation feeds | Complete: Bybit (doc). Sampled or incomplete: Bitget, OKX. Batched, not sampled per doc: Gate. None found: MEXC, KuCoin. |
| AMM DEX prices | Index constituents include `pancakeswapV3` (32 symbols), `pancakeswapV2` (3), `uniswapV2` (3), `uniswapV3` (2). Pool addresses, fee tiers, `uindex()` rule, a BNB Chain log provider, swap volume per pool: all unverified. |
| Perp DEX gaps | Variational, GRVT, Antarctic, GMX market counts; all liquidation-feed completeness; Hyperliquid builder-deployed DEXes; Binance total volume and open interest for scale. |
| Account enumeration | No venue shown to allow keyless enumeration of all accounts; Hyperliquid, dYdX and Lighter serve a known address only, but accounts seen in public trades can be read. |
| Perp DEX liquidation completeness | Unverified for all four. Not captured: dYdX and Hyperliquid WebSocket streams (cut short by the stop-on-403 rule), any Aster liquidation payload. |
