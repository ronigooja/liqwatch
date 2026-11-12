# Venue APIs (non-Binance)

Archive of keyless public API facts for venues other than Binance, surveyed on 2026-11-12 by subagents. Governing context: [liquidation-model.md](liquidation-model.md) Multiple venues, [0010](decisions/0010-liquidation-model-direction.md) item 7, [0004](decisions/0004-defer-game-layers-and-multi-venue.md). Recording another venue needs a new decision record.

Status labels: **measured** = a live keyless call succeeded on 2026-11-12; **doc** = stated in the official documentation, not tested live; **unverified** = not found or not tested. WebSocket behavior was not tested live for any venue. Counts are response-filter counts, not tradable-volume counts. Binance intersection counts were not computed for any venue.

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
| Open interest | Y, hist from 5 m | Y, 3 s; hist 1,440 rows | snapshot only | Y, hist | snapshot only (`holdVol`) | Y, hist 7 d | book summary |
| Funding | Y | Y | Y | Y | Y | Y | Y |
| Risk tiers | Y, current | Y, current | Y, current | Y, current | Y, current | Y, current | no public complete table |
| Long/short ratio | Y, from 2020-07-20, 5 m | Y, 5 m | Y, 5 m, depth unspecified | via `contract_stats` | N | N | ? |

No venue documents historical risk-tier snapshots, so tiers must be recorded as snapshots.

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
| History | trades 3 months, 100 per page; OI 1,440 rows, 5 m minimum; funding 3 months, 400 per page; mark and index 1 m candles | doc |
| Limits | REST history 10 to 20 requests per 2 s per IP; WS 3 connection attempts per s per IP, 480 subscribe requests per connection per hour, 64 KB args; 30 s idle close; connections per IP and forced reconnect not found | doc |
| Units | swap sizes are in contracts, spot in base currency | doc |
| Live access | two keyless instrument-list calls returned HTTP 403; live testing stopped; counts not measured | measured (403), cause unverified |

### Bitget
| Item | Value | Status |
|---|---|---|
| Endpoints | V3 WS `publicTrade`, `ticker` (mark, index, OI), `liquidation`, `funding-rate`, `position-tier`; V2 REST `/api/v2/mix/market/*` | doc; REST measured |
| Liquidation | once per second, at most two records per pair per second (largest long and largest short); sampled; no REST history | doc |
| History | trades 90 days, 7-day window, 1,000 per page (spot timestamps second-level); candles 1 m, 90-day range; OI snapshot only | doc |
| Limits | V2 WS: 300 connection requests per 5 min per IP, 100 connections per IP, 1,000 subscriptions per connection (under 50 recommended), ping every 30 s; V3 limits not established; V3 trade snapshot on subscribe is removed on 2026-11-17 | doc |
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
| Live access | `/api/v1/contract/detail/country` returned HTTP 403; stopped; cause unverified | measured |
| Universe | 1,104 enabled USDT contracts (perpetual type not filtered); spot 1,790 symbols, 1,412 USDT-quoted; `PEPE_USDT` contract size 10,000,000 | measured |

### KuCoin
| Item | Value | Status |
|---|---|---|
| Endpoints | classic futures WS `/contractMarket/execution:{symbol}`, `/contract/instrument:{symbol}` (mark, index, funding); spot WS `/market/match:{symbol}`; token from `POST /api/v1/bullet-public` (keyless, measured); UTA V2 `mark-price` channel includes `oi` | doc; token and REST measured |
| Missing | no public liquidation feed and no long/short ratio found | unverified |
| History | trades last 100 only; OI intervals from 5 m, 7 days (daily 70 days), data begins no earlier than 2025-12-29; funding history by time range, REST auth unclear | doc |
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

## Open points

| Point | Note |
|---|---|
| Symbol mapping | String equality is unsafe: multipliers (`1000PEPEUSDT` vs `PEPE_USDT` at 10,000,000 or 520,000), aliases (XBT for BTC), quote currency. Needs a reviewed mapping built from each venue's instrument list and Binance `exchangeInfo`. |
| Binance overlap counts | Not computed for any venue. |
| 403 responses | OKX, MEXC (`/country`) and Coinbase Exchange returned HTTP 403 to keyless calls; cause (regional block, rate rule or client filter) unknown. Recording from the recorder host needs a test from that host. |
| Liquidation feeds | Complete: Bybit (doc). Sampled or incomplete: Bitget, OKX. Batched, not sampled per doc: Gate. None found: MEXC, KuCoin. |
| DEX | Not surveyed. Index constituents include `pancakeswapV3` (32 symbols), `pancakeswapV2` (3), `uniswapV2` (3), `uniswapV3` (2) and `aster` (1) in the measured table above; overlap between these sets not computed. |
