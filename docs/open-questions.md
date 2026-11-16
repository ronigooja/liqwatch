# Open Questions and Unverified Facts

Move an item to "Verified" with source and date once checked; field handling is governed by [0017](decisions/0017-poller-field-policies.md).

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

- Request weight of `premiumIndex` (official pages returned no usable content); the exact oldest `openInterestHist` timestamp (see [survey](venue-apis.md#open-interest)); official units of `sumOpenInterest` and `sumOpenInterestValue`; whether a read-only API key can call `leverageBracket`; the text of the liquidation-price formula (the FAQ shows it as an image); whether `exchangeInfo` `maintMarginPercent` equals the first bracket rate.
- No public per-trader leverage data on Binance was found. DEX survey (one-off, 2026-11-11, via subagent, not collected): Hyperliquid `clearinghouseState` returns size, entry price, leverage setting, margin used and an optional liquidation price for a known address (POST api.hyperliquid.xyz/info, 2 weight, 1,200 weight/min/IP); dYdX v4 indexer returns size, entry price and side per address and subaccount, no leverage or liquidation price; GMX v2 Reader has paginated `getAccountPositions`; Velocity (formerly Drift) documents a subscriber over all user accounts. No venue was shown to give a complete keyless account list; population differs from Binance traders, so use only as a sensitivity reference.
- Other venues (Bybit, OKX, Bitget, Gate, MEXC, KuCoin, Coinbase): whether each publishes a liquidation feed (and whether it is sampled), open interest, leverage brackets, mark-price formula, rate limits; Binance's share of each symbol's total open interest: measured against six venues on 2026-11-12, 32.3% weighted ([venue-apis.md](venue-apis.md#binance-share-of-open-interest-measured-2026-11-12)); the rest unchecked. See [liquidation-model.md](liquidation-model.md) Multiple venues.
- Liquidation mechanics still unverified: the definition of "Contract Price" in the mark-price median (page text was truncated); whether the median actually bounds mark price during contract-only pushes (testable on recorded `mark` and `trades`); maintenance margin tiers and leverage brackets (`/fapi/v1/leverageBracket` returns 401 without a signed key); the liquidation clearance fee rate; ADL ranking details. See [liquidation-model.md](liquidation-model.md).

- Tardis.dev current pricing, plans, and free-sample policy.
- Legal and terms-of-use status of API access and automated trading in the owner's jurisdiction.


## REST survey open questions

| Question | Evidence and scope |
|---|---|
| Unknown: how closely do Binance snapshots match history across symbols and longer periods? | Measured: BTCUSDT only, 2026-11-14 08:30–09:00 UTC, 60 snapshots; nearest differed 1.332–15.822 BTC (0.0014%–0.0164%), none exact; spacing not strictly under 2/min. Source: user-provided survey, request logs unavailable. |
| Unknown: are published history rows ever revised, and how often? | Measured: OKX revised one row in two reads one minute apart; Binance overlapping rows unchanged in one recheck. Source: user-provided survey, 2026-11-14; logs unavailable. |
| Unknown: how frequent are Gate source-series gaps? | Measured: 1,994–1,997 rows for a 2,000-row request; one source minute missing. Source: user-provided survey, 2026-11-14; logs unavailable. |
| Unknown: what used-weight applies to Binance funding and website brackets? | Survey did not establish used-weight; [endpoints](venue-apis.md#funding-history). |
| Unknown: exact retention of several funding histories? | Observed bounds only: [funding history](venue-apis.md#funding-history). |

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
| Trial load evidence: 2 vCPU / 4 GB, 522 `aggTrade` + `markPrice` + `forceOrder` + 10 depth symbols + `bookTicker` top 50; CPU mean 18%, peak 53% of one core, 0 drops | [trial-plan](trial-plan.md) Results | 2026-11-02 |
| Futures WebSocket: no documented per-IP connection cap or connection-attempt rate; undocumented limits remain possible. 1024 streams and 10 incoming messages/s per connection are documented; IP bans scale from 2 minutes to 3 days; the docs give no guidance on overlap when replacing a connection | developers.binance.com futures websocket-market-streams/Connect.md and general-info.md, via subagent | 2026-11-01, 2026-11-04 |
| USDⓈ-M perpetual mark price = Median(Price 1, Price 2, Contract Price); Price 1 = Price Index × (1 + Last Funding Rate × Time Until Next Funding / Funding Period); Price 2 = Price Index + Moving Average (1-minute basis), 60 points, one per second, basis = (Bid1 + Ask1)/2 − Price Index; the 1-minute basis replaced the 2.5-minute basis from 2025-05-05 09:00 UTC | binance.com support FAQ 360033525071 and announcement of 2025-05-02, via search excerpts | 2026-11-10 |
| Liquidation is triggered when mark price reaches the liquidation price; the executed price can differ when mark price jumps; liquidation condition is collateral (initial + realized + unrealized PnL) below maintenance margin | binance.com support FAQ 360033525271 (Futures Liquidation Protocols), via search excerpts | 2026-11-10 |
| Insurance funds cover only the gap between bankruptcy price and execution price; when a fund is insufficient, ADL ranks opposing positions by leverage and profitability; BTC/ETH/BNB share one USDT fund, a second group (DOT, LINK, XMR, ADA, BCH, EOS, ETC, LTC, TRX, XLM, XRP) shares another, other USDT contracts use other pools | binance.com support FAQ 360033525371 (Introduction to Insurance Funds), via search excerpts | 2026-11-10 |
| `GET /fapi/v1/openInterest?symbol=` is public and returns `openInterest` (base units) and `time`; `GET /futures/data/openInterestHist` is public and returns `sumOpenInterest`, `sumOpenInterestValue` per period (5m tested) | Live calls from the research Mac | 2026-11-10 |
| `openInterestHist` row fields: `symbol`, `sumOpenInterest`, `sumOpenInterestValue`, `CMCCirculatingSupply`, `timestamp` (ms); aggregate only, cannot separate opening from closing. Later page-size and window observations: [survey](venue-apis.md#open-interest). | developers.binance.com Open-Interest-Statistics; live calls | 2026-11-11 |
| REST used-weight header is named `x-mbx-used-weight-1m` (seen on `openInterest`; pattern `X-MBX-USED-WEIGHT-(intervalNum)(intervalLetter)` in the general docs); history responses carried none | Live calls; developers.binance.com futures general-info, via subagent | 2026-11-11 |
| `GET /fapi/v1/leverageBracket` is USER_DATA (API key and signature required); unsigned call returns HTTP 401; at that time no public source of the per-symbol bracket table was found (`exchangeInfo` carries only a scalar `maintMarginPercent`, BTCUSDT "2.5000"); the later keyless website endpoint is recorded below; maintenance margin = notional × tier rate − maintenance amount; the authenticated response has `maintMarginRatio` and `cum` | developers.binance.com Notional-and-Leverage-Brackets and general-info; binance.com support FAQ b3c689c1f50a44cabb3a84e663b81d93; live GET, via subagent | 2026-11-11 |
| BTCUSDT tiers (position value in USDT, max leverage, maintenance margin rate, maintenance amount): 0-300k 150x 0.40% 0; 300k-800k 100x 0.50% 300; 800k-3M 75x 0.65% 1,500; 3M-12M 50x 1.00% 12,000; 12M-70M 25x 2.00% 132,000; 70M-100M 20x 2.50% 482,000; 100M-230M 10x 5.00% 2,982,000; 230M-480M 5x 10.00% 14,482,000; 480M-600M 4x 12.50% 26,482,000; 600M-800M 3x 15.00% 41,482,000; 800M-1.2B 2x 25.00% 121,482,000; 1.2B-1.8B 1x 50.00% 421,482,000. Rule limits, not trader leverage: every symbol has its own table. Maintenance amounts satisfy amount(k+1) = amount(k) + lower(k+1) × (rate(k+1) − rate(k)). `exchangeInfo` `maintMarginPercent` 2.5% is tier 6, not tier 1 | Owner copy of the public page binance.com/en/futures/trading-parameters/perpetual/leverage-margin (page renders by script, plain GET returns empty) | 2026-11-11 |
| All-symbol tier tables without a key: `POST https://www.binance.com/bapi/futures/v1/friendly/future/common/brackets` with body `{}` returned HTTP 200 and 1,081 contracts (919 end in USDT). Per symbol: `symbol`, `updateTime` (ms), `notionalLimit`, `riskBrackets[]` with `bracketSeq`, `bracketNotionalFloor`, `bracketNotionalCap`, `bracketMaintenanceMarginRate`, `cumFastMaintenanceAmount`, `minOpenPosLeverage`, `maxOpenPosLeverage`. BTCUSDT matches the owner copy; 6,151 tier rows for 756 page-listed USDT perpetuals pass the maintenance-amount recurrence. Tier 1 differs widely: max leverage from 150x (2 symbols) to 10x; maintenance rate 0.5% for 18 symbols, 1.5% for 180, 2.5% for 204. This is the website's internal endpoint, not a documented API, so it can change without notice. Not reconciled: 756 vs 919 vs the 900 USDT symbols in the mark stream; meaning of `minOpenPosLeverage` | Own POST on 2026-11-11 plus rendered-page capture, via subagent | 2026-11-11 |
| Open interest counts each contract once: Binance defines it as the total number of outstanding perpetual contracts not yet settled, and its Academy example has one trader buy 10 new contracts and another sell 10, raising open interest by 10 (not 20); a transfer of an existing contract (one side opening, the other closing) leaves it unchanged; two closing parties lower it. So a rise of d means d new long and d new short units. Mapping of this convention to the `sumOpenInterest` field is inferred, not stated; units (base asset for `sumOpenInterest`, USDT for `sumOpenInterestValue`) are only consistent with quantity × price (BTCUSDT ratio about 73,927 on 2026-11-11), not documented | binance.com support FAQ how-to-use-the-funding-rate-arbitrage-on-binance-futures-61012e690cf343e7979649282a2ccc3c; academy.binance.com what-is-open-interest; live call, via subagent | 2026-11-11 |
| Public history, keyless, tested 2026-11-11 (BTCUSDT dates 2026-10-01, 11-04, 11-07, 11-09, 11-10; HOTUSDT 11-09), base `https://data.binance.vision/data/futures/um` (A) and `https://fapi.binance.com/fapi/v1` (R): daily `A/daily/aggTrades/S/S-aggTrades-D.zip` exists (columns `agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker`; HOT 11-09 ids contiguous; compressed 18.8 MB BTC, 5.2 MB HOT; file timestamps about 8 h after the day ends) while `R/aggTrades` answers only the last 2 days (-4166); 1-minute `markPriceKlines`, `indexPriceKlines`, `premiumIndexKlines` in A and R, 1-second mark file 404; `R/fundingRate` and monthly funding files; `A/daily/metrics/S/S-metrics-D.zip` (5-minute open interest and ratios, BTC back to 2023-01-01, rows not in time order); `A/daily/bookDepth` is percentage-bucket depth (`timestamp, percentage, depth, notional`), not price levels; `A/daily/bookTicker` and `A/daily/liquidationSnapshot` return 404 on all tested dates; `R/allForceOrders` returns 404 and the change log of 2021-04-27 says it no longer accepts requests; keyless S3 listing `https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/daily/aggTrades/BTCUSDT/` works with `NextMarker` paging. Not verified: completeness beyond the one sampled day, any public history of brackets or `exchangeInfo`; later history weight observations: [survey](venue-apis.md#open-interest). Consequence: liquidation orders and the 1-second mark price cannot be rebuilt from public data | developers.binance.com change-log and market-data docs; live requests, via subagent | 2026-11-11 |
| `A/daily/metrics` column `sum_toptrader_long_short_ratio` equals the API `topLongShortPositionRatio`, with the file stamp one 5-minute step later than the API stamp (ZECUSDT 2026-11-06 01:00 to 01:35: shifted by 5 minutes the mean absolute difference is 0.00002, unshifted 0.002; 8 points); `count_toptrader_long_short_ratio` has the magnitude of `topLongShortAccountRatio` and `count_long_short_ratio` of the global account ratio, not checked point by point | live GET and archive file, 2026-11-12 (this session, on the poller host) | 2026-11-12 |
| `topLongShortPositionRatio`, `topLongShortAccountRatio`, `globalLongShortAccountRatio`, `takerlongshortRatio` under `/futures/data/` are public, 30 days of history, and give ratios or volumes, not leverage used | developers.binance.com market-data docs; live GET (BTCUSDT 5m), via subagent | 2026-11-11 |
| `GET /fapi/v1/premiumIndex?symbol=` is public and returns `markPrice`, `indexPrice`, `estimatedSettlePrice`, `lastFundingRate`, `nextFundingTime`; `GET /fapi/v1/constituents?symbol=` is public and returns each index venue with price and weight | Live calls from the research Mac | 2026-11-10 |
| Index composition differs by symbol: BTCUSDT has 8 venues, Binance spot weight 0.43; FLOCKUSDT has 4 venues (gateio, mexc, bitget, bybit), no Binance; XANUSDT has 6 including coinbase and `binance_alpha`. Weights are fixed fractions that change over time | Live `constituents` calls | 2026-11-10 |

## Recording open issues

| Question | Evidence |
|---|---|
| Unknown: is sustained machine CPU saturation a risk? | Measured: one run reached 100% maximum; [recording results](recording-results.md#seven-venue-load-and-recording). |
| Unknown: why did Bybit peak RSS increase? | Measured: the newer run's maximum exceeded the 2026-11-14 run; cause not investigated; [recording results](recording-results.md#seven-venue-load-and-recording). |
| Unknown: does sub-universe trade coverage reflect market activity? | Measured: Bitget, Gate, Bybit and KuCoin had incomplete trade coverage; not checked against market activity; [recording results](recording-results.md#seven-venue-load-and-recording). |
| Unknown: how should subscription errors be counted? | Measured: collector subscription-error counters were not exposed; [recording results](recording-results.md#seven-venue-load-and-recording). |
| Resolved: redundant raw envelope identity | Verified: owner authorized removal; current format and compatibility: [storage design](storage-design.md#writing), decision [0018](decisions/0018-path-based-raw-envelope.md). |
| Unknown: should duplicate values be recorded from both Bitget ticker and funding, MEXC ticker and mark/index/funding, or KuCoin mark and utamark? | Measured: KuCoin classic and UTA matched on 248 timestamps for one symbol only (field review sub-agent reports, 2026-11-15). |
| Unknown: which newly observed polled fields should be classified? | Verified in [0017](decisions/0017-poller-field-policies.md): payload fields are filtered, while unknown fields are retained and counted. |
| Unknown: where is KuCoin next-funding time available? | Measured: the WebSocket sample showed no next-funding-time field (field review sub-agent reports, 2026-11-15). |
| Unknown: who sent the Gate connection resets? | Inferred: unread frames during subscription are the most likely explanation, not proven; [measurements](recording-results.md#seven-venue-load-and-recording). |
| Unknown: does the Binance poller deduplicate on a real second fetch? | Measured: the [second-run check](recording-results.md#seven-venue-load-and-recording) did not fetch because jobs were not due. |
| Unknown: how do OKX, Bitget and Gate sources finalize newest buckets? | Verified deferral implementation: [history](../internal/poller/history.go). |
| Unknown: how should KuCoin Chinese-named symbols be handled? | Measured: none in the captured contract list, [fixture](../internal/venue/kucoin/testdata/README.txt), 2026-11-13. |
| Unknown: why is Gate WebSocket `contract_stats` discontinuous and `mark_price` zero? | Measured capture: [fixture](../internal/venue/gate/testdata/README.txt); REST coverage: [poller](poller.md). |
| Unknown: will Bitget's connection-per-IP limit constrain deployment? | Verified: [official quick start](https://www.bitget.com/docs/uta/quick-start), as linked in [fixture](../internal/venue/bitget/testdata/README.txt), 2026-11-13. |
| Unknown: what staging disk and CPU are needed over a full day? | Inferred: short-run estimates are extrapolations, not full-day measurements; [storage design](storage-design.md#local-disk-size). |

## Poller field policy questions

| Question | Evidence and scope |
|---|---|
| Unknown: do current history periods preserve every eligible row before upstream expiry? | Verified: [0019](decisions/0019-live-rest-recording-scope.md) leaves periods unchanged and request spreading not built. Inferred: completeness requires endpoint-specific retention and finalization evidence; an observed page size is not universal retention. |
| Unknown: why did Bitget return HTTP 429 in the production-scheduler trial? | Measured: exact first-attempt endpoint/symbol failures are in [trial results](recording-results.md#rest-production-scheduler-trial), separate from the earlier field-review sample. Unknown: cause; retained evidence lacks response headers/body and exact request spacing. |
| Unknown: will Bitget rate limits constrain production? | Verified 2026-11-15: official [trading-insight endpoints](https://www.bitget.com/docs/catalog/classic-common-apidata/classic-common-apidata) specify 1 request/s/IP each. Verified: [poller](../internal/poller/bitget.go) defaults to 1 request/s per channel; [HTTP wrapper](../internal/poller/venue.go) already handles `Retry-After`. |
| Unknown: why is OKX currency `CT` unavailable to account ratio? | Measured: [trial](recording-results.md#rest-production-scheduler-trial) recorded `51012: Token does not exist` for `CT-USDT-SWAP`. Verified: [fix scope](poller.md#operation) is a channel/symbol cooldown, not instrument or mapping deletion. |
| Resolved: why are the five selected MEXC symbols missing from contract detail? | Verified: official delisting notices establish the bounded cause, not bulk absence alone; [facts and retained evidence](#mexc-delisted-mapping-associations). Poller endpoint unchanged. |
| Unknown: which Gate ratio descriptions are correct? | Verified: official [REST](https://www.gate.com/docs/developers/apiv4/en/futures/) and [WebSocket](https://www.gate.com/docs/developers/futures/) descriptions of `lsr_taker` and `lsr_account` are reversed relative to each other (field audit sub-agent reports, 2026-11-15). |
| Unknown: what does OKX contract long/short ratio measure? | Verified: [official wording](https://www.okx.com/docs-v5/en/#trading-statistics-rest-api-get-long-short-ratio-contract) is inconsistent (field audit sub-agent reports, 2026-11-15). |
| Unknown: what do KuCoin maintenance constants mean? | Verified: `k`, `m`, `f`, `mmrLimit`, `mmrLevConstant` are undefined in the [official contract field tables](https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-all-symbols) and dropped ([policy](../internal/poller/kucoin_policy.go)); [cross-margin formula example](https://www.kucoin.com/support/35084093025049) does not map them to JSON fields. |
| Unknown: how are KuCoin cross-margin tiers obtained? | Verified: the [risk-limit endpoint](https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-isolated-margin-risk-limit) covers isolated margin only (field audit sub-agent reports, 2026-11-15). |
| Unknown: what are the Binance website `brackets` field definitions? | Verified: no official endpoint documentation found in the field audit, 2026-11-15; the documented counterpart is signed [`/fapi/v1/leverageBracket`](https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Notional-and-Leverage-Brackets). |
| Unknown: what is the contract multiplier for Binance and Bybit USDT contracts? | Verified: the field audit found no official multiplier field for Binance [exchangeInfo](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information) or Bybit [instruments](https://bybit-exchange.github.io/docs/v5/market/instrument), 2026-11-15; do not assume one contract equals one base unit. |
| Unknown: which Binance `forceOrder_all` fields can be removed? | Verified: the [collector](../internal/venue/binance/split.go) leaves it unfiltered; no recorded sample was available (field audit sub-agent reports, 2026-11-15). |
| Unknown: can omitted fields be recovered? | Inferred: recording is irreversible for omitted fields; historical backfill is not built ([poller](poller.md#operation)). |

## MEXC contract-detail audit

| Fact or question | Source and bounded scope | Date |
|---|---|---|
| Verified audit: `tempMaxLeverage`, `tempMaxLeverageReason`, `tempFeeRateEffectiveEndTime` are not defined in the three references below; their semantics remain unknown. | Official references: legacy contract detail [M1]; current contract information/detail-country documentation [M2], [M3]. Audit by implementing agent `ses_e21323287ffeDGIOzOQBPwCH1t`; not a claim that no definition exists elsewhere. | 2026-11-15 |
| Verified: these three fields are explicitly dropped under the existing audited-undefined rule, not assigned inferred meanings. | [0017](decisions/0017-poller-field-policies.md); [policy](../internal/poller/mexc_policy.go), implementation commit `69df3a5`. Other unknown fields remain retained and counted. | 2026-11-15 |
| Verified: audited sample provenance is `XAUT_USDT`: `tempMaxLeverage=1000`, `tempMaxLeverageReason="EXTRA_SCHEDULE"`; `CC_USDT`: `tempFeeRateEffectiveEndTime=1796032860000`. | Implementing-agent audit above, retained [REST trial](recording-results.md#rest-production-scheduler-trial) `data/raw/mexc/`; regression fixture `internal/poller/testdata/mexc/audited-undefined.jsonl` in `69df3a5`. Values are samples, not definitions. | 2026-11-15 |

[M1]: https://mexcdevelop.github.io/apidocs/contract_v1_en/#get-the-contract-information
[M2]: https://www.mexc.com/api-docs/futures/market-endpoints/get-contract-info
[M3]: https://www.mexc.com/zh-hans/api-docs/futures/market-endpoints/get-contract-info

## MEXC delisted mapping associations

| Fact | Official source and bounded scope | Checked |
|---|---|---|
| Verified: CROSS USDT perpetual delisting effective 2026-11-13 13:00 UTC. | [M4]; supports removing only its MEXC association, not the Binance coin or other venues. | 2026-11-15 |
| Verified: CTK and RIF USDT-M perpetual delistings effective 2026-11-14 07:00 UTC. | [M5]; other contracts in the notice are outside this mapping correction. | 2026-11-15 |
| Verified: XMR and DASH USDT-M perpetual delistings effective 2026-11-13 10:00 UTC. | [M6]; no claim about later relistings. | 2026-11-15 |
| Verified: successful official HTML and request metadata were retained, not freshly fetched for this change. | Original `/root/liqwatch-mexc-symbols-20261115T112255Z/`; inspected local copy `/tmp/liqwatch-mexc-symbols-docs-20261115T112255Z/`: `announcement-*.body`, headers, `requests.json`. | 2026-11-15 |

[M4]: https://www.mexc.com/announcements/article/delisting-of-crossusdt-perpetual-futures-pair-nov-13-2026-13-17827791539671
[M5]: https://www.mexc.com/announcements/article/delisting-of-ctk-puffer-epic-dna-baton-cnpy-ritual-hooked-nex-rif-kii-up-and-xten-usdt-m-perpetual-futures-pairs-nov-14-2026-07-17827791539672
[M6]: https://www.mexc.com/announcements/article/delisting-of-xmr-and-dash-usdt-m-perpetual-futures-pairs-nov-13-2026-10-17827791539677
