# 0016 Drop unneeded payload fields

Verified: owner decision, 2026-11-15; refines the field filter in [0015](0015-recording-scope-and-granularity.md).

## Decision

- Verified: remove from ordinary recorded payloads every field not needed for modelling. Needed fields belong to trades, liquidations, mark, index, funding value and next time, open interest, leverage/risk tiers or long/short ratios, or key, order or interpret a value (trade ID, sequence, exchange timestamp, side or size unit).
- Verified: remove fields duplicating envelope symbol, channel or venue and channel constants. Keep periodic complete-frame samples unfiltered. Owner exceptions: keep MEXC trade `O` and Gate `is_internal` for the gap in splitting trades into openings and closings.
- Inferred: removal is irreversible because recording is live with no backfill; disk and traffic are not reasons to retain fields.

## Why

- Measured: in one seven-venue Singapore run on 2026-11-15, 5 minutes, 1,665,360 lines, 10,656 files and 601.7 MB, envelope `ch`, `sym`, `venue` were 13.58% of recorded bytes. Removing them saved 0.56–2.10% after zstd -3, three representative files per venue; command not recorded.
- Verified: field classification used the venue-by-venue official API review (field review sub-agent reports, 2026-11-15). The sample was at `root@64.118.141.231:/root/env-sample`, not in the repo; the host is no longer reachable (owner report). Envelope changes are deferred.

## Consequences

Verified: field filters are built; the linked code tables are authoritative for the complete keep/drop lists. Unknown fields are retained and counted. The rows highlight removals and exceptions, not exhaustive lists except for Gate's listed policies. Binance forceOrder and MEXC ordersetting/contractmeta remain unfiltered; see linked code.

| Venue | Channel | Removed fields |
|---|---|---|
| [Binance](../../internal/venue/binance/split.go) | aggTrade; markPrice_all | `stream`; aggTrade: `e`, `s`, `nq`, `st`; mark: `e`, `s`, `st`, `ap`, `P`. Both keep the `{"data":{...}}` wrapper; `f`, `l` are kept. |
| [Bybit](../../internal/venue/bybit/bybit.go) | mark; trade; liquidation | All lose `topic`, `type`; mark loses `data.symbol`, funding interval/cap and ticker statistics; trade loses `BT`, `RPI`, `L`. Keep `cs` and `singleOpenInterest*`. |
| [OKX](../../internal/venue/okx/okx.go) | all; index | The empty `arg` is omitted; ordinary records are `{"data":[{...}]}`. Item `instId` and `instType` are dropped except index `instId`, which is kept. Index statistics are dropped. |
| OKX | funding; trade | Funding drops the estimate, settlement, formula, limit and premium fields in the linked table; trade drops `source`. |
| OKX | liquidation | Item: `instId`, `instType`, `uly`, `instFamily`; details: `ccy`, `bkLoss`. |
| [Bitget](../../internal/venue/bitget/bitget.go) | all; trade | Drop `arg.instType`, `arg.topic`, `arg.symbol`, item `symbol`; omit `arg` when empty. Trade drops `L`, `isRPI`; ticker and funding drop additional statistics and funding inputs in the linked table. |
| [Gate](../../internal/venue/gate/gate.go) | envelope; trades; liquidations; contract_stats | Envelope: `channel`, `conn_id`, `trace_id`, and `event` except contract_info. All items: `contract`; trades also: `create_time`. Keep trade `is_internal`. |
| Gate | tickers; contract_info | Both drop `last`, `change_percentage`, `volume_24h`, `volume_24h_base`, `volume_24h_quote`, `volume_24h_settle`, `quanto_base_rate`, `low_24h`, `high_24h`, `highest_bid`, `highest_size`, `lowest_ask`, `lowest_size`, `bid`, `ask`, `bid_size`, `ask_size`. |
| Gate | tickers; contract_info | Tickers additionally drop `funding_rate_indicative`, `funding_interval`, `funding_offset`, `total_size`, `event_type`, `price_type`, `change_from`, `change_price`; contract_info drops `order_price_round`, `mark_price_round`. Keep `funding_next_apply`. |
| [MEXC](../../internal/venue/mexc/mexc.go) | trade; mark; index; funding; ticker | Drop outer `symbol`, `channel` and item `symbol`; trade drops `M` and keeps `O`, `cts`. Ticker drops quote and statistics fields in the linked table. |
| MEXC | contract; contractextra | Contract drops static metadata in the linked table; keep `riskLimitCustom`. `tempMaxLeverageLimited` and `regularMaxLeverage` are retained and counted as unknown; their meaning is undefined in official docs ([fixture notes](../../internal/venue/mexc/testdata/README.txt)). `contractextra` is not recorded. |
| [KuCoin](../../internal/venue/kucoin/kucoin.go) | trade; mark; utamark | Trade drops `topic`, `type`, `subject`, `data.symbol` and order metadata; mark drops `topic`, `type`, `subject` and cadence; utamark drops `T`, `d.s` and quote/statistics fields in the linked table. |

Verified: Binance and MEXC trade paths use explicit keep/drop tables. Open questions: [recording issues](../open-questions.md#recording-open-issues).
