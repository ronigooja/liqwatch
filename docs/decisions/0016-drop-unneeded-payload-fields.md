# 0016 Drop unneeded payload fields

Verified: owner decision, 2026-11-15; refines the field filter in [0015](0015-recording-scope-and-granularity.md).

## Decision

- Verified: remove from ordinary recorded payloads every field not needed for modelling. Needed fields belong to trades, liquidations, mark, index, funding value and next time, open interest, leverage/risk tiers or long/short ratios, or key, order or interpret a value (trade ID, sequence, exchange timestamp, side or size unit).
- Verified: remove fields duplicating envelope symbol, channel or venue and channel constants. Keep periodic complete-frame samples unfiltered. Owner exceptions: keep MEXC trade `O` and Gate `is_internal` for the gap in splitting trades into openings and closings.
- Inferred: removal is irreversible because recording is live with no backfill; disk and traffic are not reasons to retain fields.

## Why

- Measured: in one seven-venue Singapore run on 2026-11-15, 5 minutes, 1,665,360 lines, 10,656 files and 601.7 MB, envelope `ch`, `sym`, `venue` were 13.58% of recorded bytes. Removing them saved 0.56–2.10% after zstd -3, three representative files per venue; sample: `root@64.118.141.231:/root/env-sample`; command not recorded.
- Verified: field classification used the venue-by-venue official API review (field review sub-agent reports, 2026-11-15); sample location above. Envelope changes are deferred, not decided here.

## Consequences

Verified: the following removals are decided, not built until the code branches merge; field review sub-agent reports, 2026-11-15.

| Venue | Channel | Removed fields |
|---|---|---|
| Binance | aggTrade | `stream`, `e`, `s`, `nq`, `st` |
| Binance | markPrice | `stream`, `e`, `s`, `st`, `ap`, `P` |
| Bybit | mark | `topic`, `data.symbol`, `type`, `fundingIntervalHour`, `fundingCap` |
| Bybit | trade; liquidation | Both: `topic`, `type`; trade also: `BT`, `RPI` |
| OKX | all | `arg.channel`, `arg.instId`, `instType`, `data[].instId` (keep `data[].instId` on index, where it is the index ID) |
| OKX | funding | `nextFundingRate`, `settFundingRate`, `settState`, `formulaType`, `method`, `impactValue`, `interestRate`, `maxFundingRate`, `minFundingRate`, `premium`, `prevFundingTime` |
| OKX | trade; liquidation | Trade: `source`; liquidation: `arg.instType`, `uly`, `instFamily`, `details[].ccy`, `details[].bkLoss` |
| Bitget | all; trade | All: `arg.instType`, `arg.topic`, `arg.symbol`, `data.symbol`/`data[].symbol`; trade also: `L`, `isRPI` |
| Gate | all WebSocket; trades | All: `result.contract`, `channel`, `event` except on contract_info; trades also: `create_time` |
| Gate | tickers; contract_info | Tickers: `funding_rate_indicative`, `event_type`, `funding_interval`, `funding_offset`, `total_size`; contract_info: `order_price_round`, `mark_price_round` |
| MEXC | all; trade | All: outer `symbol`, `channel`, `data.symbol`; trade also: `M` |
| MEXC | contract; contractextra | Contract: static metadata (names, scales, order limits, fees, tags, state, fee and index-construction fields); `contractextra` is no longer recorded. |
| KuCoin | trade; mark; utamark | Trade: `topic`, `type`, `subject`, `data.symbol`; mark: `topic`, `type`, `subject`; utamark: `T`, `d.s` |

Verified: keep questioned fields Bybit `cs`, `singleOpenInterest*`; Binance `f`, `l`; MEXC `cts`; Gate `funding_next_apply`. Binance and MEXC trade paths use explicit keep/drop tables (not built). Open questions: [recording issues](../open-questions.md#recording-open-issues).
