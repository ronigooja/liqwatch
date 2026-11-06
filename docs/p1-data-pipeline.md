# Data Pipeline: raw log to training and evaluation

One page for the whole chain after recording. Recording itself: [p0-core-design.md](p0-core-design.md). Labels and metrics: [labels-and-evaluation.md](labels-and-evaluation.md). Models: [models.md](models.md). Status values: `done`, `todo`.

## Chain

| # | Stage | Input | Output | Component | Status |
|---|---|---|---|---|---|
| 1 | Record | Binance streams | Raw log `raw/<date>/<group>-<YYYYMMDDHH>[.<slot>][.rN].jsonl.zst` | 2, 3, 4 | done |
| 2 | Archive | Raw log | Remote copy, verified, local delete after 6 h | 6, 7 | done |
| 3 | Replay | Raw log, validation log | Event stream in causal order; `book_at(symbol, t)` | 8 | todo |
| 4 | Validate | Raw log | Validation log: sequence breaks, snapshot mismatches, unavailable intervals | 5 | todo |
| 5 | Convert | Replay output | Columnar files for training (layout below) | 6 | todo |
| 6 | Label | Columnar files | Four label tasks per time step | 12, 13 | todo |
| 7 | Train and evaluate | Features, labels | Metrics and calibration | 15, P2 | todo |

Rule that every stage keeps: a value at time t may use only messages with receive time at or before t ([architecture.md](architecture.md) Invariants). Replay enforces it; later stages must not bypass replay.

## Raw record format

Envelope, one JSON object per line: `v, wseq, conn, cseq, rw, rm, ch, sym, p`. Meaning and ordering: [p0-core-design.md](p0-core-design.md) Component 4. `p` is the exchange payload untouched.

Checked on recorded data, 2026-11-06:

| `ch` | File group | `sym` | Payload `p` |
|---|---|---|---|
| `aggTrade` | `trade` | symbol | `{stream, data:{a, s, p, q, f, l, T, m, E}}` |
| `markPrice_all` | `trade` | empty | `{stream, data:[{s, p, ap, P, i, r, T, E}, ...]}` one array per second |
| `depth` | `depth` | symbol | `{stream, data:{s, U, u, pu, b, a, E, T}}` |
| `snapshot` | `depth` | symbol | `{req_start_ns, req_end_ns, status, body:{lastUpdateId, bids, asks, E, T}}` |
| `forceOrder_all` | `liq` | empty | `{stream, data:{E, o:{s, S, o, f, q, p, ap, X, l, z, T}}}` |
| `bookTicker` | `quote` | symbol | `{stream, data:{u, s, b, B, a, A, T, E}}` |

Read from code, not yet seen in recorded data (no sample existed on 2026-11-06):

| `ch` | File group | Payload `p` |
|---|---|---|
| `gap` | `gap` | `{event:"conn_closed", cause, err, shard, streams}` from [collector](../internal/collector/collector.go); `{event:"drop", channel, count, since_ns}` from [writer](../internal/rawlog/writer.go) |
| `aggTrade_backfill` | `trade` | `{req_start_ns, req_end_ns, status, from_id, body:[aggTrade rows]}` from [backfill](../internal/backfill/backfill.go) |

Treat these two as unverified until a sample is checked; then move them into the table above.

## Replay rules

Merging the raw log of one day into one stream:

1. **Slots.** Files of slot `a`, slot `b`, and unnumbered legacy files overlap during a handoff. Merge them as one source set.
2. **Order.** Within one process, order by `(rm, wseq)`. Across processes `rm` is not comparable; use `rw` ([0009](decisions/0009-zero-gap-handoff.md)). Replay time is `rw`.
3. **Duplicates.** Drop by the per-symbol keys in [0009](decisions/0009-zero-gap-handoff.md) (aggTrade `a`, bookTicker `u`, depth `(U,u)`, markPrice `E`+`s`, forceOrder `E`+`o.s`). Keep the earliest `rw` of each key.
4. **Backfill.** Rows inside an `aggTrade_backfill` page become `aggTrade` events keyed by `a`, with replay time equal to the page's `req_end_ns` (when the data was actually known), flagged `backfilled`. If the live stream already delivered the same `a`, the live one wins.
5. **Gaps.** `gap` records open an unavailable interval for the affected streams until the next message of that stream. `drop` records mark the stated channel as incomplete for the window. Labels must exclude or flag steps inside such intervals.
6. **Book.** `book_at` rebuilds from the latest `snapshot` plus `depth` diffs, per [p0-core-design.md](p0-core-design.md) Component 5. Depth exists for the depth symbols only.
7. **Truncated tail.** A recovered `.part` file can end in a torn frame; the reader stops at the last complete line and reports it.

## Columnar layout (proposal, not yet built)

One directory per stream type, partitioned by UTC date and symbol, zstd Parquet, written by the conversion stage from replay output (so duplicates and ordering are already resolved).

| Dataset | Rows | Columns |
|---|---|---|
| `trades/date=/sym=` | one aggTrade | `rw, T, a, price, qty, buyer_is_maker, backfilled` |
| `mark/date=/sym=` | one markPrice entry | `rw, E, mark, index, funding_rate, next_funding_T` |
| `quotes/date=/sym=` | one bookTicker | `rw, T, u, bid, bid_qty, ask, ask_qty` |
| `depth/date=/sym=` | one depth diff | `rw, E, U, u, pu, bids, asks` |
| `book_snapshots/date=/sym=` | one snapshot | `rw, lastUpdateId, bids, asks` |
| `liquidations/date=` | one forceOrder | `rw, E, sym, side, price, avg_price, qty, filled_qty, status` |
| `quality/date=` | one interval | `stream, sym, start_rw, end_rw, kind` (`gap`, `drop`, `seq_break`, `unavailable`) |

Prices and quantities are kept as decimal strings or scaled integers, not floats, until features are computed. Every file records the parser version ([p0-core-design.md](p0-core-design.md) Determinism). Open: whether to store `depth` diffs or only periodic book checkpoints; decide after measuring size on real data.

## Build order

1. Replay reader (rules above) with tests on real recorded files.
2. Validator and `quality` dataset.
3. Conversion to the columnar layout.
4. Volatility estimator, labeler, baseline evaluator (components 12, 13, 15).
5. Fill simulator design (component 16) starts early: its book-detail needs may change the record format.
