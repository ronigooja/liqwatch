# Data Pipeline: raw log to training and evaluation

One page for the whole chain after recording. Recording itself: [p0-core-design.md](p0-core-design.md). Labels and metrics: [labels-and-evaluation.md](labels-and-evaluation.md). Models: [models.md](models.md). Status values: `done`, `todo`.

## Chain

| # | Stage | Input | Output | Component | Status |
|---|---|---|---|---|---|
| 1 | Record | Binance streams | Raw log `raw/<date>/<group>-<YYYYMMDDHH>[.<slot>][.rN].jsonl.zst` | 2, 3, 4 | done |
| 2 | Archive | Raw log | Remote copy, verified, local delete after 6 h | 6, 7 | done |
| 3 | Replay | Raw log, validation log | Event stream in causal order; `python -m wicker_research.replay`. `book_at(symbol, t)`: `wicker_research.bookat.SymbolBooks`, reads the Parquet depth and snapshot datasets | 8 | done (book equals every verification snapshot on 3 symbols, 1 h) |
| 4 | Validate | Raw log | Validation log: sequence breaks, snapshot mismatches, unavailable intervals; `python -m wicker_research.book` | 5 | done (15 depth symbols, 1 h) |
| 5 | Convert | Replay output | Columnar files for training (layout below); `python -m wicker_research.convert` | 6 | done (checked on 1 h sample; trades match raw exactly) |
| 6 | Label | Trades | Four label tasks per time step; `python -m wicker_research.labels`. Reads raw trades, not yet the Parquet files | 12, 13 | first version |
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
| `gap` | `gap` | empty | `{event:"conn_closed", cause:"reconnect", err, shard, streams}`; `conn` is the connection that ended, `rw` is when its end was noticed |

The `gap` file group exists only for hours in which something happened, so most hours have no `gap` file. They are archived like the others and deleted locally after verification.

Read from code, not yet seen in recorded data:

| `ch` | File group | Payload `p` |
|---|---|---|
| `gap` (writer) | `gap` | `{event:"drop", channel, count, since_ns}` from [writer](../internal/rawlog/writer.go); no drop has happened yet |
| `aggTrade_backfill` | `trade` | `{req_start_ns, req_end_ns, status, from_id, body:[aggTrade rows]}` from [backfill](../internal/backfill/backfill.go); sample lives in remote trade files |

Treat these as unverified until a sample is checked; then move them into the table above.

## Replay rules

Merging the raw log of one day into one stream:

1. **Slots.** Files of slot `a`, slot `b`, and unnumbered legacy files overlap during a handoff. Merge them as one source set.
2. **Order.** Within one process, order by `(rm, wseq)`. Across processes `rm` is not comparable; use `rw` ([0009](decisions/0009-zero-gap-handoff.md)). Replay time is `rw`.
3. **Duplicates.** Drop by the per-symbol keys in [0009](decisions/0009-zero-gap-handoff.md) (aggTrade `a`, bookTicker `u`, depth `(U,u)`, markPrice `E`+`s`, forceOrder `E`+`o.s`). Keep the earliest `rw` of each key.
4. **Backfill.** Rows inside an `aggTrade_backfill` page become `aggTrade` events keyed by `a`, with replay time equal to the page's `req_end_ns` (when the data was actually known), flagged `backfilled`. If the live stream already delivered the same `a`, the live one wins.
5. **Gaps.** A `conn_closed` record carries no start or end time of the hole. Derive it per stream in `streams`: start = `rw` of the last message of that stream before the record, end = `rw` of the first message of that stream from a different `conn`. Unplanned only; a planned replacement overlaps and produces no `conn_closed`. `drop` records mark the stated channel as incomplete for the window `since_ns` to `rw`. Labels must exclude or flag steps inside such intervals. A trade-shard hole is closed by backfill (rule 4); `bookTicker`, `markPrice` and `forceOrder` holes stay.
6. **Book.** `book_at` rebuilds from the latest `snapshot` plus `depth` diffs, per [p0-core-design.md](p0-core-design.md) Component 5. Depth exists for the depth symbols only.
7. **Truncated tail.** A recovered `.part` file can end in a torn frame; the reader stops at the last complete line and reports it.

## Columnar layout (frozen 2026-11-07)

One directory per dataset, partitioned by UTC date and symbol, zstd Parquet, written from replay output (duplicates and order already resolved). Raw log stays the only source of truth; Parquet is rebuildable, so research files use floats.

| Dataset | Rows | Columns |
|---|---|---|
| `trades/date=/sym=` | one aggTrade | `rw, T, a, price, qty, buyer_is_maker, backfilled` |
| `mark/date=/sym=` | one markPrice entry | `rw, E, mark, index, funding_rate, next_funding_T` |
| `quotes/date=/sym=` | one bookTicker | `rw, T, u, bid, bid_qty, ask, ask_qty` |
| `depth/date=/sym=` | one depth diff | `rw, E, U, u, pu, bids, asks` (lists of `[price, qty]`) |
| `book_snapshots/date=/sym=` | one snapshot | `rw, lastUpdateId, bids, asks` |
| `liquidations/date=` | one forceOrder | `rw, E, sym, side, price, avg_price, qty, filled_qty, status` |
| `quality/date=` | one interval | `stream, sym, start_rw, end_rw, kind` (`gap`, `drop`, `seq_break`, `unavailable`) |

Rules: `price`, `qty`, `bid`, `ask`, `mark`, `index`, `funding_rate` are float64. `rw` is int64 nanoseconds and the sort key of every dataset. `T`, `E` are int64 milliseconds as sent by the exchange. Depth keeps every diff plus snapshots, so a book can be rebuilt exactly at any time; size is re-evaluated after the first real conversion. Each file records the parser version in its Parquet metadata ([p0-core-design.md](p0-core-design.md) Determinism).

## Language rule

Code that is still being debugged is written in Python under `research/`. Once its output format and behavior are stable it may be ported to Go; the Python version then becomes the reference for comparison tests. Recording and online paths stay Go ([architecture.md](architecture.md)).

## Measured on the 2026-11-06 17:00 UTC sample (1 hour)

| Input | Rows | Parquet size | Time | Peak memory |
|---|---|---|---|---|
| trade + liq | 2.0 M trades, 3.5 M mark, 1.7 k liquidations | 99 MB | 49 s | 2.1 GB |
| depth | 279 k diffs, 179 snapshots | 90 MB | 19 s | 3.6 GB |

The quote file (530 MB compressed) is not yet converted. Depth is the largest Parquet per row; revisit storing every diff once full days are converted.

## Build order

1. Replay reader (rules above) with tests on real recorded files.
2. Validator and `quality` dataset.
3. Conversion to the columnar layout.
4. Volatility estimator, labeler, baseline evaluator (components 12, 13, 15).
5. Fill simulator (component 16): design done in [fill-simulator.md](fill-simulator.md); it found no needed change to the record format. `book_at` and the simulator are built (`wicker_research.fillsim`).
