# Fill Simulator (component 16)

Purpose: turn a signal into a hypothetical fill so that expectancy is measured after realistic costs ([decision 0003](decisions/0003-trading-expectancy-as-criterion.md)). Outline and rules that already exist: [trading-and-risk.md](trading-and-risk.md). Inputs come from replay ([p1-data-pipeline.md](p1-data-pipeline.md)); this file only adds what the simulator needs.

## Measured inputs (2026-11-06 17:00 UTC sample, 1 hour)

| Fact | Value | Consequence |
|---|---|---|
| Data age on arrival: `rw` minus exchange time, depth diffs | p50 4.8 ms, p99 13 ms, max 74 ms (BTCUSDT and FLOCKUSDT) | Book data is fresh; the 200 ms to 1 s order latency dominates, not data age |
| Data age on arrival: aggTrade `T`, BTCUSDT | p50 6 ms, p95 155 ms, max 203 ms | aggTrade is batched by the exchange in about 100 ms steps; never treat trade time as finer than that |
| Quote resolution of depth | 100 ms diffs | Book state between diffs is unknown; fills inside one diff interval use the earlier state |
| Quoted spread, depth symbols | BTC 0.01 bp, ETH 0.04 bp, ZEC 0.07 bp, small coins 2 to 10 bp | Small-coin spread alone is 2 to 10 bp per side, comparable to the edge being sought |
| Book value within 10 bp of mid, small coins | about 0.2 k to 6 k USD per side | A market order above a few thousand USD walks past 10 bp; size must be part of every result |
| Book value within 50 bp of mid, small coins | about 3 k to 53 k USD per side | Same, at 50 bp |
| Book value within 10 bp, BTC and ETH | 11 M to 21 M USD per side | Majors are not capacity-limited at realistic sizes |
| Snapshot reach | 1000 levels covers 18 to 92 bp for BTC, ETH, ZEC and thousands of bp for small coins | Majors: a market order beyond the covered depth cannot be priced from a snapshot; reject such sizes |

These are one hour and one quiet-to-normal period. Spike periods are thinner, so every number above is optimistic for spikes. Recompute on spike windows once enough exist.

## Model

1. **Time.** Signal at replay time `t_s`. Order reaches the exchange at `t_s + L`, where `L` is the order latency (grid below). The simulator reads the book as of `t_s + L` through replay's `book_at`, never the book at `t_s`.
2. **Market order.** Walk the opposite side of the book at `t_s + L`, level by level, until the size is filled. Price is the size-weighted average. If the book snapshot does not reach far enough, the order is rejected and counted, not clipped. Fee is taker.
3. **Limit order, placed at `t_s + L`.** Rests at its price. Queue ahead at placement is the displayed size at that price. The order fills only from executed trades at or through its price: aggTrade quantity at that price reduces the queue ahead first, and only the remainder fills us. Cancels ahead are not observable (book data is aggregated by price), so run two bounds: pessimistic, no queue reduction from cancels; optimistic, queue ahead shrinks in proportion to the displayed size decrease at that price. Report both. Fee is maker. An unfilled order after its time limit is a miss, counted as no trade.
4. **Slippage after fill.** Marking and exit use the same rules: exit orders are market orders at their own latency, or limit orders with the same queue model. There is no exit at the trigger price.
5. **Exchange-side stop.** Modeled as a market order triggered when the mark price crosses the stop, with latency `L_stop` from the trigger, filled by the same walk. Mark price comes from the `markPrice` dataset.
6. **Fees and funding.** Fee tier is unknown ([open-questions.md](open-questions.md)). Run the grid below; carry funding only when a position crosses a funding timestamp.
7. **Own impact.** Our order does not change the replayed book. For sizes above 10 percent of displayed depth within the walk range, flag the result as impact-unmodeled and exclude it from headline numbers.

## Parameter grid (fixed before running, not tuned afterwards)

| Parameter | Values |
|---|---|
| Order latency `L` | 0.2 s, 0.5 s, 1 s |
| Taker fee | 0.050 percent, 0.040 percent, 0.0315 percent |
| Maker fee | 0.020 percent, 0.016 percent, 0.0 percent |
| Position size (USD notional) | 100, 500, 2000, 10000 |
| Limit queue bound | pessimistic, optimistic |

The fee values are assumptions, not account facts, until the owner gives the tier. Headline results use the first value of each row; the rest are sensitivity.

## Outputs

Per simulated trade: signal time, side, requested size, fill price, filled size, fee, slippage versus mid at `t_s` in bp, outcome (`filled`, `partial`, `rejected`, `missed`), and a flag when the replay time falls in a `quality` interval. Aggregates feed the trading rows of [labels-and-evaluation.md](labels-and-evaluation.md) Metrics: expectancy per trade after costs, per tier, month and volatility regime, and the fill-rate and rejection share.

## What may change the recording

Historical book and quote guidance below is conditional on future data availability; current scope is [decision 0011](decisions/0011-no-alert-narrower-recording.md).

| Question | Needed data | Status |
|---|---|---|
| Are 15 depth symbols enough to judge fills for the traded tier? | Book for every symbol the strategy would trade | Open. Fills on symbols without a recorded book could be approximated from `bookTicker` (best bid and ask), if available |
| Is the 100 ms depth resolution enough for queue modeling? | 100 ms is the fastest diff-depth speed listed for futures (250 ms default, 500 ms, 100 ms; [open-questions.md](open-questions.md)) | No finer data exists to record |
| Are 1000 snapshot levels enough for majors? | Level cover in bp above | Limit sizes on majors, no recording change needed |
| Is cancel behavior needed? | Order-level data is not available from Binance | Not recorded; use the two queue bounds |

## Status

Built in `research/liqwatch_research/fillsim`: market-order walk, limit orders with both queue bounds, stops on mark price, funding, round trips with exit retries (a failed exit is reported as `exit_failed`, never given a price), and the grid runner. Tests use hand-computed examples, including that nothing reads the book before `t_s + L`.

Sanity check on the 1 h sample, random entry times (no edge), 5 bp taker fee each way, 0.2 s latency, 10 s hold: BTCUSDT net about -9.9 bp at every size up to 20 k USD (fees only); FLOCKUSDT -15.5 bp at 100 USD, -23.5 bp at 1 k, -35 bp at 5 k, -72 bp at 20 k; XANUSDT -22 bp at 100 USD, -31 bp at 1 k, -65 bp at 20 k. Latency 0.2 s versus 1 s changes little at random times; it matters when the signal is a fast move. The cost of simply trading a small coin is therefore 15 to 70 bp per round trip before any edge.

Open: the 10 bp impact threshold flags nearly every small-coin trade above 1 k USD, so those results exclude our own impact; limit-order results depend on the queue bound and have not been checked against real fills.

## Build order (done in this order)

1. Market-order walk on `book_at` with latency and taker fee.
2. Limit-order queue model, both bounds.
3. Stop orders on mark price.
4. Sensitivity runner over the grid and the output table.

Tests: hand-computed walks on tiny books, a queue example with and without cancels, rejection when the book is too shallow, and a check that no result uses the book before `t_s + L`.
