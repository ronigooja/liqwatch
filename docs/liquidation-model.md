# Liquidation Model

Decision: [0010](decisions/0010-liquidation-model-direction.md). Status: design, no code yet. Exchange facts below are unverified until moved to Verified in [open-questions.md](open-questions.md).

## Question answered

For symbol S, time t, direction d (push up or down) and position size N (USD): what would it cost a hypothetical actor to push price to a liquidation cluster, and what would the forced flow beyond it be worth to them? Output is a cost, a gain and their ratio per (S, t, d, N). It estimates incentive, not intent, and promises no outcome.

## Inputs

| Input | Source | State |
|---|---|---|
| Order book at t | `book_at` ([p1-data-pipeline.md](p1-data-pipeline.md)) | Recorded for 15 depth symbols only; other symbols cannot be modeled |
| Mark price, funding | `mark` dataset | Recorded |
| Trades | `trades` dataset | Recorded, all symbols |
| Liquidations | `liquidations` dataset | Recorded; lower bound only, per symbol the stream shows the latest order within 1000 ms |
| Open interest | `/fapi/v1/openInterest` (public, current) and `openInterestHist` (public, 5 m periods) | Verified usable, not yet recorded. Needs a periodic pull added to the recorder or a separate poller |
| Index composition and weights | `/fapi/v1/constituents` (public) | Verified usable, not recorded. Weights change, so sample periodically |
| Index price | `/fapi/v1/premiumIndex` | Verified usable, not recorded |
| Leverage brackets and maintenance margin | `/fapi/v1/leverageBracket` | Needs a signed key (401 unsigned). Until then assume a bracket table as a parameter |

## Quantities (each defined once)

1. **Liquidation map** `Q_d(P)`: estimated notional that is liquidated when mark price crosses P in direction d. Built from recent trade flow (where positions were likely opened, volume-weighted), an assumed leverage distribution, and maintenance margin. Total scale comes from open interest when available, otherwise from calibration. Every parameter is an assumption until calibrated against recorded liquidations.
2. **Push cost** `C_d(P, N)`: taker fees plus book-walk slippage to move the *contract* price to the level that moves *mark price* to P, plus the cost of unwinding. Mark price is the median of Price 1 (index with funding adjustment), Price 2 (index plus 1-minute average basis) and the contract price ([verified](open-questions.md)). Consequence: a contract-only push moves mark price only if it moves the median, so the push must move the contract price past both index-based prices; the index is a spot-venue average and the contract push does not move it. Where the index has no Binance venue (e.g. FLOCKUSDT), the actor would also have to move spot elsewhere. `C` is therefore model-dependent; the first step is to test on recorded data how far mark price follows contract price during spikes.
3. **Forced flow** `F_d(P)` = `Q_d(P)` arriving as market orders into the book at P. Price after the flow `P_after` comes from walking the thinned book with `F`.
4. **Gain** `G` = actor inventory marked from P to `P_after`, minus unwind slippage, funding and failure cost. Inventory is limited to N.
5. **Net and ratio**: `G - C` and `G / C`, reported with N.

Fill and walk code is reused from [fill-simulator.md](fill-simulator.md).

## Measured: does mark price follow the contract price

Script: `research/scripts/mark_vs_contract.py`. Sample: 1 h, 2026-11-06 17:00 UTC, 15 depth symbols, 1 s steps. gap = (last trade − index) / index; dev = (mark − index) / index; pass99 = median of dev / gap over the seconds with the largest 1% of |gap|.

| Group | Mean \|gap\| (bp) | pass99 | corr(dev, gap) |
|---|---|---|---|
| BTC, ETH, ZEC | 3.3–4.4 | 0.12–0.55 | 0.56–0.75 |
| 12 spike symbols | 1.6–61 | 0.46–0.99 (BULLA −0.06) | 0.82–0.97 (BULLA 0.03) |

Reading: on small coins in this hour, mark price moved with the contract-index gap for most of the gap (pass99 mostly above 0.7). On BTC and ETH it followed less. This is a calm hour, one sample, and a gap that lasts for minutes also moves the 1-minute basis in Price 2, so it does not show that a short contract-only push moves mark price. BULLAUSDT is unexplained (mark far from index while the contract is near it). Open question 1 stays open until spike periods are measured.

## Venue adapter

Scope now: Binance only. The core (quantities above, backtest, dashboard) reads only the unified inputs: order book, trades, mark price, liquidations, open interest, index price and constituents. An adapter supplies what differs by venue:

- mark-price formula and its inputs;
- the price that triggers liquidation;
- maintenance margin and leverage brackets;
- insurance fund and auto-deleveraging rules;
- endpoints, field names and fee rates.

The Binance values are defined once, in the Verified table of [open-questions.md](open-questions.md). In code, `research/` holds one adapter interface with Binance as the only implementation. Existing Parquet data is Binance data; a new venue gets its own root directory, so the frozen layout in [p1-data-pipeline.md](p1-data-pipeline.md) and the Go recorder are not changed now.

Expectation, not verified: Binance's index uses spot prices from other venues (see the constituents in open-questions), so recording those venues later could improve the push-cost estimate.

## Calibration and backtest

- Calibrate `Q` so that the predicted liquidated notional matches the recorded liquidation flow after crossings of the mark price. Because the stream is a lower bound, report the match as a ratio with that caveat.
- Backtest: at high-score moments, did a liquidation cluster follow within a stated window, versus a random-moment baseline? Use the label rules and sealed test split in [labels-and-evaluation.md](labels-and-evaluation.md).
- No score is called good without the baseline comparison and a position size.

## Open questions (to verify before use)

Settled (see Verified in [open-questions.md](open-questions.md)): mark price formula, mark price as the liquidation trigger, public open-interest and index endpoints, insurance fund and ADL outline.

Still open:
1. How far mark price follows the contract price during spikes: measure on recorded `mark` and `trades` (decides whether `C` is defined by the contract book).
2. The definition of "Contract Price" in the median.
3. Maintenance margin tiers and leverage brackets (signed endpoint, or a parameter table).
4. Liquidation clearance fee rate and ADL ranking details.
5. Request weights of the open-interest and index endpoints, and the polling interval.

## Output and display

First form: a static HTML page opened locally, reading a JSON file written by the Python run.

- Overview table, one row per symbol: mark price; distance to the nearest estimated cluster up and down; push cost; estimated forced flow; ratio; data status (book complete, gaps).
- Per-symbol view: price axis with the estimated liquidation map, book thickness and cumulative push cost; cost shown for N = 100, 1000, 10000 USD.
- Alert when the ratio passes a threshold, sent by `wicker-alertd` later ([0006](decisions/0006-alert-interface.md)).
- The page states that liquidation levels are estimated, not real positions, and carries no accuracy claim.

## Language and porting

Python under `research/` first; Go after the formats are frozen and results are stable. Details and the equality check: [0010](decisions/0010-liquidation-model-direction.md) item 5. To verify: whether live operation on the 2 vCPU / 4 GB recorder is feasible in Python; if not, port before connecting.

## Build order

1. Measure mark-price versus contract-price behavior on recorded data (open question 1); settle the remaining open questions.
2. Liquidation map from trades and liquidations; calibration on available days.
3. Push cost and forced-flow walk on `book_at`.
4. Backtest against the baseline, report with N.
5. JSON output and the static page, driven by replay.
