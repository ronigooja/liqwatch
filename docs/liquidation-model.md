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
| Open interest | none | Not recorded. Needs a periodic REST pull; add only after the open-question check below |
| Leverage brackets and maintenance margin | exchange REST | Unverified, not recorded |

## Quantities (each defined once)

1. **Liquidation map** `Q_d(P)`: estimated notional that is liquidated when mark price crosses P in direction d. Built from recent trade flow (where positions were likely opened, volume-weighted), an assumed leverage distribution, and maintenance margin. Total scale comes from open interest when available, otherwise from calibration. Every parameter is an assumption until calibrated against recorded liquidations.
2. **Push cost** `C_d(P, N)`: taker fees plus book-walk slippage to reach the level where mark price would be P, using the contract book, plus the cost of unwinding. Depends on how mark price is formed (open question 1); if the index dominates, a contract-only push does not move mark and `C` is not defined by the contract book alone.
3. **Forced flow** `F_d(P)` = `Q_d(P)` arriving as market orders into the book at P. Price after the flow `P_after` comes from walking the thinned book with `F`.
4. **Gain** `G` = actor inventory marked from P to `P_after`, minus unwind slippage, funding and failure cost. Inventory is limited to N.
5. **Net and ratio**: `G - C` and `G / C`, reported with N.

Fill and walk code is reused from [fill-simulator.md](fill-simulator.md).

## Calibration and backtest

- Calibrate `Q` so that the predicted liquidated notional matches the recorded liquidation flow after crossings of the mark price. Because the stream is a lower bound, report the match as a ratio with that caveat.
- Backtest: at high-score moments, did a liquidation cluster follow within a stated window, versus a random-moment baseline? Use the label rules and sealed test split in [labels-and-evaluation.md](labels-and-evaluation.md).
- No score is called good without the baseline comparison and a position size.

## Open questions (to verify before use)

1. How the mark price is computed (index weighting, which venues, smoothing) and whether Binance contract trading alone can move it.
2. Which price triggers liquidation (mark price, per Binance docs?) and how maintenance margin and brackets are defined.
3. Whether `GET /fapi/v1/openInterest` and leverage-bracket endpoints exist as expected, their weights and update frequency.
4. Insurance fund and auto-deleveraging behavior at the sizes seen.

## Output and display

First form: a static HTML page opened locally, reading a JSON file written by the Python run.

- Overview table, one row per symbol: mark price; distance to the nearest estimated cluster up and down; push cost; estimated forced flow; ratio; data status (book complete, gaps).
- Per-symbol view: price axis with the estimated liquidation map, book thickness and cumulative push cost; cost shown for N = 100, 1000, 10000 USD.
- Alert when the ratio passes a threshold, sent by `wicker-alertd` later ([0006](decisions/0006-alert-interface.md)).
- The page states that liquidation levels are estimated, not real positions, and carries no accuracy claim.

## Language and porting

Python under `research/` first; Go after the formats are frozen and results are stable. Details and the equality check: [0010](decisions/0010-liquidation-model-direction.md) item 5. To verify: whether live operation on the 2 vCPU / 4 GB recorder is feasible in Python; if not, port before connecting.

## Build order

1. Verify the open questions above.
2. Liquidation map from trades and liquidations; calibration on available days.
3. Push cost and forced-flow walk on `book_at`.
4. Backtest against the baseline, report with N.
5. JSON output and the static page, driven by replay.
