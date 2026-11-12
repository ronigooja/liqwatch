# Liquidation Model

Decision: [0010](decisions/0010-liquidation-model-direction.md). Status: Python research code exists (`research/wicker_research/liqmap`, `liqcal`, `liqcausal`); the map is not used for `G` or `N*` ([0012](decisions/0012-map-not-used-for-gain.md)). Exchange facts below are unverified until moved to Verified in [open-questions.md](open-questions.md).

## Question answered

For symbol S, time t and direction d (push up or down): what would it cost a hypothetical actor to push price to a liquidation cluster, and what would the forced flow beyond it be worth to them?

Two different things are called a position here; keep them apart:
- **Liquidated positions**: other traders' positions that get force-closed. Modeled by the liquidation map `Q`.
- **Actor inventory `N`** (USD): what the hypothetical actor holds before the push. Only the gain and the ratio depend on `N`; the push cost does not.

Output: cost curve per (S, t, d); gain and ratio per (S, t, d, N); break-even `N`. It estimates incentive, not intent, and promises no outcome.

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

## Known input gaps (each weakens every result below)

| Gap | Current stand-in | Effect | How to close |
|---|---|---|---|
| Open interest not recorded | Every trade in the window counts as a new position | Long-held positions are missing; closing, hedging and market-making trades count as openings; the scale is unanchored | Pull `openInterestHist` for 2026-11-04 onward now (history reaches about 30 days, see [open-questions.md](open-questions.md)); record via the poller. It is aggregate, so opening and closing are still not separated |
| Leverage distribution of traders | Assumed weights {5, 10, 20, 50, 100} | Liquidation prices are assumptions | Not public on Binance; only assumption, back-fit from recorded liquidations, or a public DEX as a reference (unchecked) |
| Maintenance margin and tiers | `mmr = 0.005`, a placeholder, not an estimate | Every liquidation price is off by an unquantified amount | `leverageBracket` needs a key and signature; no public table found |

Results from the placeholder inputs show only that the model is not rejected by baselines; they carry no scale.

## Quantities (each defined once)

1. **Liquidation map** `Q_d(P) = Σ_v Q_{v,d}(P)` summed over venues v; a venue not modeled contributes nothing, so the result is a lower bound and is shown with the list of venues covered (an uncovered venue is never displayed as zero). Per-venue `Q_{v,d}(P)`: estimated notional that is liquidated when mark price crosses P in direction d. Built from recent trade flow (where positions were likely opened, volume-weighted), an assumed leverage distribution, and maintenance margin. Total scale comes from open interest when available, otherwise from calibration. Every parameter is an assumption until calibrated against recorded liquidations.
2. **Push cost** `C_d(P)`, independent of `N`: taker fees plus book-walk slippage to move the *contract* price to the level that moves *mark price* to P, plus the cost of unwinding. Mark price is the median of Price 1 (index with funding adjustment), Price 2 (index plus 1-minute average basis) and the contract price ([verified](open-questions.md)). Consequence: a contract-only push moves mark price only if it moves the median, so the push must move the contract price past both index-based prices; the index is a spot-venue average and the contract push does not move it. Where the index has no Binance venue (e.g. FLOCKUSDT), the actor would also have to move spot elsewhere. `C` is therefore model-dependent; the first step is to test on recorded data how far mark price follows contract price during spikes.
3. **Forced flow** `F_d(P)` = `Q_d(P)` arriving as market orders into the book at P. Price after the flow `P_after` comes from walking the thinned book with `F`.
4. **Gain** `G_d(P, N)` = inventory `N` marked from the entry price to `P_after`, minus unwind slippage, funding and failure cost. Grows with `N`.
5. **Net, ratio and break-even**: `G - C` and `G / C`, reported with `N`. Break-even `N*` = the smallest `N` for which `G - C >= 0`; it is the number to display.

Fill and walk code is reused from [fill-simulator.md](fill-simulator.md).

## Measured: does mark price follow the contract price

Script: `research/scripts/mark_vs_contract.py`. Sample: 1 h, 2026-11-06 17:00 UTC, 15 depth symbols, 1 s steps. gap = (last trade − index) / index; dev = (mark − index) / index; pass99 = median of dev / gap over the seconds with the largest 1% of |gap|.

| Group | Mean \|gap\| (bp) | pass99 | corr(dev, gap) |
|---|---|---|---|
| BTC, ETH, ZEC | 3.3–4.4 | 0.12–0.55 | 0.56–0.75 |
| 12 spike symbols | 1.6–61 | 0.46–0.99 (BULLA −0.06) | 0.82–0.97 (BULLA 0.03) |

Reading: on small coins in this hour, mark price moved with the contract-index gap for most of the gap (pass99 mostly above 0.7). On BTC and ETH it followed less. This is a calm hour, one sample, and a gap that lasts for minutes also moves the 1-minute basis in Price 2, so it does not show that a short contract-only push moves mark price. BULLAUSDT is unexplained (mark far from index while the contract is near it). Open question 1 stays open until spike periods are measured.

Spike measurement: `research/scripts/mark_spikes.py`, 2026-11-05 full day, all 522 symbols that have trades and at least 3,600 s of data (of 980 mark symbols), 1 s steps. gap and dev as above but minus their own trailing 300 s median, so the persistent basis is removed. A spike second is |gap| ≥ threshold. pass = dev / gap on spike seconds, median per symbol.

| Threshold | Spike seconds | Symbols with ≥ 30 spike s | pass, symbol quantiles 10 / 25 / 50 / 75 / 90 % | Symbols with pass > 0.5 / < 0.2 |
|---|---|---|---|---|
| 20 bp | 2,278,162 | 502 | 0.39 / 0.54 / 0.68 / 0.78 / 0.83 | 79% / 4% |
| 50 bp | 87,679 | 227 | 0.22 / 0.45 / 0.64 / 0.77 / 0.87 | 69% / 8% |

Reading: in short deviations mark price moved about two thirds of the contract-index gap in the median symbol; mark equals the last trade price in only 6% to 14% of spike seconds. Median spike run is 2 s. Not separated: the 1-minute basis in Price 2 and the contract term in the median both move with the contract, so this still does not show which input carries the move. One day, one market state; low-liquidity symbols such as JCTUSDT (0.23) pass little. Trades cover 522 symbols, not all 980.

Earlier run of a different script (`mark_follow.py`, removed; same sample, recorded here for its findings): contract price stays away from the index for long stretches (median gap 10 to 60 bp on most small symbols, 99th percentile up to 127 bp on 1000CATUSDT; BTC and ETH within about 8 bp); mark price equals the last trade price in 22% to 53% of seconds; ZECUSDT responds weakly (0.02) and BULLAUSDT 0.37. The response includes the lag of the 1-minute basis (Price 2), so it does not separate "contract price enters the median directly" from "Price 2 follows the contract with a delay". The "Contract Price" input of the median is not defined in the docs read so far.

## Venue adapter

Scope now: Binance only. The core (quantities above, backtest, dashboard) reads only the unified inputs: order book, trades, mark price, liquidations, open interest, index price and constituents. An adapter supplies what differs by venue:

- mark-price formula and its inputs;
- the price that triggers liquidation;
- maintenance margin and leverage brackets;
- insurance fund and auto-deleveraging rules;
- endpoints, field names and fee rates.

The Binance values are defined once, in the Verified table of [open-questions.md](open-questions.md). In code, `research/` holds one adapter interface with Binance as the only implementation. Existing Parquet data is Binance data; a new venue gets its own root directory, so the frozen layout in [p1-data-pipeline.md](p1-data-pipeline.md) and the Go recorder are not changed now.

Expectation, not verified: Binance's index uses spot prices from other venues (see the constituents in open-questions), so recording those venues later could improve the push-cost estimate.

## Multiple venues (design now, implement later)

Owner point, 2026-11-10: liquidation levels cannot be judged from Binance alone. Reasons (reasoning, not verified): other venues' forced orders hit their own and spot books, which feed Binance's index and hence its mark price; a market maker sees the combined clusters. How much of a symbol's total open interest sits on Binance is unknown and decides how much a Binance-only view misses.

Per venue, to verify before any adapter or recording (list in [open-questions.md](open-questions.md)): liquidation feed and whether it is sampled, open interest, leverage brackets, mark-price formula, rate limits. The first version computes Binance only, shows "venues covered: Binance", and does not claim totals. Recording other venues needs a new decision record because it changes [0004](decisions/0004-defer-game-layers-and-multi-venue.md).

## Calibration and backtest

### Consistency check (done, `research/wicker_research/liqcal`)

At each grid moment t the map is built from data up to t; `pred` is the map's notional between the start mark and the lowest (or highest) mark of the NEXT 5 minutes; `obs` is the recorded liquidation USD in that period. This uses the realised future path, so it is an **after-the-fact consistency check, not a forecast**: it says nothing about what was knowable at t. Results, BTC/ETH/ZEC/HOT/ESPORTS, 2026-11-04..06, 60 s grid, 5 min horizon (numbers from before the fixes below):

- Partial Spearman of pred and obs, controlling for the rank of the move, 0.09 to 0.78 (BTC/ETH mostly 0.4 to 0.7, HOT 0.2 to 0.5, ESPORTS 0.1 to 0.8 on 50 to 150 crossings), above the within-symbol permutation range of about ±0.1 in 11 of 12 HOT/ESPORTS cells. Moments overlap and are serially dependent, so that range is too narrow and the evidence is weaker than it looks. 2026-11-05 14:00 to 15:00 UTC holds most liquidation volume.
- Observed/predicted: 0.01 to 0.12 with bracket tiers, 0.3 to 3 with open-interest anchoring (net increase per 5 minutes only). A ratio above 1 with a lower-bound observation does not identify which assumption fails. Not usable as a calibrated scale.
- Bracket tiers changed scale and ranks little; open-interest anchoring fixed the order of magnitude and did not raise the partial correlation.

Scale stability, `bucket_both` (open interest rise `d` creates `d` longs and `d` shorts, [open-questions.md](open-questions.md)), 8 h window, 3 days x 2 sides (2026-11-04..06). Obs/pred per symbol, max/min within the symbol: BTC 3.8x (median 0.66), ETH 7.4x (0.81), ZEC 1.8x (0.21), HOT 16x (0.42), ESPORTS 8.3x. All 30 cells span 0.05 to 1.44 (32x, median 0.33); BTC+ETH 12 cells 7.4x. The owner-set standard (within 2x across symbols, days and sides) is **not met**. `bucket_both` halves obs/pred against `bucket` (x0.46 to 0.50) and leaves ranks unchanged, so it fixes the level, not the spread.

Window length, `bucket_both`, BTC/ETH/ZEC/HOT, 24 cells, max/min of obs/pred (train days 11-04/05, test day 11-06, all): 4 h 12x / 19x / 33x; 8 h 9x / 19x / 32x; 24 h 9x / 25x / 44x. Medians 0.44 / 0.42 / 0.37. Window length does not narrow the spread; 8 h kept. The per-symbol spread of HOT is 16 to 17x at every window. Only 4 symbols and 3 days, one of them extreme: this shows no window effect, it does not prove there is none.

HOT is not a small-sample case (459 to 544 crossings per side-day). Its spread is mostly between sides: 11-06 predicted up mass is 2.4x the down mass while recorded up liquidations are about a quarter of the down ones (ratios 0.05 up, 0.44 down); the same sign on 11-05 (0.20 up, 0.73 down). Cause not established. Trend hypothesis tested and not supported: per moment, past-hour mark return (and, separately, net taker flow share) splits each side into with-trend and against-trend (|return| < 5 bp dropped); pooled over 3 days, against/with ratio of obs/pred is BTC 0.54 [0.30, 1.33], ETH 0.72 [0.44, 1.39], ZEC 0.73 [0.49, 1.06], HOT 0.85 [0.58, 1.22] (return split; 1 h block bootstrap, 95%); the flow split gives BTC 0.42 [0.26, 0.92], ETH 0.80, ZEC 0.55 [0.39, 0.80], HOT 0.96. Signs flip between symbol-days, and HOT 11-06 up is 0.046 with-trend and 0.039 against. No trend correction is added.

Seven days (2026-11-04..10; 11-07..10 downloaded from the archive after the recorder stopped; 11-10 covers 00:00 to about 11:00 UTC only; open interest for all seven days, brackets still the 2026-11-11 table). Obs/pred per day, `bucket_both`, 8 h:

| Symbol side | 11-04 | 11-05 | 11-06 | 11-07 | 11-08 | 11-09 | 11-10 |
|---|---|---|---|---|---|---|---|
| BTC down | 1.18 | 0.82 | 0.50 | 0.30 (15 crossings) | 4.79 (65) | 0.64 | 1.05 |
| BTC up | 1.13 | 0.35 | 0.31 | 5.95 (5) | 0.15 (20) | 0.31 | 0.69 (19) |
| ETH down | 1.44 | 0.83 | 0.89 | 1.76 (22) | 8.37 (82) | 0.97 | 0.80 |
| ETH up | 0.64 | 0.80 | 0.19 | 2.74 (10) | 1.75 (48) | 0.31 | 0.20 |
| ZEC down | 0.20 | 0.24 | 0.15 | 0.34 | 0.62 | 0.19 | 0.27 |
| ZEC up | 0.23 | 0.16 | 0.28 | 0.09 | 0.11 | 0.09 | 0.02 |
| HOT down | 0.55 | 0.73 | 0.44 | 0.44 | 0.21 | 0.19 | 0.21 |
| HOT up | 0.40 | 0.20 | 0.05 | 0.09 | 0.23 | 0.16 | 0.11 |

Per-symbol and per-side coefficient fitted on 11-04 and 11-05 (pooled obs/pred), applied to 11-06..10 (40 cells): only 17 of 40 cells land within 0.5 to 2 times the coefficient; min 0.12, max 9.2 (80x); cells with at least 100 crossings: 13 of 28, 24x (uncorrected 40x). ZEC's stability over the first three days did not generalise (0.12 to 2.8 after correction); the cause is not identified. Crossings per cell, days 11-04..10: BTC down 143 231 139 15 65 196 71, BTC up 220 93 125 5 20 186 19, ETH down 172 269 149 22 82 221 95, ETH up 271 117 145 10 48 212 46, ZEC down 398 431 370 219 228 404 146, ZEC up 412 358 385 227 178 339 148, HOT down 544 530 535 428 471 481 209, HOT up 472 487 459 272 551 675 289. Fairer coefficient fits (review 2026-11-12), cells within 0.5 to 2 times the coefficient: leave-one-day-out over all 7 days 26 of 56 (43 cells with at least 100 crossings: 23, max/min 34x); fit excluding the extreme day 11-05, 48 cells 20 (36 cells with at least 100 crossings: 17, 37x). The result does not depend on the choice of fitting days. Note the check is a deployment gate, not a statistical rejection: observations are a lower bound with varying capture, windows overlap, and no interval is given for the ratios. The big deviations of BTC and ETH sit in cells with 5 to 82 crossings. The coefficient changes between days and sides, so the spread is not a fixed property of the symbol. ESPORTS is dropped (predicted mass 0 on 11-08 up and 11-10). The owner-set standard (within 2x) is **not met**.

Causal evaluation on seven days (rolling day pairs, ridge, 6 test days per symbol and side, c over b in Spearman, 95% block-bootstrap interval above 0 / below 0 / contains 0): BTC down 1/0/5, BTC up 0/0/6, ZEC down 3/0/3, ZEC up 3/0/3, HOT down 2/2/2, HOT up 1/0/5; total 10/2/24; mean c-b between -0.011 and +0.034. Mean Spearman a/b/c: BTC down 0.29/0.30/0.29, ZEC down 0.29/0.24/0.26, ZEC up 0.21/0.12/0.16, HOT down 0.09/0.14/0.16. c minus b isolates the localised shape given total mass, not the whole map against past-only features. The ridge experiment shows small average gains (the earlier GBM run showed +0.044); non-significant differences are not proof of no information. Test days are distinct across pairs (a day is test in one pair and training in the next), but labels overlap within a day, symbols and sides share market shocks, and 36 intervals are counted without multiplicity adjustment, so the intervals are too narrow. Method check on synthetic data (review 2026-11-12): the real split, ridge and bootstrap path detects an informative local feature (5 of 5 seeds above 0) and does not flag noise. ETH was still running when this was written.

Independent audit of `liqmap` and `liqcal` (2026-11-12, 16 synthetic checks): directions, liquidation-price formulas (hand-computed BTC-like 5x and 100x), survival boundaries, window and open-interest bucket boundaries and future invariance, scoring boundaries and partial Spearman all passed; no high-severity defect. Findings that qualify the numbers above: (1) `sum_obs` and the ratio count only moments with positive predicted mass, so liquidations at moments where the map predicted nothing are excluded and the ratio is flattering; (2) the timestamp meaning of open-interest history (value at T as snapshot at T, assigned to trades in [T, T+5 min)) is assumed, not verified; (3) survival follows trades, not marks (a mark spike with no trade crossing leaves the level alive), and tier and leverage cap use the raw trade fragment, not a position; (4) cumulative sums can lose small interval mass to float cancellation (shown with 1e20 against 1; materiality on recorded data not measured); (5) fragments with no allowed leverage are dropped without a diagnostic.

Defects found by independent review and fixed (see git log): liquidation price solved exactly (the additive formula was off by up to 94 bp on ESPORTS tier 1 at 5x, 0.2 to 4 bp on BTC), levels already beyond the start mark no longer counted, bucket and window boundary aligned, invalid liquidation records dropped. Known, not fixed: the maintenance tier is chosen from entry notional of a trade fragment; trade price (not mark) decides survival in `liqmap`; isolated margin only; leverage weights invented; positions older than the window missing.

### Causal evaluation (`research/wicker_research/liqcausal`; first run done, exploratory)

Models, nested: a = past-only baselines (volume 5 and 60 min, realised volatility, signed flow, recent liquidations by side, hour of day, funding time, open-interest change); b = a + total map mass; c = b + map mass within 10, 25, 50 bp and in volatility units and nearest-cluster distance. Ridge regression (alpha 10) on log features, fitted on one day, scored on the next (rolling pairs 11-04 to 11-05 and 11-05 to 11-06), outcome = recorded liquidation USD in the next 5 minutes, brackets and open-interest anchoring on. Block-bootstrap 95% intervals on the held-out day (60 min blocks, fixed training fit, so too narrow).

Spearman of prediction and outcome on the held-out day; c−b with 95% interval:

| Symbol, side | Test day | a | b | c | c − b |
|---|---|---|---|---|---|
| BTC down | 11-05 | 0.43 | 0.43 | 0.34 | −0.09 (−0.19, 0.01) |
| BTC up | 11-05 | 0.42 | 0.41 | 0.40 | −0.01 (−0.07, 0.06) |
| BTC down | 11-06 | 0.43 | 0.31 | 0.31 | 0.00 (−0.08, 0.11) |
| BTC up | 11-06 | 0.35 | 0.33 | 0.25 | −0.07 (−0.16, 0.00) |
| ETH down | 11-05 | 0.46 | 0.46 | 0.18 | −0.28 (−0.47, −0.09) |
| ETH up | 11-05 | 0.40 | 0.40 | 0.36 | −0.04 (−0.08, 0.02) |
| ETH down | 11-06 | 0.30 | 0.30 | 0.29 | 0.00 (−0.05, 0.05) |
| ETH up | 11-06 | 0.30 | 0.30 | 0.32 | 0.02 (−0.03, 0.07) |
| ZEC down | 11-05 | 0.38 | 0.36 | 0.40 | 0.04 (−0.02, 0.12) |
| ZEC up | 11-05 | 0.32 | 0.33 | 0.38 | 0.05 (−0.01, 0.12) |
| ZEC down | 11-06 | 0.27 | 0.06 | 0.15 | 0.09 (0.05, 0.14) |
| ZEC up | 11-06 | 0.41 | −0.10 | −0.05 | 0.04 (−0.01, 0.10) |

Reading: the past-only baseline alone reaches 0.3 to 0.46; total mass adds nothing stable (b − a near 0, ZEC 11-06 much worse); localised mass does not improve on b in 10 of 12 cells, is clearly worse in one (ETH down 11-05) and clearly better in one (ZEC down 11-06). In this test the map shows no information beyond simple past-only features. Variants without the largest event hour and on non-overlapping 5-minute windows give the same direction in every cell (c − b differs by at most 0.04 from the main value, intervals still contain 0 except ETH down 11-05 and ZEC down 11-06). Alert view (event = outcome above the training 90th percentile, base rate 4% to 17%): ROC-AUC of a is 0.67 to 0.89, of c 0.52 to 0.87; c is below a in 8 of 12 cells; recall at one alert per hour is 0.10 to 0.23 for a and 0.00 to 0.31 for c with no consistent order. A model that already knows recent liquidations and volatility ranks events well; the map adds nothing on top. Limits: two day pairs, 11-05 is the extreme day and a one-day fit is unstable (ZEC b collapses on 11-06); a linear model cannot find nonlinear structure, so this is "no gain found", not "no information"; recall at an alert budget and the without-largest-hour and non-overlap variants are in the CSV and were not examined here; no sealed test split.

Same evaluation with gradient boosting (`--model gbm`, scikit-learn HistGradientBoostingRegressor, fixed depth 3, 200 trees, learning rate 0.05, no tuning), 12 cells (BTC, ETH, ZEC; down and up; test days 11-05 and 11-06):

| | Ridge | GBM |
|---|---|---|
| c − b interval above 0 / below 0 / contains 0 | 1 / 1 / 10 | 5 / 0 / 7 |
| Mean c − b | −0.021 | +0.044 |
| Mean Spearman a / b / c | 0.37 / 0.30 / 0.28 | 0.18 / 0.20 / 0.24 |

GBM cells with the interval above 0: BTC down 11-06 (+0.08), ETH down 11-06 (+0.06), ETH up 11-06 (+0.04), ZEC down 11-06 (+0.07), ZEC up 11-05 (+0.16). Reading: with a nonlinear model the localised mass is more often helpful and never significantly harmful, but (1) GBM's own baseline a is weak (mean 0.18, 0.04 to 0.17 on 11-06), the best absolute ranking in this study is still the ridge baseline a (0.37); (2) four of the five positive cells have the extreme day 11-05 as the training day and 11-06 as the test day, where a is weakest; (3) intervals are too narrow (fixed training fit) and there are only two day pairs. This is a weak hint, not evidence of usefulness. The ZEC run first crashed because `nearest_down_bp` was NaN for the whole of 11-05 (no single level reached 1% of the side total); GBM now drops columns with fewer than two distinct training values and records them in `gbm_dropped_columns`.

Original specification. Freeze the map at t. Features known at t only: map mass within fixed distances (for example 10, 25, 50 bp, and in units of past volatility) above and below the current mark, and the nearest cluster distance. Outcomes: whether recorded liquidation notional in the next 5 minutes exceeds a threshold, and the extra price movement after a cluster is touched. Baselines using the past only: recent volume, volatility, time of day, funding. Ablation: keep total map mass and the baselines, remove the price-localised shape. Uncertainty: block permutation or circular shifts by whole days or events, non-overlapping windows as sensitivity, leave-one-event-hour-out. Splits and metrics follow [labels-and-evaluation.md](labels-and-evaluation.md): time-ordered train / tuning / sealed test with purge and embargo, false alarms per hour, recall at a fixed budget, lead time. Three days are too few for a sealed split; more days must be recorded first.

### Economics (not measured)

Push cost, forced-flow price impact and gain are unmeasured. The mark-price pass-through above is a same-time ratio, not the response to an actor's order.

### Rules

No score is called good without the baseline comparison, a position size and the causal evaluation above.

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

- Overview table, one row per symbol: mark price; distance to the nearest estimated cluster up and down; push cost; estimated forced flow; break-even `N*`; data status (book complete, gaps).
- Per-symbol view: price axis with the estimated liquidation map, book thickness and the cumulative push cost curve (one curve, not per `N`); gain and ratio shown for N = 100, 1000, 10000 USD; break-even `N*`.
- No alert ([0011](decisions/0011-no-alert-narrower-recording.md)).
- The page states that liquidation levels are estimated, not real positions, and carries no accuracy claim.

## Compute and storage

Which data is saved and why:
- Push cost is computed from the live in-memory book; it is an arithmetic walk of the book and needs no history. Saved books are not recorded now; the recorded 2026-11-04..10 books serve checks of the arithmetic. What is recorded and retention: [0011](decisions/0011-no-alert-narrower-recording.md).

Two layers:

| Layer | Content | Runs | Saved |
|---|---|---|---|
| Score | Per symbol, every few seconds: distance to nearest cluster up and down, push cost, forced flow, `N*`, data status | For all symbols continuously: the model state is rolling (recent trades, 1-minute basis), so it cannot be started on demand without a warm-up replay | Yes, with the model version. Size and interval are not measured |
| Detail | Full liquidation map and curves for one symbol | On demand, from the score layer's in-memory state | No; recomputable from raw data within the retention window |

First version: replay-driven batch on the 15 depth symbols; on-demand detail needs a server and comes with the live stage.

## Market coverage

Target: all Binance USDT-M symbols (owner, 2026-11-10); other venues later. Order books are recorded for 15 symbols now. Whether to record all symbols' books as full diffs (about 35 times the current symbols; size not measured), as periodic snapshots, or only in memory in live runs is decided after the model works on the 15, by measuring how much accuracy a snapshot-level book loses. That decision gets its own record.

## Separate poller

Open interest, index constituents and leverage brackets are polled by a separate process, not by the collector: the collector falls under the zero-gap handoff ([0009](decisions/0009-zero-gap-handoff.md)) and a missed poll is simply retried. The existing collector has no such polling; its REST calls (depth snapshots, aggTrade backfill) are tied to its streams and share one weight budget. The poller writes the same raw-log format so the archiver and Parquet chain are reused. Binance REST weight is per IP, so the poller needs its own fixed weight cap and must read the used-weight header (`x-mbx-used-weight-1m`, verified; see [open-questions.md](open-questions.md)).

Polled data and the minimum frequency (a stored snapshot is the only way to know the value in force at a past time):

| Data | Changes | Minimum | Note |
|---|---|---|---|
| Leverage brackets, every symbol | Binance updates tiers and adds symbols at any time | Daily snapshot, each stored with its fetch time; a changed table is kept as a new version | Source under test (page JSON endpoint, otherwise a read-only key); until a source works, one-off copies are labelled with their date |
| Open interest | Continuous | Every 5 minutes while recording; history of about 30 days can be back-filled once ([oihist](../research/wicker_research/oihist/__init__.py)) | Back-fill expires, start before it does |
| Index constituents and weights | Over time | Daily | |
| Symbol list and `exchangeInfo` | New listings | Daily | Needed for coverage and the depth symbol re-check |

## Language and porting

Python under `research/` first; Go after the formats are frozen and results are stable. Details and the equality check: [0010](decisions/0010-liquidation-model-direction.md) item 5. To verify: whether live operation on the 2 vCPU / 4 GB recorder is feasible in Python; if not, port before connecting.

## Build order

1. Measure mark-price versus contract-price behavior on recorded data (open question 1); settle the remaining open questions.
2. Liquidation map from trades and liquidations; calibration on available days.
3. Push cost and forced-flow walk on `book_at`.
4. Backtest against the baseline, report with N.
5. JSON output and the static page, driven by replay.
