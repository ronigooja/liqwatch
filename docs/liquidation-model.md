# Liquidation Model

Decision: [0010](decisions/0010-liquidation-model-direction.md). Status: design, no code yet. Exchange facts below are unverified until moved to Verified in [open-questions.md](open-questions.md).

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
5. **Net, ratio and break-even**: `G - C` and `G / C`, reported with `N`. Break-even `N*` = the smallest `N` for which `G - C >= 0`; it is the number to alert on and to display.

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
- Alert when the ratio passes a threshold, sent by `wicker-alertd` later ([0006](decisions/0006-alert-interface.md)).
- The page states that liquidation levels are estimated, not real positions, and carries no accuracy claim.

## Compute and storage

Which data is saved and why:
- Saved order books serve backtest, calibration, replay of missed or false alerts, and book-reconstruction checks. Live computation reads the in-memory live book, not saved data.
- Retention follows [0010](decisions/0010-liquidation-model-direction.md) item 6. A per-second summary of book thickness near the touch may be enough for a rough push cost; its accuracy is not measured.

Two layers:

| Layer | Content | Runs | Saved |
|---|---|---|---|
| Score | Per symbol, every few seconds: distance to nearest cluster up and down, push cost, forced flow, `N*`, data status | For all symbols continuously. Alerts need it, and the model state is rolling (recent trades, 1-minute basis), so it cannot be started on demand without a warm-up replay | Yes, with the model version. Size and interval are not measured |
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
