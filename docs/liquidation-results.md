# Liquidation map results

The owner-set deployment criterion is scale within 2x across symbols, days and sides. Obs/pred is not a true liquidated fraction: the observation stream is a lower bound.

Common run, commit dfe4045: `python -m liqwatch_research.liqcal <day roots> --syms <SYM> --brackets <table> --oi <5-minute history> --oi-mode bucket_both --window-hours 8`.

- Inputs: Parquet from recorded raw files (2026-11-04..10); brackets are the 2026-11-11 table for all days; open-interest history is public.
- Outputs of the original runs are in `~/Agents/liqwatch-results/liqproc/root_results.tgz`, outside the repository. The ZEC fit rows were re-run from the saved decompositions with the repository script and match.

## Mark versus contract

Method: recorded one-second mark, index and trade prices; pass is the median detrended deviation ratio on spike seconds.

| Question | Method | Numbers | Date | Source |
|---|---|---|---|---|
| How much of a spike passes to mark? | Per-symbol quantiles, 20 bp threshold | 2,278,162 seconds; 502 symbols; 0.39 / 0.54 / 0.68 / 0.78 / 0.83; 79% above 0.5, 4% below 0.2 | 2026-11-05 | `research/scripts/mark_spikes.py` |
| Same, 50 bp threshold? | Per-symbol quantiles | 87,679 seconds; 227 symbols; 0.22 / 0.45 / 0.64 / 0.77 / 0.87; 69% above 0.5, 8% below 0.2 | 2026-11-05 | `research/scripts/mark_spikes.py` |

- These same-time ratios do not isolate the response to an actor's order.

## A. Scale stability over seven days

Method: realised-path consistency check, `bucket_both`, eight-hour window, five-minute horizon; parenthesised figures are crossing counts. The final day is partial.

| Symbol side (obs/pred; crossings) | 11-04 | 11-05 | 11-06 | 11-07 | 11-08 | 11-09 | 11-10 |
|---|---|---|---|---|---|---|---|
| BTC down | 1.18 (143) | 0.82 (231) | 0.50 (139) | 0.30 (15) | 4.79 (65) | 0.64 (196) | 1.05 (71) |
| BTC up | 1.13 (220) | 0.35 (93) | 0.31 (125) | 5.95 (5) | 0.15 (20) | 0.31 (186) | 0.69 (19) |
| ETH down | 1.44 (172) | 0.83 (269) | 0.89 (149) | 1.76 (22) | 8.37 (82) | 0.97 (221) | 0.80 (95) |
| ETH up | 0.64 (271) | 0.80 (117) | 0.19 (145) | 2.74 (10) | 1.75 (48) | 0.31 (212) | 0.20 (46) |
| ZEC down | 0.20 (398) | 0.24 (431) | 0.15 (370) | 0.34 (219) | 0.62 (228) | 0.19 (404) | 0.27 (146) |
| ZEC up | 0.23 (412) | 0.16 (358) | 0.28 (385) | 0.09 (227) | 0.11 (178) | 0.09 (339) | 0.02 (148) |
| HOT down | 0.55 (544) | 0.73 (530) | 0.44 (535) | 0.44 (428) | 0.21 (471) | 0.19 (481) | 0.21 (209) |
| HOT up | 0.40 (472) | 0.20 (487) | 0.05 (459) | 0.09 (272) | 0.23 (551) | 0.16 (675) | 0.11 (289) |

| Question | Method | Numbers | Date | Source |
|---|---|---|---|---|
| Do fitted coefficients generalise? | Fit first two days, test remaining | 17 of 40 within gate; 0.12–9.2, 80x; >=100 crossings: 13 of 28, 24x (uncorrected 40x) | 2026-11-04..10 | common run; coefficient test inline over `g_*` outputs (not saved) |
| Does the fit split decide the result? | Leave one day out | 26 of 56; 43 cells with >=100 crossings: 23, 34x | 2026-11-04..10 | same |
| Does the extreme day decide it? | Exclude 11-05 | 20 of 48; 36 cells with >=100 crossings: 17, 37x | 2026-11-04..10 | same |

- ESPORTS is omitted because predicted mass was zero in some cells. The test is a deployment gate, not a statistical rejection.

## B. Parameter and option trials

Method: realised-path obs/pred comparison; the ZEC fit uses disjoint training and test days.

| Question | Method | Numbers | Date | Source |
|---|---|---|---|---|
| Does window length narrow spread? | Train / test / all, four symbols | 4h 12x/19x/33x, 8h 9x/19x/32x, 24h 9x/25x/44x; medians 0.44/0.42/0.37 | 2026-11-04..06 | common run with `--window-hours 4` and `24` |
| Does open-interest mode affect ranks? | `bucket_both` versus `bucket` | Scale x0.46–0.50; ranks unchanged | 2026-11-04..06 | common run with `--oi-mode bucket` against `bucket_both`; commit ddd298d |
| Do brackets change the result? | Compare tiers | Little change to scale or ranks | 2026-11-04..06 | common run with and without `--brackets`; commit bdd7a3a |
| Does trend explain spread? | Return split, against/with; block-bootstrap intervals | BTC 0.54 [0.30, 1.33]; ETH 0.72 [0.44, 1.39]; ZEC 0.73 [0.49, 1.06]; HOT 0.85 [0.58, 1.22] | 2026-11-04..06 | `research/scripts/liq_trend_split.py`; not re-run (raw Parquet deleted) |
| Can ZEC weights generalise? | 14 leverage weights | 4 of 8, 28.6x, rho 0.554 | 2026-11-04..10 | `research/scripts/liq_decompose.py` then `liq_fit_weights.py --train 20261104,20261105,20261106 --test 20261107..20261110`; reproduced from the saved decompositions |
| Does long/short split help? | Add top-trader ratio | 5 of 8, 21.5x, 0.554 | 2026-11-04..10 | same |
| Does survival help? | Add mark-price survival | 5 of 8, 21.1x, 0.562 | 2026-11-04..10 | same |
| Which ZEC cells miss? | Training and held-out cells | Training 0.83–1.47; failing 11-08 down 2.8, 11-10 down 3.8, 11-10 up 0.18 | 2026-11-04..10 | same |

## C. Causal evaluation

Method: rolling day-pair ridge; interval counts above / below / containing zero for localised shape over total mass and past-only features.

| Question | Method | Numbers | Date | Source |
|---|---|---|---|---|
| Does shape add information? | Six held-out days per side | BTC down 1/0/5, BTC up 0/0/6; ZEC down 3/0/3, ZEC up 3/0/3; HOT down 2/2/2, HOT up 1/0/5; ETH down 1/0/5, ETH up 0/0/6 | 2026-11-04..10 | `python -m liqwatch_research.liqcausal --rolling --model ridge`, same inputs, commit dfe4045 |
| What is the aggregate? | 95% block-bootstrap intervals | 11/2/35 over 48; mean c-b between -0.011 and +0.048 | 2026-11-04..10 | same |
| Nonlinear evidence? | Earlier three-day GBM, twelve cells | 5/0/7; mean c-b +0.044 | 2026-11-04..06 | `liqcausal --model gbm`, commit 7dd371c |

- Intervals omit training-fit uncertainty and are not multiplicity-adjusted; nonsignificance does not prove absence of information.

## D. Event analysis

Method: after-the-fact ZEC realised-path event inspection.

| Question | Method | Numbers | Date | Source |
|---|---|---|---|---|
| What drove the large miss? | Inspect event and fitted prediction | 11-08 21:50–21:55 UTC; 58% of day's observed in 8 moments; fit 3.5x low, 10.9x first minute | 2026-11-08 | `research/scripts/liq_event_inspect.py`; not re-run (raw Parquet deleted) |
| Was mass present? | Compare observations to one leverage level crossed | Observed 64–75% of crossed mass | 2026-11-08 | same |
| Does liquidation share vary? | Compare same crossed mass | 72% on 11-08 21:51 vs 9% on 11-04 18:33 | 2026-11-04..08 | same |
| Is missed mass decisive? | Moments with zero prediction | 2–15% of observed | 2026-11-04..10 | same |
| Does the one-per-second stream cap hide events? | ZEC SELL records, 11-08 21:50–21:56 UTC | 77 events in 77 distinct seconds of 360; busiest minute 33 of 60. No saturation; drops inside a second are invisible | 2026-11-08 | recorded liquidation Parquet |
| Liquidation size relative to taker selling? | Liquidation notional / taker-sell volume in the window | ZEC 11-08 21:50–21:56 10.2%; ZEC 11-04 18:30–18:40 1.3%; ZEC 11-10 02:25–02:35 2.0%; BTC 11-05 14–15 UTC 2.3%; ETH 2.1% | 2026-11-04..10 | recorded liquidation and trade Parquet |

## E. Independent reviews

Method: independent synthetic code audit and methodological review.

| Question | Method | Numbers / finding | Date | Source |
|---|---|---|---|---|
| Are results affected by scoring? | Code audit | Positive-prediction conditioning of `sum_obs` excludes zero-prediction observations and flatters the ratio | 2026-11-12 | `research/liqwatch_research/liqcal` |
| Are inputs and survival aligned? | Code audit | Open-interest timestamp semantics assumed; survival by trade price may leave mark-crossed levels alive | 2026-11-12 | `research/liqwatch_research/liqmap` |
| Are tiers and numeric operations exact? | Code audit | Fragment tiers are not position tiers; float cancellation can lose small mass; silent drop of fragments with no allowed leverage | 2026-11-12 | `research/liqwatch_research/liqmap` |
| Is the decision justified? | Independent method review | Partly supported: stable scale not demonstrated, cause unknown; deployment gate not statistical rejection | 2026-11-12 | [decision](decisions/0012-map-not-used-for-gain.md) |

## F. Literature and vendor findings

Method: review the existing current documentation for attributable findings and URLs.

| Source | What it says | Relevance | Date | Method |
|---|---|---|---|---|
| [Binance stream docs](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market) | One order per symbol per 1,000 ms; current page says latest, legacy says largest | The stream is a sample, a lower bound | 2026-11-12 | Web research |
| Vendor guides (Kingfisher, CoinAnk, BitcoinCounterFlow) | Maps are relative strength; closing, margin changes and cross margin not modelled; no scale calibration or accuracy study published | Vendors show levels, not amounts | 2026-11-12 | Web research |
| Karaptic blog study (2026-10); Biyond vendor claim | Recorded liquidation features and "cluster as magnet" gave no gain over a price-and-volatility baseline | Independent weak evidence that map clusters add little | 2026-11-12 | Web research; short samples, unreviewed |
| Garcia Seuma 2026 (arXiv 2608.03616, 2607.27070) | Across seven cascades no public pre-event measure graded severity; threshold map swept by the price path untested | Nobody has validated the fraction liquidated | 2026-11-12 | Working papers, web research |
| Binance Portfolio Margin pages; clearance fee protocol | Portfolio-margin accounts liquidate on whole-account margin; a clearance fee is deducted | Single-position isolated-margin map cannot reproduce them | 2026-11-12 | Official pages, web research |
| Public research overall | No published estimate of the share of crossed positions that liquidate; no validated Binance position-level data | The fraction liquidated is not derivable from public data | 2026-11-12 | Web research |
