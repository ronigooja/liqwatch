# Liquidation Model

Decision: [0010](decisions/0010-liquidation-model-direction.md). Status: Python research code exists (`research/liqwatch_research/liqmap`,
   `liqcal`, `liqcausal`); the map is not used for `G` or `N*` ([0012](decisions/0012-map-not-used-for-gain.md)). Exchange facts below are
   unverified until moved to Verified in [open-questions.md](open-questions.md).

## Question answered

For symbol S, time t and direction d (push up or down): what would it cost a hypothetical actor to push price to a liquidation cluster, and what would the forced flow beyond it be worth to them?

Two different things are called a position here; keep them apart:
- **Liquidated positions**: other traders' positions that get force-closed. Modeled by the liquidation map `Q`.
- **Actor inventory `N`** (USD): what the hypothetical actor holds before the push. Only the gain and the ratio depend on `N`; the push cost does not.

Output: the book-walk part of the cost curve proceeds separately; full cost is unfinished. Forced flow, gain, ratio and break-even `N` are conditional on a map meeting [0012](decisions/0012-map-not-used-for-gain.md). It estimates incentive, not intent, and promises no outcome.

## Inputs

Recorded inputs describe historical data; recording scope is in [0011](decisions/0011-no-alert-narrower-recording.md).

| Input | Source | State |
|---|---|---|
| Order book at t | `book_at` ([p1-data-pipeline.md](p1-data-pipeline.md)) | Recorded for 15 depth symbols only; other symbols cannot be modeled |
| Mark price, funding | `mark` dataset | Recorded |
| Trades | `trades` dataset | Recorded, all symbols |
| Liquidations | `liquidations` dataset | Recorded; lower bound only, per symbol the stream shows the latest order within 1000 ms |
| Open interest | `/fapi/v1/openInterest` (public, current) and `openInterestHist` (public, 5 m periods) | Design: [poller](poller.md) |
| Index composition and weights | `/fapi/v1/constituents` (public) | Verified usable, not recorded. Weights change, so sample periodically |
| Index price | `/fapi/v1/premiumIndex` | Verified usable, not recorded |
| Leverage brackets and maintenance margin | Keyless website endpoint | Tested internal endpoint, subject to change; see [verified source](open-questions.md#verified) |

## Known input gaps (each weakens every result below)

| Gap | Current stand-in | Effect | How to close |
|---|---|---|---|
| Open interest unavailable | Treat all window trades as openings | Missing old positions and misclassified closings leave scale unanchored | Use [history](open-questions.md#verified); aggregate interest does not separate openings and closings |
| Leverage distribution of traders | Assumed weights {5, 10, 20, 50, 100} | Liquidation prices are assumptions | Not public on Binance; only assumption, back-fit from recorded liquidations, or a public DEX as a reference (unchecked) |
| Maintenance margin and tiers | `mmr = 0.005`, a placeholder, not an estimate | Every liquidation price is off by an unquantified amount | Use the tested internal website endpoint, which may change ([open questions](open-questions.md#verified)) |

Placeholder inputs do not establish a calibrated scale; see [results](liquidation-results.md).

## Quantities (each defined once)

1. **Liquidation map** `Q_d(P) = Σ_v Q_{v,d}(P)` summed over venues v; a venue not modeled contributes nothing, so the result is a lower bound
   and is shown with the list of venues covered (an uncovered venue is never displayed as zero). Per-venue `Q_{v,d}(P)`: estimated notional that
   is liquidated when mark price crosses P in direction d. Built from recent trade flow (where positions were likely opened, volume-weighted), an
   assumed leverage distribution, and maintenance margin. Total scale comes from open interest when available, otherwise from calibration. Every
   parameter is an assumption until calibrated against recorded liquidations.
2. **Push cost** `C_d(P)`, independent of `N`: taker fees plus book-walk slippage to move the *contract* price to the level that moves *mark
   price* to P, plus the cost of unwinding. Mark price is the median of Price 1 (index with funding adjustment), Price 2 (index plus 1-minute
   average basis) and the contract price ([verified](open-questions.md)). Consequence: a contract-only push moves mark price only if it moves the
   median, so the push must move the contract price past both index-based prices; the index is a spot-venue average and the contract push does
   not move it. Where the index has no Binance venue (e.g. FLOCKUSDT), the actor would also have to move spot elsewhere. `C` is therefore
   model-dependent; the first step is to test on recorded data how far mark price follows contract price during spikes.
3. **Forced flow** `F_d(P)` = `Q_d(P)` arriving as market orders into the book at P. Price after the flow `P_after` comes from walking the thinned book with `F`.
4. **Gain** `G_d(P, N)` = inventory `N` marked from the entry price to `P_after`, minus unwind slippage, funding and failure cost. Grows with `N`.
5. **Net, ratio and break-even**: `G - C` and `G / C`, reported with `N`. Break-even `N*` = the smallest `N` for which `G - C >= 0`; it is the number to display.

Fill and walk code is reused from [fill-simulator.md](fill-simulator.md).

## Measured: does mark price follow the contract price

The recorded spike study found substantial but variable same-time pass-through; it does not identify the response to an actor's order. See [measured results](liquidation-results.md#mark-versus-contract).

## Venue adapter

Scope now: Binance only. The core (quantities above, backtest, dashboard) reads only the unified inputs: order book, trades, mark price, liquidations, open interest, index price and constituents. An adapter supplies what differs by venue:

- mark-price formula and its inputs;
- the price that triggers liquidation;
- maintenance margin and leverage brackets;
- insurance fund and auto-deleveraging rules;
- endpoints, field names and fee rates.

The Binance values are defined once, in the Verified table of [open-questions.md](open-questions.md). In code, `research/` holds one adapter
   interface with Binance as the only implementation. Existing Parquet data is Binance data; a new venue gets its own root directory, so the
   frozen layout in [p1-data-pipeline.md](p1-data-pipeline.md) and the Go recorder are not changed now.

Expectation, not verified: Binance's index uses spot prices from other venues (see the constituents in open-questions), so recording those venues later could improve the push-cost estimate.

## Multiple venues

liquidation levels cannot be judged from Binance alone. Reasons (reasoning, not verified): other venues' forced orders hit their own and spot
   books, which feed Binance's index and hence its mark price; a market maker sees the combined clusters. How much of a symbol's total open
   interest sits on Binance is unknown and decides how much a Binance-only view misses.

Per venue, to verify before any adapter or recording (list in [open-questions.md](open-questions.md)): liquidation feed and whether it is
   sampled, open interest, leverage brackets, mark-price formula, rate limits. The first version computes Binance only, shows "venues covered:
   Binance", and does not claim totals. Recording other venues needs a new decision record because it changes
   [0004](decisions/0004-defer-game-layers-and-multi-venue.md).

## Open questions

See [open questions](open-questions.md) for exchange facts and unresolved inputs. In particular, contract-price definition, mark response to a push, historical tiers, clearance fees and ADL details need verification.

## Output and display

A proposed local static page reads replay-generated JSON. It shows estimated levels, book completeness, and covered venues; estimates are not real positions and carry no accuracy claim.
The book-walk cost proceeds separately. Forced flow, gain, ratio and break-even `N*` are conditional on a map meeting [0012](decisions/0012-map-not-used-for-gain.md).
The liquidation model produces no alert ([0011](decisions/0011-no-alert-narrower-recording.md)).

## Compute and storage

Push cost uses the live in-memory book; existing historical books can check the arithmetic. Recording and retention are governed by [0011](decisions/0011-no-alert-narrower-recording.md).
Replay-driven batch is the proposed first form; on-demand detail would need a server. Persisted scores and full map details remain design questions.

<a id="separate-poller"></a>Polling design: [poller](poller.md).

## Market coverage

The target is Binance USDT-M symbols, with other venues later. Historical order books cover the depth subset; book coverage needs a separate decision informed by measured snapshot accuracy.

## Language and porting

Python research precedes Go porting after formats and results are stable; see [0010](decisions/0010-liquidation-model-direction.md). Live Python feasibility remains to be checked.

## Build order

1. Resolve mark-price response and remaining input questions.
2. Research and evaluate the map against past-only and total-mass baselines using time-ordered splits.
3. Continue book-walk cost independently; full cost is unfinished.
4. Produce replay-driven JSON and a static page; map-derived forced flow, gain and `N*` require a map meeting [0012](decisions/0012-map-not-used-for-gain.md).
