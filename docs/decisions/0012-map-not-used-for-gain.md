# 0012 The trade-flow liquidation map is not used to compute gain `G` or `N*`

Date: 2026-11-12. Refines [0010](0010-liquidation-model-direction.md) build order; supersedes nothing.

## Decision

1. The liquidation map `Q` built from trades, bracket tiers and open interest (liqmap, `bucket_both`, 8 h) is not used to compute `G` or `N*` as single values. Reason: the owner-set standard was that observed/predicted liquidation scale stays within 2x across symbols, days and sides; over seven days it is 24x to 80x, also after a per-symbol, per-side coefficient fitted on the first two days ([liquidation-model.md](../liquidation-model.md), Consistency check).
2. In the ridge causal evaluation the localised map adds small average gains over past-only features plus total mass (seven days: 10 intervals above 0, 2 below, 24 containing 0; mean gain in Spearman between -0.011 and +0.034; the earlier GBM run +0.044). This is not an upper bound on the map's information.
3. The book-walk part of push cost `C` depends only on the order book and continues separately, computed from the live book. The full `C` also needs how far mark price follows the contract price, fees and unwind ([liquidation-model.md](../liquidation-model.md) Quantities); that part is not done.
4. A one-symbol fit (ZEC, owner request) with 14 free leverage weights, long/short split by the top-trader position ratio and mark-price survival did not bring the test days within 0.5 to 2 times; see [liquidation-model.md](../liquidation-model.md). Further work on `Q` needs a new input or a changed assumption (for example a real position or leverage source, or a stock-of-positions variant for BTC and ETH); more tuning of the present form is not planned. Window length (4, 8, 24 h), trend, leverage mix and per-symbol scale were tried and did not close the spread.

## Why

- The spread is between days and sides, so a per-symbol constant cannot remove it; leave-one-day-out and fits without the extreme day give the same verdict (about half the cells within 0.5 to 2 times the coefficient, spread 34x to 77x).
- The 2x standard is a deployment gate, not a statistical test of the latent map: observed liquidations are a lower bound with varying capture. What is established is that a stable scale has not been demonstrated; the cause is not identified (map form, censored observations, regime or sparse cells). Independent method review 2026-11-12: partly supported, wording narrowed accordingly.
- Binance publishes no positions or leverage; the opening-trade assumption is unverified and is the likely weak point (inference, not shown).

## Consequences

- No `G` or `N*` is reported until a map meets the standard. Any later number must carry the observed spread.
- Recorded liquidations stay valuable as the only check; see [0011](0011-no-alert-narrower-recording.md).
