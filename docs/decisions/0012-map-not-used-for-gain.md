# 0012 The trade-flow liquidation map is not used to compute gain `G` or `N*`

Date: 2026-11-12. Refines [0010](0010-liquidation-model-direction.md) build order; supersedes nothing.

## Decision

1. The liquidation map `Q` built from trades, bracket tiers and open interest (liqmap, `bucket_both`, 8 h) is not used to compute `G` or `N*` as single values. Reason: the owner-set standard was that observed/predicted liquidation scale stays within 2x across symbols, days and sides; over seven days it is 24x to 80x, also after a per-symbol, per-side coefficient fitted on the first two days ([liquidation-model.md](../liquidation-model.md), Consistency check).
2. The map adds at most a small amount to a past-only baseline in the causal evaluation (seven days: 10 intervals above 0, 2 below, 24 containing 0; mean gain in Spearman at most 0.034).
3. Push cost `C` depends only on the order book and continues separately, computed from the live book.
4. Further work on `Q` needs a new input or a changed assumption (for example a real position or leverage source, or a stock-of-positions variant for BTC and ETH); more tuning of the present form is not planned. Window length (4, 8, 24 h), trend, leverage mix and per-symbol scale were tried and did not close the spread.

## Why

- The spread is between days and sides, so a per-symbol constant cannot remove it.
- Binance publishes no positions or leverage; the opening-trade assumption is unverified and is the likely weak point (inference, not shown).

## Consequences

- No `G` or `N*` is reported until a map meets the standard. Any later number must carry the observed spread.
- Recorded liquidations stay valuable as the only check; see [0011](0011-no-alert-narrower-recording.md).
