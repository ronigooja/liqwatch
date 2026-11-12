# 0012 The trade-flow liquidation map is not used to compute gain `G` or `N*`

Date: 2026-11-12. Refines [0010](0010-liquidation-model-direction.md); supersedes nothing.

## Decision

The present trade-flow map is not used to compute `G` or `N*` as single values until a map meets the [scale standard](../liquidation-results.md). Any later number must carry the observed spread.
The book-walk part of push cost continues independently; full cost remains unfinished ([design](../liquidation-model.md#quantities-each-defined-once)).
Further map work needs a new input or changed assumption; more tuning of the present form is not planned.

## Why

- [Scale and fitting results](../liquidation-results.md#a-scale-stability-over-seven-days) do not demonstrate stable scale across days and sides.
- The standard is a deployment gate, not a statistical test of the latent map; observed liquidations are a varying lower bound and the cause is not identified.
- [Causal and parameter results](../liquidation-results.md) do not establish that tuning closes the gap; the causal test is not an upper bound on information.
- The opening-trade assumption is unverified and may be a weak point; this is an inference.
- Recorded liquidations remain valuable as a check; see [0011](0011-no-alert-narrower-recording.md).
