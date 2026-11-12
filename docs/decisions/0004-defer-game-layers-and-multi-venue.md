# 0004: Defer game-state layers, deep models, and other exchanges

Multi-venue design is permitted by [0010](0010-liquidation-model-direction.md); collection remains deferred.

Status: accepted

Context: public L2 lacks order-level lifecycle, so latent game states are weakly supervised and likely to overfit. Multi-exchange data adds clock and alignment cost before any baseline signal is known.

Decision: do not schedule latent game-state features, TCN/Hawkes, fusion, or other exchanges until feature layers 1–4 are evaluated. Then apply the ablation rule in [models.md](../models.md).

Consequences: smaller first scope; v1 sections on these topics are archived, not discarded.
