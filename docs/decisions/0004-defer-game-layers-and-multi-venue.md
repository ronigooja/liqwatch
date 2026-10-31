# 0004: Defer game-state layers, deep models, and other exchanges

Status: accepted

Context: public L2 lacks order-level lifecycle, so latent game states are weakly supervised and likely to overfit. Multi-exchange data adds clock and alignment cost before any baseline signal is known.

Decision: do not schedule latent game-state features, TCN/Hawkes, fusion, or other exchanges until feature layers 1–4 are evaluated. Then apply the ablation rule in [05-models.md](../05-models.md).

Consequences: smaller first scope; v1 sections on these topics are archived, not discarded.
