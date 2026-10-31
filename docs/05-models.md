# Models

Targets are defined in [04-labels-and-evaluation.md](04-labels-and-evaluation.md).

## Order of work

1. Baseline S: historical conditional frequency with hierarchical smoothing.
2. Baseline L: logistic regression.
3. Main model G: LightGBM for risk state, extreme refresh, quantiles, and first touch; probability calibration (method chosen on validation).
4. Extreme range: model as volatility forecast times a standardized distribution; use quantile regression or conformal prediction for coverage. Evaluate the volatility part separately.

One cross-symbol model with liquidity and volatility state as inputs comes first; per-symbol calibration only if needed.

## Feature layers

| Layer | Content | Phase |
|---|---|---|
| 1 | Price path, volatility, active order flow, best bid/ask and top-N book imbalance | Core |
| 2 | Effective depth, cancel-rate proxies, impact and recovery | Add by ablation |
| 3 | Spot basis | Add by ablation |
| 4 | Open interest, funding, liquidation proxies | Add by ablation |
| 5 | Latent game-state features | Deferred |
| 6 | TCN, Hawkes, fusion | Deferred |

Book data is aggregated by price level, so order-level lifecycle is not observable. Effective depth weights must be estimated from proxy labels, with estimation error reported. Suspicious cancel patterns are labeled "abnormal book behavior", never as manipulation.

## Ablation rule

A layer is kept only if, on the sealed test set, with the same false-alarm budget, latency budget, and symbol set, it gives stable gain over the best model without it. Complexity or interpretability is not evidence. Layers 5 and 6 are not scheduled until layers 1–4 are evaluated.

## Fusion

Fuse models only if the fusion beats the best single model out of sample.

## Research assistants

LLMs may propose falsifiable feature hypotheses, write experiment code, and analyze failures offline. They are not in the online prediction path.
