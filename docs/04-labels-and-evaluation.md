# Labels and Evaluation

## Event definition

Thresholds are volatility-normalized: `a = k·σ_t`, where σ_t is a short-horizon volatility estimated from past data only. Absolute thresholds (1/2/3%) are not used as the primary definition because they give near-zero positives for majors and mostly positives for small coins.

Reference price: mid price at prediction time t. Horizons h ∈ {5, 10, 30} s.

## Label tasks

| Task | Label |
|---|---|
| Risk state | Within h, `max log(P_s/P_t) ≥ a` (up) or `min ≤ −a` (down); both can hold, so they are separate binary labels |
| Extreme refresh | Given an observed in-spike high/low at t, does price exceed it by at least δ within h |
| Extreme range | Quantiles (Q10/Q50/Q90) of future max and min within h; enforce non-crossing |
| First touch | Extension threshold hit first / retrace threshold hit first / neither within h (mutually exclusive, sums to 1) |

Ambiguity rule (to be fixed in P1): when both thresholds are reached inside one data tick and order cannot be determined, assign a conservative label and report the share of such cases.

## Leakage rules

- Rolling statistics use only past data.
- Time-ordered split into train / tuning / sealed test. Purge and embargo around overlapping labels.
- No choosing thresholds, models, or symbols on the sealed test.
- Replay uses real arrival order and the data available at that moment.

## Metrics

| Layer | Primary | Not sufficient alone |
|---|---|---|
| Risk state | False alarms per hour; recall at fixed false-alarm budget; lead time P50/P90; PR-AUC | Overall accuracy |
| Probabilities | Brier, log loss, reliability curve, ECE, per regime | Raw model score as percent |
| Range | Pinball loss, coverage, average width, conditional coverage | Error of Q50 only |
| First touch | Multiclass Brier/log loss, share of unresolved order | Terminal direction |
| Trading | Expectancy per trade after costs, max drawdown, CVaR, win rate with payoff ratio, capacity | Win rate alone |

Trading metrics must be reported per tier, per month, and per volatility regime. Strategies profitable only in a few extreme events are rejected. Cost and fill assumptions: [06-trading-and-risk.md](06-trading-and-risk.md).
