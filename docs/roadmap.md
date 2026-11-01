# Roadmap

Durations are rough estimates; replace with measured values.

| Phase | Work | Pass condition | Estimate |
|---|---|---|---|
| P0 Data recording | Tiered self-recording in Japan, replayable raw log. Details: [data-recording.md](data-recording.md) | Acceptance list in that file | ~4 weeks |
| P1 Event library and baseline | Volatility-normalized labels, event statistics per tier, conditional-frequency baseline, fill simulator ([trading-and-risk](trading-and-risk.md)) | Labels reproducible and leak-free; baseline cost-adjusted expectancy measured | 2–3 weeks |
| P2 Models | Baselines L and G, calibration, risk state and in-spike tasks ([models](models.md)) | Stable out-of-sample gain over baseline and positive cost-adjusted expectancy | 3–4 weeks |
| P3 Shadow run | Live signals, hypothetical fills, no orders | Shadow results consistent with backtest | ongoing |
| P4 Simulated and minimal-capital trading | Real order path, latency and fill quality measurement | Live-vs-shadow gap bounded; risk controls trigger as designed | staged |
| P5 Scale and iterate | Gradual capital increase; add feature layers by ablation | Each layer shows independent out-of-sample gain | ongoing |

P0 runs in parallel with: event statistics from tier A data, fill-simulator design, label ambiguity rules.

## Gates and stop rules

1. End of P1: baseline cost-adjusted expectancy not positive and model gain small -> no automated entries; use risk state for risk control only.
2. End of P2: in-spike models not stably above baseline -> reduce scope to risk control.
3. P4: large live-vs-shadow gap -> revisit the fill simulator in P1.

Total time to judge trading value: about 2–3 months.
