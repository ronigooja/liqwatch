# Overview

## Purpose

Support semi-automated or automated trading on Binance USDT-M perpetuals around short price spikes (about 1–60 seconds).

## Products

| Product | Question answered | Use |
|---|---|---|
| Risk state | Is the chance of an abnormal move in the next 5/10/30 s well above normal? | Risk control: reduce size, cancel orders, tighten stops |
| In-spike tracking | After a spike has started: will the extreme be refreshed, which comes first (extension or retrace), and where will the future high/low lie? | Entry and exit decisions |

Pre-spike direction prediction is excluded; its signal is expected to be near random. See [decision 0002](decisions/0002-risk-state-and-in-spike-focus.md).

## Success criterion

Expected return per trade after fees, spread, slippage, and latency, with acceptable tail loss. Calibration and ranking metrics (Brier, PR-AUC) are diagnostics only. Definitions: [labels-and-evaluation.md](labels-and-evaluation.md).

## Non-goals

- No promised win rate or accuracy.
- No manipulation or intent attribution to specific accounts.
- No multi-exchange linkage, game-theory state models, TCN/Hawkes, or model fusion until justified by the ablation rule in [models.md](models.md).

## Scope

- Venue: Binance USDT-M perpetuals; Binance spot as optional reference.
- Instruments and roles: [universe.md](universe.md).
- Phases and gates: [roadmap.md](roadmap.md).
