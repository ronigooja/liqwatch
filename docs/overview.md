# Overview

## Purpose

The main direction is the [liquidation model](liquidation-model.md), with Binance implemented first and a multi-venue design ([decision 0010](decisions/0010-liquidation-model-direction.md)). Recording and model scope were narrowed by [0011](decisions/0011-no-alert-narrower-recording.md); the trade-flow map does not compute G or N* ([0012](decisions/0012-map-not-used-for-gain.md)). Recorder status: [p0-rollout.md](p0-rollout.md).

## Retained prior work: risk-state and in-spike products

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
- Game-theory state models, TCN/Hawkes, and model fusion require the ablation rule in [models.md](models.md).

## Scope

- Current implementation: Binance USDT-M perpetuals; the model design covers multiple venues ([0010](decisions/0010-liquidation-model-direction.md)).
- Instruments and roles: [universe.md](universe.md).
- Phases and gates: [roadmap.md](roadmap.md).
