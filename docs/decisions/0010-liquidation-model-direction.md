# 0010 Liquidation model as the main direction

The alert-service scope was later narrowed by [0011](0011-no-alert-narrower-recording.md).

Date: 2026-11-10. Changes the priority set in [0002](0002-risk-state-and-in-spike-focus.md) and [0003](0003-trading-expectancy-as-criterion.md); neither is superseded.

## Decision

1. The main direction is a liquidation model: per symbol and moment, estimate the cost and the gain of a hypothetical actor pushing price into a cluster of forced liquidations. Design: [liquidation-model.md](../liquidation-model.md).
2. (Superseded by [0011](0011-no-alert-narrower-recording.md): no alert.) First use is a risk feature and an alert. No order placement is supported now (owner, 2026-11-10). Position-size reporting still applies to every conclusion ([0003](0003-trading-expectancy-as-criterion.md)).
3. "Real time" first means replay-driven runs on the research host; connecting to the recorder comes after the model is stable.
4. The fill simulator, label pipeline and evaluation framework stay. They are the backtest tools for this model.
5. Languages: Python under `research/` while the model changes; port to Go when the model's input and output formats are frozen and results are stable on multi-day data. Ported output must match the Python output on the same replay before the switch. Dashboard: static HTML plus JavaScript reading a JSON file, no framework or build step. `wicker-alertd` is written in Go later, when a consumer exists.
6. (Superseded by [0011](0011-no-alert-narrower-recording.md).) Retention (owner confirmed 2026-11-10): keep full raw data for a recent window (proposed 30 days, to be fixed once storage cost is known); older data is reduced to a summary (per-second book thickness, liquidations, mark price, trades). Summary fields are defined in [liquidation-model.md](../liquidation-model.md) when calibration needs are known. Decision 0001 (self-record) is unchanged.
7. Binance is implemented first (owner, 2026-11-10), but the liquidation map is defined as a sum over venues from the start ([liquidation-model.md](../liquidation-model.md) Multiple venues), because liquidation levels cannot be judged from one venue. Output states which venues are covered and is a lower bound. The core must not use Binance field names or rules; venue rules live in one adapter per venue (Venue adapter). A venue's rules must be verified in [open-questions.md](../open-questions.md) before its adapter is written. No collection of other venues now; recording them needs a new decision because it changes [0004](0004-defer-game-layers-and-multi-venue.md).
8. Project rename (candidate name `liqwatch`) is deferred to a planned collector upgrade (owner, 2026-11-10). Names stay `wicker` until then.

## Why

- Binance does not publish positions, so the cost of moving price and the size of the forced flow beyond it are the quantities that can be estimated from recorded data.
- The recorded raw log already holds order books, liquidations, mark price and trades.

## Consequences

- Recording is unchanged except for a possible open-interest feed, which is an open question in [liquidation-model.md](../liquidation-model.md).
- Roadmap gets a liquidation-model track ([roadmap.md](../roadmap.md)). Its gate does not change the P1 gate.
