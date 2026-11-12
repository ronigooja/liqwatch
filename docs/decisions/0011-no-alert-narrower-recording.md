# 0011 No alert; recording narrowed to data that cannot be fetched

Date: 2026-11-11. Supersedes items 2 (first use) and 6 (retention) of [0010](0010-liquidation-model-direction.md); the rest of 0010 holds. [0006](0006-alert-interface.md) stays valid for data-health alerts only.

## Decision

1. The liquidation model does not produce an alert (owner, 2026-11-11). Its output is the estimate itself: liquidation map, push cost, break-even `N*`, shown on the static page. `liqwatch-alertd` is not built for the model. No order placement (unchanged).
2. Push cost is computed from the live in-memory order book at the moment of use. Saved order books are not required for that, and are not recorded until a use needs them (for example checking the push-cost arithmetic against recorded books, or a study that needs book history). Recorded 15-symbol books from 2026-11-04 to 2026-11-10 exist for such checks.
3. Quote (`bookTicker`, top 50) is no longer recorded: the full book contains the touch, and nothing reads the quote data (about 12 GB of 18 GB per day compressed, measured 2026-11-05).
4. Record what cannot be fetched from public sources ([open-questions.md](../open-questions.md), 2026-11-11): liquidation orders (no public history) and the 1-second mark price (public history is 1-minute). Trades, open interest, funding and 1-minute mark, index and premium candles can be fetched; recording trades remains optional until the fetch is built and checked for gaps.
5. Retention (replaces 0010 item 6): keep recorded raw data while storage allows; reduce older data only when storage forces it. The 30-day window is dropped.

## Why

- No alert means no threshold tuning, no false-alarm budget and no replay of alerts, which were the reasons for saving books ([liquidation-model.md](../liquidation-model.md) Compute and storage).
- The recorder stopped when its traffic quota ran out; at 18 GB per day the quota is the binding limit, and quote is two thirds of it.

## Consequences

- Collector configuration change (quote off, depth off or reduced) is a code and deploy change; it needs the next collector upgrade and the owner's confirmation of the traffic limit, which is not recorded anywhere.
- The earlier evaluation framing (alert recall, false alarms per hour) is not a goal. A prediction test remains useful only as a check of whether the map carries information, see [liquidation-model.md](../liquidation-model.md).
- Open: owner traffic quota and reset date; which host resumes recording.
