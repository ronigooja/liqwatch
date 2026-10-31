# Trading and Risk

## Strategy candidates (priority order)

| Candidate | Logic | Assessment |
|---|---|---|
| Risk-state control | Reduce size, cancel orders, tighten stops when risk is high | Most robust; gain is smaller losses |
| In-spike retrace | Fade a spike when first-touch model signals exhaustion | Most plausible edge; highest adverse-selection and knife-catching risk |
| In-spike extension | Follow when extreme refresh is likely | Entry is chasing; cost is high |
| Pre-spike direction entry | Enter on predicted direction before the spike | Not pursued |

## Fill simulator (built in P1)

- Walk the replayed book for market orders.
- Add order latency; start with 200 ms to 1 s and run sensitivity analysis.
- Limit orders: simulate queue position and partial fills; never assume a resting order fills at the touch price.
- Include maker/taker fees at the actual account tier (see [open-questions.md](open-questions.md)).
- Report sensitivity of results to each assumption. Backtests that fill at the trigger price are invalid.

## Latency

The signal-to-order path must be short because the usable window is 5–30 s. Latency budget is set from simulator sensitivity results in P1, not fixed in advance.

## Risk controls (defined before models are used for trading)

- Per-trade maximum loss; per-symbol and account exposure caps; daily loss circuit breaker.
- Hard stop placed on the exchange side, not only in local software.
- Stop opening positions when data health is degraded.
- Automatic downgrade or halt on model drift or abnormal false-alarm rate.
- Staged rollout: simulated, then minimal capital, then scale, each with explicit exit criteria ([roadmap.md](roadmap.md)).

## Known traps

- High win rate with rare large losses can still have negative expectancy.
- Fill-at-trigger assumptions overstate results badly during spikes.
- Gainers-list selection must use only information available at the time.
