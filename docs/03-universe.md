# Universe

## Tiers by role

| Type | Expected character | Role |
|---|---|---|
| Major (BTC, ETH) | Rare spikes, tight spreads, heavily arbitraged | Control and risk reference |
| Ordinary altcoins | Enough spikes, recoverable shape, moderate cost | Primary experiment set |
| Gainers-list altcoins | Frequent spikes, thin depth, severe adverse selection, fast drift | Record, shadow-run, risk control only until stable evidence exists |

These are judgments, not measurements; P1 results replace them ([07-roadmap.md](07-roadmap.md)).

## Selection rules

- Depth-recording set (tier B, see [02-data-recording.md](02-data-recording.md)): BTC and ETH, plus ordinary altcoins split into 2–3 liquidity bands, 5–10 per band. Liquidity measure and band cutoffs: to be defined in P1.
- Gainers-list membership uses only information available at the time (for example trailing 1-hour return rank). A symbol promoted to the depth set records its promotion time; backtests must respect it.
- Evaluate every result per tier and per liquidity band, never only pooled. Trade only tiers with positive stable expectancy.
