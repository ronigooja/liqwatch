# Universe

## Tiers by role

| Type | Expected character | Role |
|---|---|---|
| Major (BTC, ETH) | Rare spikes, tight spreads, heavily arbitraged | Control and risk reference |
| Ordinary altcoins | Enough spikes, recoverable shape, moderate cost | Primary experiment set |
| Gainers-list altcoins | Frequent spikes, thin depth, severe adverse selection, fast drift | Record, shadow-run, risk control only until stable evidence exists |

These are judgments, not measurements; P1 results replace them ([roadmap.md](roadmap.md)).

## Selection rules

- Depth-recording set (tier B, see [data-recording.md](data-recording.md)): BTC and ETH, plus ordinary altcoins split into 2–3 liquidity bands, 3–6 per band, 10–20 symbols in total. Expansion happens only after cost-adjusted profit is shown. Liquidity measure and band cutoffs: to be defined in P1.
- Gainers-list membership uses only information available at the time (for example trailing 1-hour return rank). A symbol promoted to the depth set records its promotion time; backtests must respect it.
- Evaluate every result per tier and per liquidity band, never only pooled. Trade only tiers with positive stable expectancy.
