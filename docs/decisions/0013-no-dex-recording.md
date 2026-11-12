# 0013 Decentralized exchanges are not recorded

Date: 2026-11-12. Refines [0010](0010-liquidation-model-direction.md) item 7 and [0004](0004-defer-game-layers-and-multi-venue.md); supersedes nothing.

## Decision

1. No DEX data is recorded (owner, 2026-11-12): not the perpetual DEXes (Lighter, dYdX v4, Hyperliquid, Aster) and not the AMM pools (PancakeSwap, Uniswap) that appear in Binance index constituents.
2. Other centralized venues are a separate decision, not made here.

## Why

- Scale (measured 2026-11-12): Binance USDT perpetuals 24 h volume about $53.3bn and open interest about $18.4bn (516 of 522 symbols). Lighter: $2.69bn volume, $0.69bn open interest, about 5.0% and 3.8% of Binance; dYdX v4: $15.7m and $38.3m, about 0.03% and 0.2%. Venue figures are DefiLlama reported values, not normalized; Lighter's open interest includes non-crypto markets, so its overlap with Binance symbols is smaller. Details: [venue-apis.md](../venue-apis.md).
- Liquidation records (live capture): public market-wide only on dYdX v4 and Lighter; Hyperliquid per user only; Aster none observed in 26.5 minutes. Completeness unverified for all four.
- AMM pools: pool contract, fee tier and Binance's `uindex()` rule are not established, so on-chain prices cannot be tied to the index price.

## Consequences

- The multi-venue design in [liquidation-model.md](../liquidation-model.md) stays Binance-only for now; recorded scope is unchanged by this record.
- Reopen when the liquidation model shows that other venues' forced flow matters; start from [venue-apis.md](../venue-apis.md).
