# Venue symbol map

Verified design and implementation of the reviewed venue-to-Binance map ([code](../internal/venuemap/map.go)); recording scope: [0014](decisions/0014-daily-pack-storage.md).

## Format and use

Verified in [config/venue-map.toml](../config/venue-map.toml): each `[[pair]]` has `binance`, `venue`, `symbol`, `exp10`, `status`, `note`. `exp10` is the rounded log10 of venue mark price divided by Binance mark price; `note` records the sampled ratio. These are price ratios, not contract-size multipliers.

Verified in [map.go](../internal/venuemap/map.go) and [filter.go](../internal/venue/filter.go): `auto` and `reviewed` are usable; `review` and `rejected` are excluded. A venue symbol cannot appear twice, nor can two usable rows share a Binance symbol at one venue. `collect.map_path` enables filtering; Binance passes through unchanged and retained symbols stay in venue form.

## Matching and regeneration

Verified in [match.go](../cmd/liqwatch-mapgen/match.go): normalize scale prefixes and XBT/BTC; exact names require a price ratio near a power of ten (log10 tolerance 0.06), otherwise reject. Chinese aliases require ratio within 3% and review. Substring candidates require ratio within 1%; short or ambiguous names and competing matches require review. Exact candidates take precedence; one venue symbol receives one proposal.

Verified in [main.go](../cmd/liqwatch-mapgen/main.go): regenerate using `liqwatch-mapgen -existing config/venue-map.toml -diff -out /tmp/venue-map.toml`; inspect output before replacing the checked-in file. Live discovery and prices can change proposals.

Verified: when regenerating a selected venue, remove old associations absent from its eligible active discovery list; preserve unselected venues and active symbols without proposals. For present proposals, preserve existing `reviewed`, `review`, and `rejected` fields.

Verified: `-diff` prints removals, changes and review entries; errors or empty discovery abort before writing. The scoped MEXC removals use [official delisting evidence](open-questions.md#mexc-delisted-mapping-associations), not bulk absence alone.

## Checked-in counts

Measured from [the map](../config/venue-map.toml), 2026-11-16; counts are rows, not verified token identity or live coverage.

| Venue | auto | reviewed | review | rejected |
|---|---:|---:|---:|---:|
| Bitget | 447 | 6 | 3 | 2 |
| Bybit | 461 | 9 | 3 | 1 |
| Gate | 492 | 8 | 4 | 3 |
| KuCoin | 477 | 9 | 4 | 0 |
| MEXC | 496 | 12 | 5 | 1 |
| OKX | 264 | 2 | 1 | 4 |

## Entries needing review

Verified in [the map](../config/venue-map.toml); Binance symbol → venue symbol.

| Venue | Entries |
|---|---|
| Bitget | BROCCOLI714USDT → BROCCOLIUSDT; LUNA2USDT → LUNAUSDT; RAYSOLUSDT → RAYUSDT |
| Bybit | BROCCOLI714USDT → BROCCOLIUSDT; SKYAIUSDT → SKYAI1USDT; XAUTUSDT → XAUUSDT |
| Gate | BROCCOLI714USDT → BROCCOLI_USDT; LUNA2USDT → LUNA_USDT; RAYSOLUSDT → RAY_USDT; RONINUSDT → RON_USDT |
| KuCoin | RAYSOLUSDT → RAYUSDTM; REDUSDT → REDSTONEUSDTM; RONINUSDT → RONUSDTM; 牛来USDT → NIULAIUSDTM |
| MEXC | BROCCOLI714USDT → BROCCOLI_USDT; DATAIPUSDT → DATA_USDT; RAYSOLUSDT → RAY_USDT; 币安人生USDT → BIANRENSHENG_USDT; 牛来USDT → NIULAI_USDT |
| OKX | RAYSOLUSDT → RAY-USDT-SWAP |
