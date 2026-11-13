# Venue adapters

Design and verified implementation status of collector adapters; governed by [0011](decisions/0011-no-alert-narrower-recording.md), [0013](decisions/0013-no-dex-recording.md), and [0014](decisions/0014-daily-pack-storage.md). API observations and limitations: [venue APIs](venue-apis.md#live-adapter-captures).

## Contract

Verified in [adapter.go](../internal/venue/adapter.go): `Symbols` discovers venue-form instruments, `Shards` defines connections (including optional dial headers), and `Subscribe` and `Heartbeat` define outbound frames. Call `Symbols`, then `Shards`, before `Split`; state shared across connections must be concurrency-safe.

Verified in [adapter.go](../internal/venue/adapter.go): `Split` preserves data payloads or per-symbol envelopes; the collector supplies receive metadata. Control frames return nil, unknown data becomes a gap record, and `Classes` selects droppable classes; unmatched channels have no assigned drop class. Symbol filtering is specified in [venue map](venue-map.md).

## Implementations

Verified in the linked code: each table describes configured subscriptions, not proof of completeness. Measured cadence and deviations are linked to their sources; sample counts are not guarantees.

### Binance

| Item | Verified in [code](../internal/venue/binance/binance.go) |
|---|---|
| Channels | `aggTrade`, `markPrice_all`, `forceOrder`; `bookTicker` class remains defined but is not subscribed |
| Drop classes | `aggTrade` → trade, `markPrice_all` → mark; legacy `bookTicker` → quote |
| Shards | Trade groups plus one all-market mark/liquidation shard |
| Cadence | No adapter fixture; historical Binance trial: [trial plan](trial-plan.md) |

### Bybit

| Item | Verified in [code](../internal/venue/bybit/bybit.go) |
|---|---|
| Channels | `publicTrade`, `tickers`, `allLiquidation` → trade, mark, liquidation |
| Drop classes | trade → trade; mark → mark |
| Shards | Up to 90 subscriptions and 21,000 argument characters per connection |
| Cadence and deviations | Measured: [capture](venue-apis.md#live-adapter-captures); ticker deltas omit unchanged fields |

### OKX

| Item | Verified in [code](../internal/venue/okx/okx.go) |
|---|---|
| Channels | `trades-all`, mark, index, funding, open interest and liquidation |
| Drop classes | trade → trade; mark, index, funding, OI → mark |
| Shards | Public and business endpoint groups; up to 100 arguments each |
| Cadence and deviations | Measured: [capture](venue-apis.md#live-adapter-captures); liquidation feed self-declared incomplete |

### Bitget

| Item | Verified in [code](../internal/venue/bitget/bitget.go) |
|---|---|
| Channels | V3 public trade, ticker, funding, position tiers and global liquidation |
| Drop classes | trade → trade; ticker → mark |
| Shards | Up to 48 market subscriptions; separate global liquidation shard |
| Cadence and deviations | Measured and documented: [capture](venue-apis.md#live-adapter-captures); position-tier object differs from documented example; 100 connections per IP is a deployment constraint |

### Gate

| Item | Verified in [code](../internal/venue/gate/gate.go) |
|---|---|
| Channels | Trades, tickers, contract stats, contract info and global liquidations |
| Drop classes | trades → trade; tickers → mark |
| Shards | Up to 100 symbols per market shard; separate global shard; decimal-size dial header |
| Cadence and deviations | Measured: [capture](venue-apis.md#live-adapter-captures); stats batching rejected, no stats update observed |

### MEXC

| Item | Verified in [code](../internal/venue/mexc/mexc.go) |
|---|---|
| Channels | Deals, fair price, index, ticker, funding and contract updates |
| Drop classes | trade → trade; mark and index → mark |
| Shards | Up to 100 subscriptions per market shard; separate contract shard |
| Cadence and deviations | Measured: [capture](venue-apis.md#live-adapter-captures); four unexplained gap records are under investigation |

### KuCoin

| Item | Verified in [code](../internal/venue/kucoin/kucoin.go) |
|---|---|
| Channels | Classic execution and instrument; UTA mark price/open interest |
| Drop classes | trade → trade; mark and openinterest → mark |
| Shards | Classic: 25 symbols; UTA: 50 symbols; token acquired for classic connections |
| Cadence and deviations | Measured: [capture](venue-apis.md#live-adapter-captures); token refresh is being fixed separately; Chinese-named symbol not captured |
