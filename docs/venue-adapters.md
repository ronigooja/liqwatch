# Venue adapters

Design and verified implementation status of collector adapters; governed by [0011](decisions/0011-no-alert-narrower-recording.md), [0013](decisions/0013-no-dex-recording.md), and [0014](decisions/0014-daily-pack-storage.md). API observations and limitations: [venue APIs](venue-apis.md#live-adapter-captures).

## Contract

Verified in [adapter.go](../internal/venue/adapter.go): `Symbols` discovers venue-form instruments, `Shards` defines connections (including optional dial headers), and `Subscribe` and `Heartbeat` define outbound frames. Call `Symbols`, then `Shards`, before `Split`; state shared across connections must be concurrency-safe.

Verified in [adapter.go](../internal/venue/adapter.go): `Split` produces filtered data payloads or per-symbol envelopes; the collector supplies receive metadata. Control frames return nil, unknown data becomes a gap record, and `Classes` selects droppable classes; unmatched channels have no assigned drop class. Symbol filtering is specified in [venue map](venue-map.md).

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
| Cadence and deviations | Measured: [capture](venue-apis.md#live-adapter-captures); ticker deltas are merged into per-symbol state; see [open issue](open-questions.md#recording-open-issues) |

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
| Channels | Classic execution and instrument; UTA `utamark` (mark price and open interest) |
| Drop classes | trade → trade; mark and utamark → mark |
| Shards | Classic: 25 symbols; UTA: 50 symbols; token acquired for classic connections |
| Cadence and deviations | Measured: [capture](venue-apis.md#live-adapter-captures); classic tokens (24 h) are renewed through `Shard.Refresh` before each reconnect or planned replacement (`MaxAge` 12 h), verified live and by test; Chinese-named symbol limitation: [open issues](open-questions.md#recording-open-issues) |

## Recorded content

Verified in the linked adapter code: explicit per-channel keep/drop field tables are authoritative; unknown fields are retained and counted. Eligible frames produce a complete `{"sample":...}` frame at most once per channel and shard per hour. Subscribe errors become `gap` records with `event=subscribe_error` and the frame; acknowledgements are counted.

Verified in [Bybit](../internal/venue/bybit/bybit.go): ticker deltas merge into per-symbol state, marked `partial` until a snapshot. Verified in [Bitget](../internal/venue/bitget/bitget.go): consecutive identical filtered ticker payloads are deduplicated, with forced emission after 60 seconds.

Verified in [MEXC](../internal/venue/mexc/mexc.go): mapped symbols only receive `contract`, `contractextra`, `ordersetting`, and `contractmeta` metadata. Verified in [Gate](../internal/venue/gate/gate.go): `Shard.Header` sets `X-Gate-Size-Decimal: 1`. Verified in [OKX](../internal/venue/okx/okx.go): WebSocket uses port 443.

## Throttling

Verified in [collector](../internal/collector/throttle.go): `[collect.throttle]` enables adapter `venue.Throttler` defaults and channel overrides. The last record per symbol and receive-time bucket is retained, flushed on timer or close; `/metrics` exposes thinned counters. Other channels are not thinned.

| Venue | Verified defaults in `internal/venue/<venue>/throttle.go` |
|---|---|
| Binance | None |
| Bybit | `mark`: 1 s |
| OKX | `mark`, `index`: 1 s; `funding`: 60 s |
| Bitget | `ticker`: 1 s; `funding`: 60 s |
| Gate | `tickers`: 1 s |
| MEXC | `mark`, `index`, `ticker`: 1 s; `funding`: 60 s |
| KuCoin | `mark`, `utamark`: 1 s |
