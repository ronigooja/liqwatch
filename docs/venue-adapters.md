# Venue adapters

Design and verified implementation status of collector adapters; governed by [0011](decisions/0011-no-alert-narrower-recording.md), [0013](decisions/0013-no-dex-recording.md), [0014](decisions/0014-daily-pack-storage.md), [0015](decisions/0015-recording-scope-and-granularity.md), and [0016](decisions/0016-drop-unneeded-payload-fields.md). API observations: [venue APIs](venue-apis.md#live-adapter-captures).

## Contract

Verified in [adapter.go](../internal/venue/adapter.go): `Symbols` discovers venue-form instruments, `Shards` defines connections, and `Subscribe` and `Heartbeat` define outbound frames. Call `Symbols`, then `Shards`, before `Split` or optional `ConnSplitter.SplitConn`; shared state must be concurrency-safe.

Verified in [collector](../internal/collector/collector.go): optional `ConnLifecycle` signals open and close. The collector reads, runs heartbeats and monitors stalls while a goroutine writes subscriptions; the stall clock starts on the first frame or last subscription, whichever comes first. A subscription write error ends the connection with that error.

Verified in [adapters](../internal/venue/), [poller](../internal/poller/venue.go), [backfill](../internal/backfill/backfill.go) and [discovery](../internal/symbols/symbols.go): REST requests use `Mozilla/5.0` as User-Agent. Measured: OKX Cloudflare returned HTTP 403, code 1010, for a non-browser User-Agent (owner report, 2026-11-15). WebDAV and metrics checks are unchanged.

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
| Cadence and deviations | Measured: [capture](venue-apis.md#live-adapter-captures); ticker state isolation: [recorded content](#recorded-content) |

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
| Shards | Up to 100 symbols per market shard; separate global shard; decimal-size dial header; subscription spacing 100 ms for market, 700 ms for all |
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
| Channels | Classic execution → `trade`; classic instrument `mark.index.price` → `mark`, `funding.rate` → `funding`; UTA → `utamark` (mark price and open interest); subscriptions unchanged |
| Drop classes | trade → trade; mark, funding and utamark → mark |
| Shards | Classic: 25 symbols; UTA: 50 symbols; token acquired for classic connections |
| Cadence and deviations | Measured: [capture](venue-apis.md#live-adapter-captures); classic tokens (24 h) are renewed through `Shard.Refresh` before each reconnect or planned replacement (`MaxAge` 12 h), verified live and by test; Chinese-named symbol limitation: [open issues](open-questions.md#recording-open-issues) |

## Recorded content

Verified in the linked adapter code: unknown fields are retained and counted. Adapters that sample eligible frames produce a complete `{"sample":...}` frame at most once per channel and shard per hour; [Binance](../internal/venue/binance/testdata/README.txt) does not currently emit samples. Subscribe errors become `gap` records with `event=subscribe_error` and the frame; acknowledgements are counted.

Verified in [collector](../internal/collector/collector.go): all seven adapters share one WebSocket dial/read/reconnect path; none implements a separate connect loop. Lifecycle gap fields and known Bybit snapshot events are defined once in [storage](storage-design.md#websocket-gap-payloads), under [0022](decisions/0022-ws-gap-records.md).

Verified: the per-venue field filter is built, including explicit Binance and MEXC trade tables; [0016](decisions/0016-drop-unneeded-payload-fields.md#consequences) links to each adapter's authoritative tables and describes exceptions.

Verified in [Bybit](../internal/venue/bybit/bybit.go): ticker state is per connection and symbol and released on close, fixing the former overlap contamination window. A partial record is suppressed while another open connection of the shard has complete state for that symbol; otherwise it is marked `partial` until a snapshot.

Verified in [Bitget](../internal/venue/bitget/bitget.go): consecutive identical filtered ticker payloads are deduplicated, with forced emission after 60 seconds.

Verified: [0017](decisions/0017-poller-field-policies.md#consequences) specifies polled channels and MEXC WebSocket alignment. Verified in [Gate](../internal/venue/gate/gate.go): `Shard.Header` sets `X-Gate-Size-Decimal: 1`. Verified in [OKX](../internal/venue/okx/okx.go): WebSocket uses port 443.

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
| KuCoin | `mark`, `utamark`: 1 s; `funding`: 60 s; distinct channel/symbol buckets prevent classic funding from replacing mark/index records |
