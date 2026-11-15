# REST poller

Verified in [code](../internal/poller/): the Go poller records REST data separately from the collector; governed by [0014](decisions/0014-daily-pack-storage.md), [0015](decisions/0015-recording-scope-and-granularity.md) and [0017](decisions/0017-poller-field-policies.md). Deployment: [infrastructure](infrastructure.md).

## Operation

Verified in [main](../cmd/liqwatch-poller/main.go): `liqwatch-poller -config /etc/liqwatch/config.toml` accepts `-venue`, `-once`, and `-syms`. Verified in [venue.go](../internal/poller/venue.go): `Env` and `RegisterVenue` provide per-venue configuration under `[poller.venues.<name>]`, central User-Agent, and the mapped universe. State files are per venue.

Verified in [history.go](../internal/poller/history.go): cursor and overlap support backward, forward, and window paging, coverage-based paging, deduplication and revision marking; `source_gap`, `retention_gap`, and daily `no_data` records describe missing data. An unfinished newest bucket is deferred until `ts + Step + Grace <= now`.

Verified in [scheduler.go](../internal/poller/scheduler.go), implementation commit `2fbe5cb`: retry backoff and endpoint `RetryError.After` cooldown start after the failed fetch returns, not before its request/wait time.

Verified in [binance.go](../internal/poller/binance.go), implementation commit `2ba66e2`: Binance HTTP requests convert HTTP-date `Retry-After` relative to the post-response clock, avoiding double-counting request time.

Verified in [okx.go](../internal/poller/okx.go), commit `04ca1ec`: currency account-ratio code `51012` emits `unsupported` and uses the existing persisted 24 h no-data cooldown for that channel/symbol only. Other channels and the mapping remain enabled; [trial evidence](recording-results.md#rest-production-scheduler-trial).

Verified in [poller implementations](../internal/poller/): default initial lookback is 2 h, without historical backfill. Periods are daily except Bitget history hourly; Gate `contractStats` uses 1-minute data. No venue polls open-interest history except Binance; funding history is polled only by Binance; insurance is not polled.

## Field policies

Verified: [0017](decisions/0017-poller-field-policies.md#consequences) identifies recorded and dropped channels, authoritative field tables, cursor and deduplication behavior. Nested paths use `[]` for root-array members and append `[]` for array fields; for example `[].tiers[]` ([policy.go](../internal/poller/policy.go)).

Verified in [scheduler.go](../internal/poller/scheduler.go) and [policy.go](../internal/poller/policy.go), implementation commit `375b7a8`: `DropChannel` disables scheduled initialization, fetches and retries in continuous and `-once` runs, suppresses output, and preserves existing cursor state.

Verified in [main](../cmd/liqwatch-poller/main.go): each sweep logs `poller unknown fields venue=<venue> channel=<channel> count=<n>` for each channel with a nonzero cumulative count.

Verified: the MEXC audited-undefined classification follows [0017](decisions/0017-poller-field-policies.md); exact fields and bounded official-reference audit are recorded once in [facts](open-questions.md#mexc-contract-detail-audit).

Verified in [test helpers](../internal/poller/policy_helpers_test.go): `withoutPolicies`, `assertVenueGolden`, and `assertVenueDedup` support baseline, filtered golden and dedup tests. When adding samples, never modify or add `*.json` under `internal/poller/testdata/<venue>/`: [TestFixtureRecordGolden](../internal/poller/fixture_golden_test.go) scans them. Use `.jsonl` or `.txt` for new samples.

## Configured channels

| Venue | Verified channels and endpoint paths in linked code; default daily unless noted |
|---|---|
| [Binance](../internal/poller/binance.go) | `exchangeInfo`, `brackets`, `constituents`, `openInterestHist`, four long/short or taker ratios, `fundingRate`; `/fapi/v1/`, `/futures/data/`, and website brackets |
| [Bybit](../internal/poller/bybit.go) | `accountRatio`, `riskLimit`, `instruments`; `/v5/market/account-ratio`, `/risk-limit`, `/instruments-info` |
| [OKX](../internal/poller/okx.go) | Four contract ratio/taker series, account ratio, `instruments`, `positionTiers`; `/api/v5/rubik/stat/`, `/api/v5/public/` |
| [Bitget](../internal/poller/bitget.go) | `longShort`, `accountLongShort`, `positionLongShort`, `takerBuySell` hourly; `positionTiers`, `contracts` daily; `/api/v2/mix/market/` |
| [Gate](../internal/poller/gate.go) | `contractStats`, `riskLimitTiers`, `contracts`; `/futures/usdt/contract_stats`, `/risk_limit_tiers`, `/contracts` |
| [MEXC](../internal/poller/mexc.go) | `contractDetail`; `/api/v1/contract/detail` |
| [KuCoin](../internal/poller/kucoin.go) | `riskLimit`, `contracts`; `/api/v1/contracts/risk-limit/`, `/api/v1/contracts/active` |
