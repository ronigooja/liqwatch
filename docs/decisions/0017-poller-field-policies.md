# 0017 REST poller field policies

Owner decision, 2026-11-15, except the three exceptions below (proposed by the main session from the owner's rules, not confirmed one by one); refines [0015](0015-recording-scope-and-granularity.md) and [0016](0016-drop-unneeded-payload-fields.md).

## Decision

Verified: apply the collector's modelling-only rule to polled rows and response bodies using explicit channel keep/drop policies; retain and count unknown fields ([implementation](../../internal/poller/policy.go)).
Verified: keep venue-provided ratios and shares, not their numerators, denominators or counts; omit taker-volume channels (trades are recorded), collector duplicates and fields without official definitions.
Inferred, not individually confirmed by the owner: intentional exceptions are Binance `brackets` fields without official definitions (the only tier source), KuCoin `nextFundingRateDateTime` (its only next-funding-time source), and MEXC `countryConfigContractMaxLeverage`; evidence and limitations: [open questions](../open-questions.md#poller-field-policy-questions).

## Why

Measured: the [2026-11-15 poller sample](../recording-results.md#poller-field-review) and verified: the venue-by-venue audit against official documentation (sub-agent reports, 2026-11-15) support the classification; unresolved definitions are in [open questions](../open-questions.md#poller-field-policy-questions).

## Consequences

Verified in the linked policy tables: these summaries are not exhaustive; code is authoritative for every field. Unregistered channels pass through unchanged ([mechanism](../../internal/poller/policy.go)).

| Venue | Channel | What is recorded | Removed |
|---|---|---|---|
| [Binance](../../internal/poller/binance_policy.go) | `openInterestHist`, three long/short ratios, `fundingRate`, `brackets`, `exchangeInfo` | OI, ratios, funding, tiers and instrument identity | Unneeded fields; entire `constituents` and `takerlongshortRatio` channels |
| [Bybit](../../internal/poller/bybit_policy.go) | `accountRatio`, `riskLimit`, `instruments` | Ratios, risk tiers and instrument metadata | Symbol duplicates, wrappers and unrelated instrument fields |
| [OKX](../../internal/poller/okx_policy.go) | Four ratio histories; `instruments`, `positionTiers` | Ratio histories unchanged as `[ts, ratio]`; filtered instruments and tiers | Unneeded metadata; entire `takerVolumeContract` channel |
| [Bitget](../../internal/poller/bitget_policy.go) | Three ratio histories; `positionTiers`, `contracts` | Ratio histories unchanged; filtered tiers and contracts | Unneeded metadata; entire `takerBuySell` channel |
| [Gate](../../internal/poller/gate_policy.go) | `contractStats`, `riskLimitTiers`, `contracts` | Ratios, OI, prices, liquidations, tiers and contract metadata | Counts, volume components and unrelated metadata |
| [KuCoin](../../internal/poller/kucoin_policy.go) | `contracts`, `riskLimit` | Funding time and contract/risk metadata | Undefined constants and unrelated metadata |
| [MEXC](../../internal/poller/mexc_policy.go) | `contractDetail` | Contract size, leverage and risk metadata including `countryConfigContractMaxLeverage` | Unneeded metadata including `openingTime` |

Verified in [MEXC collector](../../internal/venue/mexc/mexc.go): the same two field choices apply to WebSocket `contract`; `ordersetting` and `contractmeta` are no longer recorded.
Verified in [history](../../internal/poller/history.go) and [scheduler](../../internal/poller/scheduler.go): cursor timestamps come from unfiltered rows; deduplication and revision marking compare filtered payloads. A policy change can cause one extra revision-marked record per row on its first fetch; no `unfiltered_payload_sha256` field is emitted.
Verified in [policy.go](../../internal/poller/policy.go): `[]` addresses object members of a root array; syntax and operation: [poller](../poller.md#field-policies).
