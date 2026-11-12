# 0001: Self-record market data, starting in phase P0

Scope later narrowed by [0011](0011-no-alert-narrower-recording.md).

Status: accepted

Context: no free full historical incremental L2 for Binance perpetuals. Spikes last 1–60 s and are rare, so history length matters and must start accumulating immediately.

Decision: record all data ourselves, with a light tier for all symbols and a depth tier for a selected subset. Purchased history may supplement later.

Consequences: data accrues from day one of P0; later phases depend on recording quality. Details: [data-recording.md](../data-recording.md).
