# 0022: Self-sufficient WebSocket gap records

Record observable connection boundaries without scanning market files. Extends [0018](0018-path-based-raw-envelope.md); payload definition: [storage](../storage-design.md#websocket-gap-payloads).

## Date

2026-11-17

## Decision

- Verified implementation: put last-message times, uptime and message count in `conn_closed`; record successful subscription writes in `conn_opened` and first received messages in `conn_first`.
- Verified implementation: preserve envelope keys and queue-drop/REST semantics. Do not emit per-symbol Bybit snapshot gaps or invent connection-wide resync completion; a summary would require new aggregation state.

## Why

| Question | Method and outcome | Date and source |
|---|---|---|
| Why enrich gaps? | Measured: single 6-hour recorder run; 174 reconnects: Bitget 86, KuCoin 51, Bybit 33, OKX 4; 170 TCP resets by peer, 4 EOF; every reconnect matches a journal line | 2026-11-17; binaries `4148b94`; evidence below |
| Can a threshold scan identify loss? | Measured: 13.5 GB, 27,085 files, 69,385,991 rows, 0 integrity errors; receive-time silences flagged, but quiet symbols could not be separated from loss | Same single run and recorder; main-session recorder test |

Measured source: main-session recorder test on kernel `6.1.0-50-amd64`, 2 vCPU/2 GB, HDD data directory; `root@VT01OK:/root/liqwatch-full6h-20261116T184916Z/reconnect-journal-events.json`.

- Inferred: the reset cause itself is not observable from our side; the error string identifies a symptom, not its upstream cause.

## Consequences

- Verified: gap files alone delimit observed receive silence per shard, including silence before close and after subscribe; missing boundary events remain unknown.
- Inferred: observed silence does not prove market-data loss. Added hot-path cost is reader-local bookkeeping, not benchmarked; details in storage.
- Verified: no deployment, retrospective rewriting or remote recorder inspection is included.
