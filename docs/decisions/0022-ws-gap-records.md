# 0022: Self-sufficient WebSocket gap records

Record observable connection boundaries without scanning market files. Extends [0018](0018-path-based-raw-envelope.md); payload definition: [storage](../storage-design.md#websocket-gap-payloads).

## Date

2026-11-17

## Decision

- Verified implementation: put last-message times, uptime and message count in `conn_closed`; record successful subscription writes in `conn_opened` and first received messages in `conn_first`.
- Verified implementation: preserve envelope keys and queue-drop/REST semantics. Record known Bybit ticker snapshots without inventing replay or acknowledgement guarantees.

## Why

| Question | Method and outcome | Date and source |
|---|---|---|
| Why enrich gaps? | Measured (owner report): 6-hour recorder run, 174 reconnects: Bitget 86, KuCoin 51, Bybit 33, OKX 4; 170 resets by peer, 4 EOF. Silence versus loss required scanning 13 GB with a threshold; spread not supplied | 2026-11-17 task evidence; original run command/data location not supplied |

- Inferred: the reset cause itself is not observable from our side; the error string identifies a symptom, not its upstream cause.

## Consequences

- Verified: gap files alone delimit observed receive silence per shard, including silence before close and after subscribe; missing boundary events remain unknown.
- Inferred: observed silence does not prove market-data loss. Added hot-path cost is reader-local bookkeeping, not benchmarked; details in storage.
- Verified: no deployment, retrospective rewriting or remote recorder inspection is included.
