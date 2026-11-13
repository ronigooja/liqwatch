# P0 Core Component Design

Components 2, 3, 4, 5, 8 from [components.md](components.md). Recording scope and tiers: [data-recording.md](data-recording.md). Hosts: [infrastructure.md](infrastructure.md).

Verified in code: the collector no longer records depth, quotes or snapshots. Book-specific sections below describe historical data/research only; the current writer and archiver are defined in [storage-design.md](storage-design.md).

Fact status markers: **[V]** verified from a cited source in [open-questions.md](open-questions.md); **[U]** unverified, check against official futures docs before implementation.

## Process model

Three processes on one host, communicating through the raw log, not through memory.

| Process | Reads | Writes | Failure effect |
|---|---|---|---|
| collector (components 2, 3, 4) | WebSocket, REST | raw log, metrics | Recording stops; must stay minimal |
| validator (5) | raw log (tailing) | validation events log, metrics | Recording unaffected |
| archiver (6) | raw log | Verified uploads ([design](p0-support-design.md)) | Recording unaffected |
| research replay / conversion (8) | raw log, validation log | Replay and Parquet ([pipeline](p1-data-pipeline.md)) | Recording unaffected |

The validator reads the written log, not the live queue, so it can lag or crash without losing data.

## Component 2: Stream Collector

- Futures endpoints are routed by data type [V, official]: `wss://fstream.binance.com/public` (book ticker, depth) and `/market` (aggTrade, markPrice, forceOrder). The legacy unrouted paths were to be decommissioned after 2026-04-23; unrouted connections receive only public streams. Therefore shard by endpoint first.
- Verified in code: connections are sharded by venue adapter and symbol group; see [adapter interface](../internal/venue/adapter.go). Exchange limits: [open questions](open-questions.md).
- On read, before any parsing, take `recv_wall_ns` and `recv_mono_ns` and hand the raw bytes to the writer queue. Parsing for routing is limited to extracting the stream name.
- Answer server ping frames promptly; do not rely on the exchange for liveness: a connection with no message for a per-channel timeout is treated as stalled and replaced.
- Replacement connection: open the new connection and subscribe first, run both in parallel for a short overlap, then close the old one. Duplicates are kept in the log (tagged by connection id) and removed by the replay reader.
- Reconnect or stall emits a `gap` marker record for every affected stream.
- Planned restarts use an overlapping handoff between two slots ([0009](decisions/0009-zero-gap-handoff.md)).
- Futures docs do not describe a shutdown event; treat any close as a reconnect.

## Component 3: Snapshot Scheduler

- Futures WebSocket sends no initial book [V]. Fetch `GET /fapi/v1/depth` with `limit=1000` [V, official]. Weight by limit [V]: 5/10/20/50 = 2, 100 = 5, 500 = 10, 1000 = 20. The total IP weight limit is not stated on the docs page; it is in `rateLimits` of `GET /fapi/v1/exchangeInfo` [V, official], read it at startup and budget REST use against it. HTTP 429 means back off; continuing after 429 causes an automated IP ban (418) lasting 2 minutes to 3 days [V, official]. Limits are per IP, not per key.
- Triggers: stream start, resync after a validator sequence break (signal from the validator via a small control file or local socket), and a verification snapshot every 5–10 minutes per tier B symbol.
- Snapshot REST responses are written to the same raw log with their own `recv` times and request start/end times, because the snapshot's place in the message order matters for rebuild.
- Rate budget: read the limit from `exchangeInfo` at startup; keep REST weight per minute under a fixed fraction of it (fraction to be set in the trial); stagger symbols; stop and back off on any 429, never retry in a tight loop, and log it. Snapshots share the IP budget with every other REST call from the host.

## Component 4: Raw Log Writer

Verified in code: the current writer uses [storage-design.md](storage-design.md), not the historical hourly layout below.

Record envelope (one record per message, one record per line):

```
{"v":1,"wseq":<writer seq>,"conn":"<conn id>","cseq":<per-conn seq>,"rw":<recv wall ns>,"rm":<recv mono ns>,"ch":"<channel>","sym":"<SYMBOL>","p":<raw payload exactly as received>}
```

- The payload is embedded without re-serialization, so the original bytes are preserved. Source of truth for order: `(rm, wseq)`.
- Exchange event time is not copied into the envelope; it stays in the payload and is read at parse time.
- Verified in code: daily files are grouped by venue, symbol and channel; see [storage design](storage-design.md#writing) for the current envelope and layout.

## Component 5: Book Validator

- Tails the raw log per tier B symbol and rebuilds the book from the last snapshot plus depth diffs.
- Procedure [V, official futures doc]: buffer diffs from the `/public` depth stream; fetch the snapshot; drop events with `u` < snapshot `lastUpdateId`; the first processed event must satisfy `U` ≤ `lastUpdateId` and `u` ≥ `lastUpdateId`; afterwards each event's `pu` must equal the previous event's `u`, otherwise restart from the snapshot. Event quantities are absolute; quantity 0 removes the level; removing a level not in the local book is normal; for the same price the latest update wins.
- On a break: emit `seq_break(symbol, last_good_u, event_u, recv_time)`, request a new snapshot, and mark the interval unavailable until the book is resynchronized and one diff has been applied cleanly.
- Verification: compare the rebuilt book with each verification snapshot at the same update id (apply or buffer diffs up to the snapshot's `lastUpdateId`). Record per-snapshot top-N level mismatch count and a boolean pass. The mismatch rate is the core quality metric.
- Outputs an append-only validation log: `seq_break`, `resync_done`, `verify(symbol, pass, mismatch_levels)`, `unavailable(symbol, start, end)`. Unavailable intervals are derived from this log and used by replay and by label building.
- Levels beyond the snapshot depth are unknown until they change; the validator and replay must expose "known depth" so features do not treat them as empty.

## Component 8: Replay Engine

- Input: raw log plus validation log. Output: a stream of events in causal order with duplicates removed. Merge, ordering, de-duplication, backfill and gap rules, including the two collector slots: [p1-data-pipeline.md](p1-data-pipeline.md) Replay rules. Within one process the order is `(rm, wseq)`; across processes it is `rw`.
- Causality: the stream carries a replay clock equal to the event's receive time. Consumers (features, simulator) see only events at or before the clock; there is no random access to future events.
- `book_at(symbol, t)`: rebuilt book as of receive time t, built from the nearest earlier snapshot and diffs; returns "unavailable" inside unavailable intervals.
- Latency injection: replay can shift event delivery by a configured delay, so simulated order latency and data latency are modeled in one place ([trading-and-risk.md](trading-and-risk.md)).
- Determinism: the same inputs and parser version give byte-identical output; parser version is recorded in outputs.
- Checkpoints: book checkpoints every few minutes are stored during archiving so `book_at` does not replay from the start of the day.

## Acceptance for this design (before coding)

1. Every [U] item above is resolved against official futures docs and recorded in [open-questions.md](open-questions.md).
2. A 1–2 day trial measures CPU, memory, disk, and message rates on the recorder host, and fixes queue size, compression level, and tier B symbol count.
3. A hand-checked sample of recorded messages rebuilds a book that matches a verification snapshot.
