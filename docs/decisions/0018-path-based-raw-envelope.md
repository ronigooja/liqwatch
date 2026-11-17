# Path-based raw envelope

Verified: records the owner-authorized compact envelope; governs [storage design](../storage-design.md#writing).

## Date

2026-11-15

## Decision

- Verified: [0026](0026-live-zstd-recording.md) supersedes only the plain-JSONL storage assumption for opt-in compressed writes; envelope v2 and path identity remain unchanged.

- Verified: for new collector and poller writes, remove envelope `ch`, `sym`, `venue`, and diagnostic `conn`; use version 2 and recover identity from the storage path.
- Verified: supersedes only the envelope-field deferral in [0016](0016-drop-unneeded-payload-fields.md); its payload policies and [0017](0017-poller-field-policies.md) remain unchanged.
- Verified: annotate only the superseded deferral in 0016; its remaining historical content stays unchanged.

## Why

- Verified in [rawlog](../../internal/rawlog/writer.go) and [poller](../../internal/poller/sink.go): paths already encode venue, symbol scope and channel.
- Verified: the owner excludes connection diagnostics from modelling; no channel is removed by this decision.

## Consequences

- Verified: retain receive times, sequences, exchange timestamps, payloads and poller metadata; do not rewrite existing recordings.
- Verified: readers accept legacy self-describing records and recover compact identity from intact paths; renamed compact files without identity fail explicitly.
- Inferred: without connection identity, replay can bound a connection gap by the next observed stream message, not distinguish overlapping connections.
