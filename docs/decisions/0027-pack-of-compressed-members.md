# 0027: Store already-compressed raw members

Purpose: choose the daily container for live-zstd recording. Governs [storage](../storage-design.md#pack-and-compress-after-the-utc-day-boundary).

Date: 2026-11-17

## Decision

Verified: when any day member ends in `.jsonl.zst`, store all members unchanged in `<date>.tar`, unit `stored_tar`, level 0; this includes mixed transition days.
Verified: plain-only days retain configured `tar_then_zstd` or legacy `zstd_then_tar`. This supersedes the compression-unit choice in [0014](0014-daily-pack-storage.md) only for days containing live-compressed members.

## Why

Measured: [recorder-local second pass](../packing-results.md#second-pass-compression) expanded its tar by 73 bytes (0.002694%); three repeats agree.
Inferred policy: require at least 1% reduction to justify a second CPU/compression pass; smaller gains do not justify work proportional to every stored byte. This sample falls below that threshold.
Inferred limitation: one largest-first sample is not all venues/days; revisit if representative future samples exceed the threshold. The decision is based on real records, not the synthetic control.

## Consequences

Verified: remote names are `raw/<venue>/<symbol>/<date>.tar` plus `.meta.json`; `.tar.zst` is used only with outer zstd.
Verified: manifest paths identify the container; `.jsonl.zst` member names identify stored compression. Member hashes/sizes cover source stored bytes, never decoded records.
Verified: metadata records `stored_tar` and level 0. Streaming [0023](0023-packer-streaming.md), cache release [0025](0025-packer-drop-page-cache.md), and deletion gate [0021](0021-gate-archiver-deletion.md) are unchanged.
