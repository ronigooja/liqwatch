# 0026: Record live zstd on the root disk

Purpose: govern opt-in writer compression; extends [0018](0018-path-based-raw-envelope.md) and supersedes its plain-JSONL assumption, not envelope v2.
Verified status: existing-tick policy partially superseded by [0028](0028-record-flush-interval.md); other decisions remain in force.

Date: 2026-11-17

## Decision

- Verified owner decision: record directly on root NVMe, without HDD or a staging/mover path; deployment remains separately authorized. `[pack].dir` is unchanged.
- Verified in [writer](../../internal/rawlog/writer.go) and [poller](../../internal/poller/sink.go): `[record].compress=false` preserves plain bytes; true selects `.jsonl.zst`; `[record].level` defaults to 1 (range 1..22).
- Verified: use one serialized reusable encoder per writer/sink, concurrency 1, 64 KiB window, lower-memory mode; do not retain an encoder per open file.
- Verified: independent finished frames on each buffer write/flush, including the existing ticks, LRU close and shutdown. Reopening appends a fresh frame; envelope bytes do not change.
- Inferred: `.jsonl.zst` makes content explicit while preserving channel/slot identity and legacy `.jsonl` compatibility. Readers must retain whole newline records and report an incomplete final frame.

## Why

- Measured owner evidence: HDD utilization 96–100% during packing; Binance drops 3,654 (`packnew2`) and 1,248 plus OKX 504 (`packnew3`), at `/root/liqwatch-packnew2-20261117T052232Z` and `/root/liqwatch-packnew3-20261117T061944Z` on VT01OK.
- Measured: writer memory/frame comparisons and VT01OK replay ratios/CPU are recorded once in [results](../packing-results.md#live-writer-compression); the encoder pool avoids multiplying compression state by 4,096.
- Measured: VT01OK, kernel `6.1.0-50-amd64`, 2026-11-17 replay gave 3.241:1 at 4,096 open outputs; see linked results for scope, per-venue values and source verification.
- Inferred from linked results: live framing loses ratio versus offline packing but allows the estimated daily compressed bytes to fit the owner-reported root headroom; it does not establish retention capacity during upload outages.

## Consequences

- Verified: no recorder services, HDD changes, mover, deployment, archive deletion gate or remote-layout changes are authorized by this implementation.
- Verified: tests exercise whole-file goldens, LRU append, tick publication, every-byte truncation, poller reopen and unchanged plain bytes. An incomplete frame may expose complete records before its tail error; partial lines must never be emitted.
- Inferred: build-host writer RSS is not recorder cgroup usage; replay also includes source scanners and verification. Recorder live CPU/RAM/drops and real crash behavior remain unmeasured.
- Inferred: root is shared with nginx/docker and has no dedicated mount guard; root loss loses unarchived data. Archive/upload validation and the [0021 gate](0021-gate-archiver-deletion.md) remain unresolved and unchanged.
