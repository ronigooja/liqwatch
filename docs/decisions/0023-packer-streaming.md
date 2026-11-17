# 0023: Stream pack members with bounded compression memory

Purpose: bound packer heap independently of raw member size; implementation detail of [0014](0014-daily-pack-storage.md), not a grouping or retention change.

Date: 2026-11-17.

## Decision

- Verified in [packer](../../internal/pack/pack.go): stream members and hashes; reject byte-count, size, modification-time or quiet-period changes before publishing.
- Verified: reuse zstd encoders with concurrency 1, a 1 MiB window and lower-memory mode; preserve level 3 by default and both compression-order formats.
- Verified: for `zstd_then_tar`, spool one compressed member on disk before writing its sized tar header; never buffer a whole member or pack in RAM.

## Why

| Question | Method / measured result | Date / reproducible source |
|---|---|---|
| Did the old packer hit its ceiling? | Measured: one 5-minute trial; cgroup peak 512.004 MiB, 2,023 max events, no OOM kill; spread unavailable. | 2026-11-17; VT01OK, kernel 6.1.0-50, 2 vCPU / 2 GB; evidence below. |
| What load was packed? | Measured: same run, 2.69 GB / 14,013 files; 1,096 packs in about 271 s, 4.22 MB/s; spread unavailable. | Same date, host and evidence. |

Measured source: user-supplied `root@VT01OK:/root/liqwatch-pack5b-20261117T031936Z/`; no new recorder access.

- Verified in baseline `6889839`: whole-file `os.ReadFile`, default streaming encoder concurrency, and per-member encoders plus compressed byte slices in `zstd_then_tar` caused avoidable allocations.
- Verified: [Writing](../storage-design.md#writing) has daily rotation without a size cap. The [six-hour sample](0022-ws-gap-records.md#why) is not a full-day maximum; bounded memory must not depend on extrapolated member sizes.

## Consequences

- Verified: [tests](../../internal/pack/stream_test.go) cover old-pack readability, new-pack reading, changed-member rejection and a two-member 256 MiB/member heap bound; whole-pack byte comparisons are in [pack tests](../../internal/pack/pack_test.go).
- Inferred: a smaller window and single-worker compression may trade compression ratio/throughput for memory; benchmark data is synthetic and not recorder cgroup usage.
- Verified: `zstd_then_tar` needs temporary disk space for its largest compressed member; cleanup occurs on success and error. Packing remains sequential within each call; publication stays atomic.
- Verified: no changes to grouping, manifests, quiet-period rules, archiver/deletion logic or `MemoryMax`; no deployment or enablement.
