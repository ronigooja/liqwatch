# 0025: Release packer page cache

Purpose: govern Linux packer cache lifetime; extends [0023](0023-packer-streaming.md), without superseding streaming or [0024](0024-pack-output-dir.md).

Date: 2026-11-17

## Decision

- When packing quiet past-day files on Linux, release each source's cache after its successful full read and before close using best-effort `FADV_DONTNEED`.
- Release the completed temporary pack's cache after its existing sync and final hash read, before close/rename. Keep atomic publication unchanged; non-Linux is a no-op.
- Do not sync sources, add noisy error counters, or tune sequential readahead without evidence of benefit.

## Why

- Measured: [recorder baseline](../packing-results.md#recorder-baseline) records anonymous memory versus cache pressure, throughput loss and the unconstrained control, with evidence location.
- Inferred: each source is read once; retaining consumed clean pages spends cgroup headroom without reuse. Output cache is smaller, but its existing sync makes release cheap without another writeback barrier.
- Measured: [build-machine comparison](../packing-results.md#build-machine-cache-release) supports source cache release; recorder effect remains unmeasured.

## Consequences

- Verified in [code](../../internal/pack/pack.go): source errors remain unchanged; cache advice cannot fail a pack or alter bytes, hashes or manifests.
- Inferred: advisory release is not a hard cache bound; dirty pages and the currently read member can remain cached. Quiet past-day sources are expected clean; current-day recorder files are excluded.
- Inferred: metadata slab growth from the baseline's many files is not fixed; neither are Binance drops during packing. IO versus CPU causality remains unknown and is a separate issue.
