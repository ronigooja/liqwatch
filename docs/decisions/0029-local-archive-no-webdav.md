# 0029: Keep verified archives on the local HDD

Purpose: replace the WebDAV destination in [0014](0014-daily-pack-storage.md) and placement in [0024](0024-pack-output-dir.md); retain daily packs and [0026](0026-live-zstd-recording.md).

Date: 2026-11-17

## Decision

- Verified owner choice: drop WebDAV upload for this recorder; root recording stays separate from HDD `packs/` and `manifest/`. Current host paths: [infrastructure](../infrastructure.md#vt01ok-preparation-status).
- Verified in [archiver](../../cmd/liqwatch-archiver/local.go): reuse the binary with `archive.mode="local"`; default mode remains `webdav`. Local mode constructs no network client and reads no credentials or remote root.
- Verified: local verification always rereads pack/hash, every tar member and every source, checks source identity/size/known pack-time mtime, and rejects leftovers, current UTC days and quiet-period files; no extraction.
- Verified: local `delete_sources` defaults true; false verifies/reports only. Delete only the verified raw-day directory, never its pack. WebDAV verification/deletion behavior is unchanged.
- Verified: states `closed → local_verified → local_deleted` isolate local proof from remote states. Reverify `local_verified` on every run before deletion; stored member mtime is optional for old manifests.
- Verified: [0021](0021-gate-archiver-deletion.md) is a deployment-preparation policy leaving archiver disabled until upload/safety checks; this decision supersedes its upload gate for local mode, not a claim of owner-mandated manual deletion.

## Why

| Question | Method / measured numbers | Date / source |
|---|---|---|
| Is upload viable? | Measured: 100 MB PUT 26.2 s (~4.0 MB/s); 1 MB 6.4–7.8 s (0.13–0.16 MB/s); 10 MB 7.6–11.5 s; MKCOL 5.5 s, HEAD 3.8 s. Request counts unavailable; spread only where given. | 2026-11-17; owner-supplied VT01OK probe in task conversation; raw probe commands/logs unavailable here. |
| What local capacity is needed? | Measured: 693 MB compressed in one 78-minute recording interval; no repeat spread. Inferred: ~13 GB/day; 100 GB holds ~7–8 nominal days before overhead/headroom. | Same date/source; recording release and host status in linked infrastructure table. |

- Inferred: fixed several-second request overhead, ~5 requests/pack × ~2,700 packs/day implies ~19–21 h sequential work; not viable. nginx runs on the host itself; upstream latency was not measured. Infrastructure-only nginx change is recorded once in the linked host table.

## Consequences

- Verified implementation: reads are sequential per day, use 32 KiB pacing buffers, bounded zstd decoders and best-effort Linux `FADV_DONTNEED`; `max_kbps` limits local physical reads, while existing WebDAV upload pacing is unchanged.
- Verified: blocked-by-rule days report exit 0; hash/source mismatch and I/O errors report nonzero. Logs give verified/would-delete/blocked/deleted counts and deletion member/byte counts.
- Inferred: dropping upload leaves no offsite copy; HDD loss loses packs and manifest, root loss loses unarchived raw. The owner chose local storage; no separate acceptance of this failure risk is asserted.
- Verified: old-pack retention/cleanup, durable writer exclusion and crash recovery between raw removal and manifest update are not built; final check-to-delete race remains. WebDAV mode remains available but unvalidated at scale.
- Verified repository preparation: local units contain no secrets file, use idle IO/Nice and packer-sized memory limit; units remain disabled, with no deployment/enablement in this change. [Remaining gates](../open-questions.md#archive-deployment-gates).
