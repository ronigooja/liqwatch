# 0030: Recoverable raw deletion and capacity retention

Purpose: partially supersede deletion, retention and unit consequences of [0029](0029-local-archive-no-webdav.md); retain its local-only archive choice.

Date: 2026-11-17

## Decision

- Verified in [local archiver](../../cmd/liqwatch-archiver/local.go): fully verify, persist `local_deleting` and observed size/mtime evidence, exclusively rename the raw day to sibling `<day>.deleting`, recheck all members without hashing, then remove it.
- Verified: changed content blocks removal; restore only with a no-replace rename. A recreated original path is never overwritten; the renamed directory remains blocked and is reported loudly.
- Verified: reconcile deletion intent before each normal pass; missing both paths completes deletion, original-only retries full verification, renamed paths recheck before removal. A durable post-recheck flag permits missing members after interrupted removal.
- Verified in [retention](../../cmd/liqwatch-archiver/retention.go): local capacity retention defaults enabled, starts at 90%, stops at 80%, processes oldest UTC days first, at most three days/run; thresholds and cap are configurable.
- Verified: prune only `local_deleted` packs without either raw path; retain newest on-disk day and unknown files. Keep manifest/member history as `pruned`. Remaining usage >= high warns and exits nonzero.
- Verified in [units](../../deploy/): packer/local archive/checks require the HDD mount and root raw path; only HDD has a mount-point condition. Recorder/poller require the root path, not a mount point; legacy WebDAV units are unchanged.

## Why

- Measured basis: the single 78-minute compressed recording sample and inferred daily volume are recorded once in [0029](0029-local-archive-no-webdav.md#why); repeats and full-day spread are unavailable.
- Inferred: its ~13 GB/day on a nominal 100 GB HDD reaches 90% in about 6.9 days before filesystem/manifest overhead; capacity retention bounds growth, not a fixed day count.
- Verified: packer timer is 00:30 UTC, beyond the 10-minute quiet period; local archive is 02:00 UTC, each persistent with <=60 s jitter. This is scheduling headroom, not a measured pack-duration guarantee or strict dependency.

## Consequences

- Verified chosen retention: pruning deletes the only copy; no offsite copy exists. HDD loss loses packs/manifest; root loss loses unarchived raw. `retention=false` disables capacity pruning, not raw cleanup.
- Verified remaining race: a writer holding an already-open descriptor into the renamed directory can append after the metadata recheck; those bytes can be unlinked without entering a pack. Rename protects subsequent opens at the original path, not old descriptors.
- Verified in [collector writer](../../internal/rawlog/writer.go): past-day handles close on the next serviced flush tick; [compressed poller](../../internal/poller/sink.go) closes all handles each tick. Default compressed tick is 30 s, configurable up to 5m.
- Inferred: tick scheduling, blocked IO and old queued timestamps prevent a hard wall-clock bound; plain poller descriptors persist until shutdown. Durable cross-process writer exclusion is not built.
- Verified: crash tests cover intent, rename, partial unlink and removal before state update; no recorder access or deployment is performed. Power-loss durability, sustained retention IO, full-day/busy-day capacity and concurrent writer loss frequency are not measured.
