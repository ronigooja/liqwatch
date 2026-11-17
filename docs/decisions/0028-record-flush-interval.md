# 0028: Configure the live recording flush interval

Purpose: govern recorder flush timing; partially supersedes the existing-tick policy in [0026](0026-live-zstd-recording.md), not its frame or encoder policy.

Date: 2026-11-17

## Decision

- Verified owner decision: when `[record].compress=true`, an unset `flush` selects 30 s for both recorders.
- Verified in [config](../../internal/config/config.go): `flush` is a Go duration string, positive and at most 5m; an explicit value applies in both modes.
- Verified: when compression is off and `flush` is unset, collector retains 5 s and poller retains 1 s.
- Verified: no buffer-size, queue-capacity, request-scheduling, deployment or durability-policy change is authorized.

## Why

- Measured owner evidence: the [flush sweep](../packing-results.md#flush-interval-sweep) favors the selected default; repeat-run spread is unavailable.
- Inferred: the upper bound limits nominal buffered-data exposure; extending beyond the bound offers little additional compression in this sample, not a proven universal optimum.

## Consequences

- Verified: collector drop-marker publication and past-day cleanup share its flush tick; disk checks remain separately timed. Compressed poller releases descriptors at each tick.
- Verified: full buffers, LRU close and shutdown still publish frames before the tick; see [writer behavior](../storage-design.md#writing).
- Inferred: nominal crash-loss exposure is one flush interval of buffered data, not measured durable storage; unsynced kernel writes and queued records are outside that bound.
- Verified: replay uses the same buffer size as production but record-time rather than wall-time ticks; this is not a live-recorder measurement.
