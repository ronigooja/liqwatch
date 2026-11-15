# 0019 Live REST recording scope

Verified: owner decision. Date: 2026-11-15. Governs live REST recording and refines [0015](0015-recording-scope-and-granularity.md). Implementation: [poller](../poller.md#live-recording-boundary).

## Decision

- Verified: supersede only 0015's initial two-hour lookback and its before-lookback consequence; no initial lookback or today-to-start bootstrap is allowed. Its overlap recovery and other decisions remain unchanged.
- Verified: [0014](0014-daily-pack-storage.md) daily packs concern storage, not daily fetching; [0017](0017-poller-field-policies.md) field policies remain unchanged. Neither contains an initial-lookback clause to supersede.
- Verified: persist one immutable venue `recording_start_ms` before requests. Fresh histories start at that inclusive floor; later walks use `max(floor, cursor - overlap)`, subject to existing retention clamps, in every paging mode.
- Verified: request only the live recording interval, not pre-start history. If an API ignores bounds and returns older rows, filter them before storage; do not page further to acquire pre-start rows.
- Verified: restart and UTC midnight do not reset the floor. Ongoing recovery may query yesterday within the recorded interval; a moving today-only cutoff must not truncate continuity.
- Verified: an empty newly opened range is not upstream absence and must not trigger the no-data cooldown. Preserve explicit unsupported-series handling and existing legacy cursor recovery.
- Verified: `initial_lookback` is deprecated compatibility input and cannot widen fresh recording scope.
- Verified: spread requests over time as separate work; not built, deferred. This decision changes neither configured periods nor rate limits and does not claim coverage is complete.

## Why

- Verified: the owner requires ongoing self-recording, not pre-start acquisition or a two-hour startup bootstrap.
- Inferred: an immutable recording boundary preserves continuity across midnight without expanding the intended scope.

## Consequences

- Verified in implementation `332631b`: migrated cursor-bearing jobs retain `legacy_history` compatibility; cursorless and newly discovered jobs use the persisted venue floor. Existing state is not erased.
- Inferred: data before a fresh recording boundary is unavailable from this recording; cadence, upstream retention and finalization can still leave gaps. Uncertainty: [open questions](../open-questions.md#poller-field-policy-questions).
- Verified: historical [trial results](../recording-results.md#rest-production-scheduler-trial) retain their original lookback parameters; they are not measurements of this scope.
