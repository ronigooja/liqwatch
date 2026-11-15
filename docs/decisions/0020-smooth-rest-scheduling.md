# 0020 Smooth REST scheduling

Verified: owner GO decision. Date: 2026-11-16. Governs REST scheduling; supersedes only the spreading deferral in [0019](0019-live-rest-recording-scope.md). Operation: [poller](../poller.md#scheduling-and-pacing).

## Decision

- Verified: preserve configured periods, weights, endpoint rates and venue budgets; spread eligibility and pace requests rather than reduce recording scope.
- Verified: assign canonical job ranks to equally spaced phases per venue/cadence. Preserve phases for unchanged configuration/universe; recompute when membership changes.
- Verified: make first snapshots immediately eligible, subject to pacing. Histories may wait for their initial phase while retaining the persisted recording floor and existing recovery behavior.
- Verified: after success, use the next phase within one period as an eligibility deadline, not a successful-fetch guarantee. Coalesce missed cycles rather than replay scheduling debt.
- Verified: pace request starts by venue weight budget without startup or idle credit; pagination shares pacing and endpoint limits.
- Verified: prioritize earliest eligible deadlines; retries use their own backoff deadlines with existing endpoint `Retry-After` handling.
- Verified: report `schedule_over_capacity` for an impossible minimum configured load; do not silently change cadence or claim that a warning-free configuration can sustain all traffic.

## Why

- Verified: the owner approved implementing the separate request-spreading work identified in [0019](0019-live-rest-recording-scope.md#decision).
- Inferred: phase spreading reduces synchronized due work; pacing limits bursts when startup, missed cycles or pagination concentrate requests.

## Consequences

- Verified in [scheduler.go](../../internal/poller/scheduler.go): pacing state is in memory; a restart permits one immediate request before charging its interval. Phase derivation needs no new persisted state.
- Verified: scheduling, `-once`, capacity and source-level test coverage are defined in [poller operation](../poller.md#scheduling-and-pacing).
- Inferred: serial slow HTTP, pagination, overload and upstream failures can delay work beyond eligibility. This decision proves neither upstream retention coverage nor loss-free recording.
