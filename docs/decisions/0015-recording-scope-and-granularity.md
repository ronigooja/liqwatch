# 0015 Recording scope and granularity

Verified: owner-approved recording and polling plan, 2026-11-14; supersedes the recording-source preference in [0011 item 4](0011-no-alert-narrower-recording.md), not its other decisions.

## Decision

- Verified: retain fields belonging to recorded data items, retaining and counting unknown fields; preserve periodic complete-frame samples. Implementation: [adapters](../venue-adapters.md#recorded-content).
- Verified: retain mark and index at one record per second per symbol and funding at one per minute; do not thin other data. Implementation and channel exceptions: [throttling](../venue-adapters.md#throttling).
- Verified: use the owner-approved [polling plan](../poller.md): daily except Bitget history hourly; Binance alone polls open-interest and funding history; Gate polls minute contract statistics.
- Verified: initial two-hour lookback superseded by [0019](0019-live-rest-recording-scope.md); ongoing cursor recovery still uses overlap. Other decisions here remain unchanged.

## Why

- Inferred: filtering and bounded granularity reduce recorder load while retaining selected market information.
- Inferred: scheduled history polling provides continuity without initial historical backfill.

## Consequences

- Verified: the recorded payload is not generally the original complete frame; [samples and limitations](../venue-adapters.md#recorded-content) remain important for interpretation.
- Verified: the before-lookback consequence is superseded by [0019](0019-live-rest-recording-scope.md#consequences).
