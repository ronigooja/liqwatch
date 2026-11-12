# Separate REST poller (`wickerpoll`)

Applies to `research/wicker_research/poll/`; collector behavior is unchanged.
The repository has no deployment assets; the service was removed ([infrastructure](infrastructure.md#hosts)).
Purpose and minimum input frequencies: [liquidation model](liquidation-model.md#separate-poller).
Endpoint shapes, source caveats and verified exchange facts: [open questions](open-questions.md#verified).

## Run and scheduling

Run `python -m wicker_research.poll --raw <collector-raw-root>` from `research/`.
The CLI identifies itself as `wickerpoll`; no installed console script is required.
One process holds `poll.lock`; do not use collector handoff slots for it
([handoff scope](decisions/0009-zero-gap-handoff.md)). HTTP session and clock are injectable.
The default checkpoint is `<raw>/poll-state.json`; `--state` overrides it.
Use a supervisor to alert on nonzero exit and restart; deployment is not included.

Daily tasks run at UTC 00:05; startup immediately fills missing current-day snapshots.
`exchangeInfo` selects TRADING USDT PERPETUAL symbols for both per-symbol tasks.
Use `--syms BTCUSDT,ETHUSDT` to restrict OI and constituents to eligible symbols; default: all.
For `--syms`, empty items, lowercase and malformed symbols are rejected at startup; full brackets/exchangeInfo storage is unchanged.
Missing requested symbols are skipped, saved in state as `missing_symbols`, and warned once per UTC day.
Constituents use independent per-symbol daily completion checkpoints, so a slow or
failed sweep rolls forward without discarding unfinished symbols or blocking OI.
OI is due on five-minute UTC boundaries, immediately on startup; each timestamp
means response receipt, not simultaneous sampling. Overdue jobs run once, not in bursts.
If coverage exceeds rate/budget capacity, freshness degrades; no cadence is guaranteed.

## Limits and failures

The shared local budget defaults to 240 estimated weight/minute (10% of the
recorded limit linked above). `--weight-cap` changes it. Observed used-weight
headers constrain the entire poller until the next UTC minute, not just OI.
This conservative ceiling can stall polling when the collector alone exceeds it;
it is not a reservation or coordination protocol with the collector.
OI weight is unverified: budget assumption 1 (`--oi-weight`), at most 2 requests/s.
Constituents weight is unverified: assumption 1 (`--constituents-weight`), at most
1 request/s. Both rate ceilings can only be lowered through the CLI.
Other endpoint weight assumptions are also configurable; none verifies an exchange weight.
Requests time out; failures retry indefinitely with capped exponential backoff and
logged deferrals. HTTP 429/418 blocks that endpoint for Retry-After (seconds or HTTP
date; default 60 s). Pending work does not block unrelated endpoints.
Bracket validation checks `data.brackets` and required envelope/entry/tier fields (`updateTime` may be null)
before writing valid rows. Schema failure preserves `brackets_error` and exits 1;
restart alone will not repair a changed source. No partial table is accepted.
The source is the website's internal, undocumented endpoint; its change risk is
recorded in [open questions](open-questions.md#verified), not a stable API guarantee.

## Persistence and downstream

Envelope and UTC hourly naming follow [the pipeline](p1-data-pipeline.md#raw-record-format):
group `poll`, slot `p`, restart segments `.rN`; `p` retains the response JSON
(brackets: one symbol entry per record). Extra envelope `meta` stores status,
`used_weight_1m` (null if absent), and brackets' exact response-byte SHA-256.
Each record is a complete fsynced zstd frame in `.part`; close/hour rotation renames
it and writes a local `.meta.json` (size, SHA-256, single local chunk filename).
Recovery finalizes only poll-owned parts; a torn final frame is handled by replay.
Checkpoints follow durable records; crashes can duplicate snapshots, never justify deduping
different fetches. Recovered tails may be incomplete and require coverage review.
The existing collector sweep registers closed files in a shared raw root; the archiver
creates its own remote chunk metadata. Standalone manifest registration is not implemented.
Replay preserves poll payload/meta without stream deduplication; conversion adds
`poll_<channel>` datasets with `rw`, `payload_json`, `meta_json`, date/symbol partitions.
These additive schemas do not replace any [frozen stream dataset](p1-data-pipeline.md#columnar-layout-frozen-2026-11-07).
Operational host/storage constraints remain [infrastructure](infrastructure.md).
