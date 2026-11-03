# P0 Rollout and Observation Period

How the production collector goes live and when it counts as stable. Component designs: [p0-core-design.md](p0-core-design.md), [p0-support-design.md](p0-support-design.md). Scope decision: [0007](decisions/0007-trial-outcome.md). Measured load: [trial-plan.md](trial-plan.md). Hosts: [infrastructure.md](infrastructure.md).

## Stage 1 scope (first release)

| In | Out (later stages) |
|---|---|
| Stream Collector, Raw Log Writer, Snapshot Scheduler (components 2, 3, 4) | Book Validator (5), Replay Engine (8) |
| Minimal Symbol Manager: daily `exchangeInfo`, symbol list file, change log (1) | Automatic tier B reserved slots |
| Archiver with upload, verification, cleanup (6) and Manifest (7) | Sampled-only verification, daily quality report (10) |
| `wicker-checks` timer writing events to `state/alerts.jsonl`; `wicker-alertd` after the first week | Webhook adapters beyond the generic one |

Why the archiver is in stage 1: expected raw growth is ~18 GB/day compressed ([trial-plan.md](trial-plan.md)) against ~21 GB free, so unattended recording without upload and cleanup fills the disk in about a day. It runs hourly so local use stays near 5–8 GB ([decision 0008](decisions/0008-hourly-archive-and-retention.md)).

Subscriptions: `@aggTrade` all 522 symbols, `!markPrice@arr@1s`, `!forceOrder@arr`, `@bookTicker` top 50 by volume, `@depth@100ms` for the tier B list (10 trial symbols to start; final list is an owner input in [open-questions.md](open-questions.md)). Top 50 is fixed at startup and refreshed daily with the symbol list; every change is logged.

## Release and service setup

- Build on a GitHub-hosted Linux runner, verify the artifact sha256, copy to `/opt/wicker/releases/<version>/`, switch the `current` symlink. Rollback is switching the link back.
- Each service has its own systemd unit with `LimitNOFILE`, `Restart=always` with backoff, and CPU, IO and memory limits per [p0-support-design.md](p0-support-design.md) (Component 11). The collector has the highest priority.
- Config in `/etc/wicker/config.toml`; WebDAV credentials in `/etc/wicker/secrets.env` mode 0600, copied from `/root/pass/openlist` on the host, never into the repo.
- Start order: collector, then archiver timer, then checks timer. Do not run `trialrec` and the collector at the same time on the host.

## Deployment log

| Release | Contents | Live from (UTC) |
|---|---|---|
| `p0-1` | First production build | 2026-11-02 22:56 |
| `p0-2` | `checks` fix for the unverified-age threshold; collector binary identical to `p0-1` (hard link) | 2026-11-03 00:10 |

Observation period restarted from scratch 2026-11-03 00:32:08 UTC: local data and manifest cleared, WebDAV cleared by the owner, `chunk_mb` raised from 64 to 128.

## Observation period (first 48 h)

The period replaces the dropped R5 soak. It counts from the first clean start; any collector crash restarts the count only if it caused unexplained data loss.

Must be observed at least once:

1. A forced 24 h connection close: the shard reconnects, a `gap` record is written, subscriptions are restored.
2. A full hourly archive cycle across a UTC day boundary: pack, upload, verify, mark `verified`, delete local files 6 h after verification ([decision 0008](decisions/0008-hourly-archive-and-retention.md)).
3. A deliberate collector restart: gap appears in the manifest and recording resumes with snapshot resync for depth symbols.

Pass criteria (proposals, set before the run and not tuned afterwards):

| Item | Criterion |
|---|---|
| Collector CPU | Mean under 50% of one core; no sustained second above 150% |
| Memory | Collector RSS growth under 10% between hour 6 and hour 48; host available above 1.5 GB |
| File descriptors | Flat after hour 1 |
| Drops | Zero for depth and snapshot data; any other drop is explained |
| Reconnect recovery | Each within 10 s with a matching `gap` record |
| Disk | Used stays under 80% including a full day awaiting verification |
| Archive | Every closed file `verified` within 3 h; upload does not starve recording |
| Clock | Offset under 50 ms throughout |

A failed row blocks the move to stage 2 (validator) until fixed and re-observed for 24 h.

## Open items

| Item | Note |
|---|---|
| Remote capacity | OneDrive quota behind OpenList for ~18 GB/day is unchecked ([open-questions.md](open-questions.md)); ~540 GB per month. |
| Depth symbol list | Final tier B symbols ([universe.md](universe.md)). |
| Depth 250 ms stream name | Not needed while 100 ms is used. |
