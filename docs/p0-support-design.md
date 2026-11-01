# P0 Supporting Component Design

Components 1, 6, 7, 9, 10, 11 from [components.md](components.md). Core components: [p0-core-design.md](p0-core-design.md). Hosts and storage facts: [infrastructure.md](infrastructure.md).

Markers as in [p0-core-design.md](p0-core-design.md): **[V]** verified in [open-questions.md](open-questions.md); **[U]** unverified.

## Tier A channel choice (open, decided by trial)

- The all-market `!bookTicker` stream updates every 5 s [V, official]. That is too coarse for 1–60 s spikes, so it is not a substitute for per-symbol `@bookTicker` (real time).
- Per-symbol `@bookTicker` for every perpetual may be too heavy for 2 vCPU / 4 GB. Candidates, to be compared in the trial:
  1. `@aggTrade` for all symbols plus `!markPrice@arr@1s` (one stream); `@bookTicker` only for tier B.
  2. Option 1 plus `@bookTicker` for the top-N symbols by recent volume.
  3. `@bookTicker` for all symbols (only if the measured load allows).
- Minimum for tier A: every trade, so any spike can be reconstructed from the trade path. Quote data for tier A is a bonus.

## Component 1: Symbol Manager

- Source: `GET /fapi/v1/exchangeInfo` [V that it exists and carries `rateLimits`]. `symbols[]` fields [V, official]: `symbol`, `pair`, `status` (`TRADING`), `contractType`, `baseAsset`, `quoteAsset`, `marginAsset`, `onboardDate`, `deliveryDate` (ms timestamps), `underlyingType`. Target set: `contractType == PERPETUAL`, `status == TRADING`, `quoteAsset == USDT`. A `TRADIFI_PERPETUAL` contract type appears in the continuous-kline enum, so it is excluded by the `PERPETUAL` match; whether exchangeInfo uses the same value is [U], so log any contractType value not seen before.
- Refresh: at startup, daily at a fixed UTC time, and when the `!contractInfo` stream (on `/market`) reports a change [V that the stream exists].
- Each refresh writes `state/symbols/<UTC timestamp>.json` plus a content hash. If the hash changed, append events to `state/symbol-events.jsonl`: `listed`, `delisted`, `status_change`, `tier_change`.
- Tier B list: from a config file edited by the owner (10–20 symbols, [universe.md](universe.md)). A small number of reserved slots may be filled automatically from a trailing-return ranking; each automatic change records its time and the ranking used, so backtests respect it.
- Tier changes are applied to the live collectors through subscribe/unsubscribe messages, rate-limited to stay far below 10 messages/s per connection [V].
- A delisted symbol keeps its raw files; only the subscription stops.
- Outputs are read by the collector and by research code that needs the universe as of a date.

## Component 6: Archiver

Recorder-side job is packing, upload, and verification only. Parquet conversion is **not** done on the recorder (CPU is scarce); it moves to the research side. This replaces the earlier Parquet-on-recorder idea in [data-recording.md](data-recording.md).

Daily run, after the last hour file of the previous UTC day is closed:

1. List closed hour files from the manifest. Skip files still open.
2. Compute SHA-256 for each file.
3. Split each file into chunks (initial size 64 MB; nginx limit is 1 GB, observed 100 MB uploads worked [measured]). Name `…jsonl.zst.part0000`, and so on, and upload one chunk at a time.
4. Remote layout: `wicker/raw/<venue>/<UTC date>/<group>-<HH>.jsonl.zst.partNNNN` plus `<group>-<HH>.jsonl.zst.meta.json` (size, sha256, chunk list, parser-independent record count).
5. Verify: remote size equals local size per chunk. Download-and-hash verification for the first 7 days, then a sampled fraction (download measured ~12–20 MB/s [measured]).
6. Only after verification mark the file `verified` in the manifest, and delete local data older than the retention window (keep at least 3 days locally even if verified).
7. Throttle: cap upload rate and run at low CPU/IO priority so it never competes with recording. Upload throughput seen: ~4.5 MB/s [measured, single test]; re-measure over several days.
8. On any failure, retry with backoff and leave the file in `uploading`; alert if a file is not `verified` after 24 hours.

Storage host free disk is small (~6 GB) and nginx buffers request bodies [measured], so one chunk at a time. Prefer a dedicated WebDAV account limited to the `wicker/` folder instead of the admin account (see open questions).

## Component 7: Manifest

SQLite file on the recorder (single writer per table, WAL mode), copied to remote storage daily.

| Table | Key columns |
|---|---|
| files | id, path, group, venue, hour_start, hour_end, msg_count, bytes, sha256, closed_at, state (`open`, `closed`, `uploading`, `uploaded`, `verified`, `deleted`), remote_path, chunk_count |
| connections | conn_id, group, endpoint, started_at, ended_at, end_reason |
| gaps | stream, start_ns, end_ns, cause (`reconnect`, `stall`, `drop`, `seq_break`) |
| drops | window_start, channel, dropped_count (queue overflow) |
| daily_reports | date, path, generated_at |

The manifest is the index. The raw files and the validator log remain the source of truth; the manifest can be rebuilt from them.

## Component 9: Monitoring and Alerting

- Each process serves metrics on localhost in Prometheus text format. No Prometheus or Grafana on the recorder in the first stage (memory). A check job (systemd timer, every minute) reads the metrics, evaluates thresholds, and records history locally. Prometheus/Grafana can be added later on a separate host.
- Metrics: per channel message rate; `recv_time − event_time` P50/P99; reconnect count; stalls; queue depth; drops; sequence breaks; snapshot refetches; unavailable-interval seconds; book mismatch rate; archiver backlog; disk usage; CPU, memory; clock offset; REST weight used.
- Initial thresholds (tune from the trial): stall longer than the per-channel timeout; queue above 50%; any drop of depth or snapshot data; disk above 80%; clock offset above 50 ms; mismatch rate above a value set after the trial; unverified file older than 24 h.
- Alerts are emitted through the alert interface below, not through any specific chat or app. Alerts are deduplicated and repeated at a low rate until cleared.
- Alerts are about data health. Nothing here triggers trades.

## Alert interface (part of component 9)

Decision: [0006](decisions/0006-alert-interface.md). Producers and delivery are separate so new consumers (websites, apps, chat bots) are added without touching the recorder.

**Flow:** check job or process -> append event to `state/alerts.jsonl` (append-only, source of truth) -> `wicker-alertd` reads it -> delivers through adapters and serves the pull API.

**Event (schema version 1):**

```
{"schema":1,"id":"<ULID>","ts":"<UTC RFC3339>","severity":"info|warning|critical",
 "state":"firing|resolved","type":"<namespace.name>","source":"<component>",
 "subject":{"host":"...","channel":"...","symbol":"..."},
 "dedup_key":"<stable key>","summary":"<one line>","details":{...}}
```

- `type` uses namespaces, e.g. `data.stall`, `data.seq_break`, `disk.high`, `archive.unverified`, `clock.offset`. Other domains (for example `risk.*` later) reuse the same envelope.
- `firing` and `resolved` share a `dedup_key`; consumers can track open alerts from the stream alone.
- Ids are unique and sortable, so consumers can de-duplicate and resume.

**Push (webhook adapter):**
- Config lists endpoints with filters (minimum severity, type prefixes). Each endpoint has its own secret in `/etc/wicker/secrets.env`.
- HTTP POST with the event as JSON and an HMAC-SHA256 signature header over the body plus a timestamp header, so receivers can verify origin and reject replays.
- Delivery is at least once. Retries use exponential backoff with a cap; a per-endpoint cursor survives restarts. Receivers de-duplicate by `id`.
- A failing endpoint never blocks other endpoints or recording.

**Pull (HTTP, read-only):**
- `GET /v1/alerts?since=<id>&min_severity=&type=` returns events after a cursor.
- `GET /v1/alerts/open` returns currently firing alerts.
- `GET /v1/status` returns a small health summary (per-channel state, data coverage today, archive backlog, disk).
- `GET /v1/health` returns liveness of the alert service itself.
- Bearer-token auth. Listens on localhost by default. Exposing it beyond localhost requires TLS and the token, ideally through a reverse proxy with an IP allowlist. Not exposed without owner approval.
- The API is versioned in the path; fields are only added within a version.

**Other adapters** (chat, email, mobile push) are separate consumers of the same stream or the webhook. None is part of the first stage.

**Self-monitoring:** if `wicker-alertd` stops, a systemd failure hook and a periodic dead-man heartbeat to a configured endpoint (optional) make the silence visible.

**Resource budget:** the service is small and runs at low priority; it must stay within a memory cap set in systemd.

## Component 10: Daily Quality Report

Generated shortly after the archiver finishes, stored with the manifest and uploaded, and a short summary is emitted as an `info` alert (`report.daily`).

Contents: per channel message counts and rates; coverage (valid seconds over total) per symbol and tier, excluding marked gaps; list of gaps and unavailable intervals with causes; reconnects; sequence breaks; verification pass rate and mismatch levels; latency P50/P99 per channel; drops; clock offset; archive status; resource use. Flags a day as `good`, `degraded`, or `bad` using thresholds recorded in the report. Days marked `bad` are excluded from training windows unless reviewed.

## Component 11: Deployment and Ops

- **Binaries:** built off the recorder (cross-compiled) and copied. No compilers on the host. Layout `/opt/wicker/releases/<version>/` with a `current` symlink; rollback by switching the link.
- **User and paths:** dedicated unprivileged user `wicker`; `/var/lib/wicker/{raw,state,manifest}`; config in `/etc/wicker/config.toml`; secrets in `/etc/wicker/secrets.env` mode 0600. Config templates will live in this repo under `config/` when code starts.
- **Services (systemd):** `wicker-collector`, `wicker-validator`, `wicker-archiver` (timer), `wicker-checks` (timer), `wicker-alertd` (alert interface). The collector has the highest priority; the validator and archiver are lower (CPU weight, IO weight, memory max). Open-file limit raised (`LimitNOFILE`). `Restart=always` with a backoff.
- **Host:** SSH keys only; firewall allows inbound SSH only; metrics bound to localhost; journald size capped; chrony running.
- **Health:** a restart of any service writes an event. A restart of the collector is a gap and must appear in `gaps`.
- **Changes:** every release records its version in each log record (parser version in outputs, see [p0-core-design.md](p0-core-design.md)).

## Open items from this design

| Item | Where |
|---|---|
| Tier A quote channel: option 1, 2, or 3 | Trial result |
| Whether exchangeInfo returns `TRADIFI_PERPETUAL` and how `underlyingType` marks non-crypto contracts | Check a live response in the trial |
| First consumer: none yet, generic interface first | Owner |
| Dedicated WebDAV account restricted to `wicker/` (pending) | Owner |
| Local retention window and hot-buffer disk size | Trial |
