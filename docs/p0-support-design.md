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

- Source: `GET /fapi/v1/exchangeInfo` [V that it exists and carries `rateLimits`]. Field names for contract type, status, and quote asset are [U]; confirm before coding. Target set: USDT-margined perpetuals in trading status.
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
- Alert delivery channel: owner's choice (open question). Alerts are deduplicated and repeated at a low rate until cleared.
- Alerts are about data health. Nothing here triggers trades.

## Component 10: Daily Quality Report

Generated shortly after the archiver finishes, stored with the manifest and uploaded, and a short summary is sent through the alert channel.

Contents: per channel message counts and rates; coverage (valid seconds over total) per symbol and tier, excluding marked gaps; list of gaps and unavailable intervals with causes; reconnects; sequence breaks; verification pass rate and mismatch levels; latency P50/P99 per channel; drops; clock offset; archive status; resource use. Flags a day as `good`, `degraded`, or `bad` using thresholds recorded in the report. Days marked `bad` are excluded from training windows unless reviewed.

## Component 11: Deployment and Ops

- **Binaries:** built off the recorder (cross-compiled) and copied. No compilers on the host. Layout `/opt/wicker/releases/<version>/` with a `current` symlink; rollback by switching the link.
- **User and paths:** dedicated unprivileged user `wicker`; `/var/lib/wicker/{raw,state,manifest}`; config in `/etc/wicker/config.toml`; secrets in `/etc/wicker/secrets.env` mode 0600. Config templates will live in this repo under `config/` when code starts.
- **Services (systemd):** `wicker-collector`, `wicker-validator`, `wicker-archiver` (timer), `wicker-checks` (timer). The collector has the highest priority; the validator and archiver are lower (CPU weight, IO weight, memory max). Open-file limit raised (`LimitNOFILE`). `Restart=always` with a backoff.
- **Host:** SSH keys only; firewall allows inbound SSH only; metrics bound to localhost; journald size capped; chrony running.
- **Health:** a restart of any service writes an event. A restart of the collector is a gap and must appear in `gaps`.
- **Changes:** every release records its version in each log record (parser version in outputs, see [p0-core-design.md](p0-core-design.md)).

## Open items from this design

| Item | Where |
|---|---|
| Tier A quote channel: option 1, 2, or 3 | Trial result |
| `exchangeInfo` field names for contract type, status, quote asset | Verify from official docs |
| New-connection rate limit for futures WebSocket | Verify from official docs |
| Alert channel (Telegram, email, other) | Owner |
| Dedicated WebDAV account restricted to `wicker/` | Owner |
| Local retention window and hot-buffer disk size | Trial |
