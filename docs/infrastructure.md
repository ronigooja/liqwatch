# Infrastructure

Hosts and storage. Credentials are never written in this repo. Provider choice is the owner's; governing records: [0005](decisions/0005-japan-region.md), [0021](decisions/0021-gate-archiver-deletion.md).

## Hosts

| Role | Address | Spec | Notes |
|---|---|---|---|
| Recorder | Osaka, hostname VT01OK | Verified: Debian 12, kernel `6.1.0-50`; current resource sizing not rechecked here | Verified: owner report, 2026-11-17; recording/release/storage below. |
| Historical recorder (P0) | 136.0.11.159, Japan, hostname EV02OK | 2 vCPU, ~3.9 GB RAM, 30 GB disk (~25 GB free), Debian 12, 2 GB swap | [Down; outage and open recovery questions](p0-rollout.md#recorder-outage). Reinstalled clean by the owner on 2026-11-01 and confirmed as the recorder. Still to do: timezone is Asia/Shanghai, uses systemd-timesyncd (no chrony), open-file limit 1024, no Go. Docker, python3, curl, zstd present |
| Linux build, test and live recording measurement | 64.118.141.231, Singapore | 1 core, 1.8 GB RAM | Verified: test machine; live measurements run with nothing else running (owner report, 2026-11-15); current collector and poller code is built and merged, not deployed. |
| Linux build and test | 38.47.101.8, Hong Kong | Debian 12, 1 core, 1.9 GB RAM | Verified: shared with permanent services `xboard-node` and `3proxy`; rsync installed 2026-11-15 (owner report). Never use for CPU measurements. |
| REST poller | 154.19.187.117 | Shared host | `liqwatchpoll.service` stopped, disabled and deleted by the owner on 2026-11-12; other services untouched. Its 55 data files are kept only on the Mac outside the repo. Verified in code: current poller is Go ([poller](poller.md)); its deployment is not established. |
| Storage gateway | 45.45.217.235 | 2 vCPU, ~3.9 GB RAM, 19 GB disk (~6 GB free), Debian 12 | Runs OpenList (127.0.0.1:5244, behind nginx), Docker services (including a MySQL port), and other processes; shared host |
| Processing and training | Vultr, on demand; previous `liq-proc` and its `liq-data` volume deleted 2026-11-12 (owner). Results and liquidation Parquet retained on Mac in `$HOME/Agents/liqwatch-results/liqproc/`; other Parquet can be rebuilt from archive | Verified: no processing host running (owner report, 2026-11-12); recorder resources are listed separately | Full conversion to Parquet, labels, LightGBM; Mac runs small samples |

## VT01OK preparation status

| Item | Current status | Source/date |
|---|---|---|
| Archive volume | Verified: 100 GB HDD, `liqwatch-archive-hdd-100g`, ID `a0230815-700a-4e12-a66c-afa5a841ae7d`, $2.50/month | Owner report, 2026-11-17; replaces deleted prior disk |
| Archive mount | Verified: `/dev/vdb`, UUID `eaa9e603-29a1-432d-ae78-4116b90e2659`, mounted `/data/liqwatch-archive`; `[pack].dir` selects this root | Owner report, 2026-11-17; [0029](decisions/0029-local-archive-no-webdav.md) |
| WebDAV reachability | Measured: one 1 MiB write/readback/delete test passed; no spread available, not a sustained-throughput test | Main-session probe from VT01OK to the [archive root](#storage-account-and-re-test-2026-11-02), 2026-11-16 |
| Recording | Verified: release `98f4937`, root `/var/lib/liqwatch/data`, `[record].compress=true`; running since `2026-11-17T09:22:56Z`; archive units not enabled by this change | Owner report, 2026-11-17 |
| Checks | Verified: active minute timer passes `-config /etc/liqwatch/config.toml`; live and pack roots are correct. Avoid `-h` on the old binary: its argument parser falls through to the default config rather than showing help. | Recorder unit/config inspection, 2026-11-22; [required channel design](storage-design.md#required-channel-freshness); [audit](recording-results.md#production-channel-completeness) |
| nginx upload limit | Verified: on VT01OK `client_max_body_size` changed `50m → 512m` after HTTP 413; backup `/etc/nginx/sites-available/icandrinkmore.beer.bak-20261117T102641Z` | Owner report, 2026-11-17; upstream not measured |
| Sustained capacity | Inferred: short-run volume does not establish full-day/busy-day capacity; [measurements and decision](decisions/0029-local-archive-no-webdav.md#why) | Owner evidence, 2026-11-17; [remaining gates](open-questions.md#archive-deployment-gates) |

## Historical recorder network probe

Network from the recorder to Binance (3 curl probes on 2026-11-02, not a benchmark): `fapi.binance.com` TCP connect 20–49 ms, TLS done 40–74 ms; `fstream.binance.com` TCP connect 11–13 ms, TLS done 33–37 ms. Proper latency measurement is part of the P0 trial ([data-recording.md](data-recording.md)).

## Recorder host requirements

- Debian 12 (or equivalent), dedicated to the recorder; no other services.
- Region Japan ([decision 0005](decisions/0005-japan-region.md)).
- Clock sync running (chrony preferred); offset is monitored.
- Open-file limit for the recorder service raised above the default 1024 (systemd `LimitNOFILE`).
- Swap enabled as a safety margin, not as working memory.
- Disk: hot buffer until upload is verified; size for daily packs ([storage design](storage-design.md#local-disk-size)).
- Outbound access to configured venue REST and WebSocket endpoints and storage WebDAV.
- SSH key access for the owner and the working agent; no credentials stored in this repo.

## Storage design

- OpenList exposes WebDAV; daily packs replace hourly archives ([decision 0014](decisions/0014-daily-pack-storage.md)). WebDAV is unsuitable for appends or many small files.
- Local disk on the recorder is the hot buffer. Its free space bounds how long uploads can be delayed; size this from the trial.
- The storage gateway's own free disk is small. Real capacity must come from an added storage server or a mounted cloud drive.
- Sustained throughput of a multi-GB file remains to be tested before relying on it.

## Storage account and re-test (2026-11-02)

The owner created a dedicated WebDAV account `liqwatch`. Its credentials are in `/root/pass/openlist` on the recorder host (line 1 user, line 2 password, file mode 0644, should be 0600). Scope (changed by the owner on 2026-11-03): the account's root is `/dav/`, which contains the mount `ronigooja_onedrive/`; writes go inside that mount (creating a folder directly under the root returns 405).

Tests from the recorder: MKCOL 201; PUT 1 MB 201; PUT 100 MB 201 in 22 s (~4.8 MB/s); GET 100 MB 8.3 s (~12.7 MB/s); checksums match; DELETE 204; the test folder was removed. Wrong password returns 401.

The archiver remote path is therefore `https://icandrinkmore.beer/dav/ronigooja_onedrive/liqwatch/...`.

## Storage probe (2026-11-01, admin account, historical)

WebDAV endpoint `https://icandrinkmore.beer/dav`, mount `ronigooja_onedrive` (the mount root has no quota; upload inside the mount only). Tested from the recorder host. Credentials stay on the storage host, not in this repo.

| Step | Result |
|---|---|
| Initial writes | 403 until the owner enabled write permission for the account |
| After permission fix: MKCOL, PUT 1 MB and 20 MB, GET, DELETE | OK, checksums match |
| PUT 150 MB | 413 (nginx `client_max_body_size 50m` on the `icandrinkmore.beer` site) |
| After nginx change (below): PUT 150 MB | 201, checksum OK; upload took 281 s (~0.56 MB/s), download 7.8 s (~20 MB/s) |
| PUT 500 MB | Did not finish within 15 min; interrupted. Cause unknown |

nginx change (owner approved), file `/etc/nginx/sites-available/icandrinkmore.beer` on the storage host, backups saved beside it as `.bak-<timestamp>`: `client_max_body_size` 50m to 1g; `proxy_send_timeout` and `proxy_read_timeout` 60s to 600s. Other sites were not touched. `proxy_request_buffering off` was tried and removed (see below).

Upload speed investigation:
- With `proxy_request_buffering off` and HTTP/2, uploads ran at ~0.56 MB/s (100 MB: 189 s; 150 MB: 281 s). An earlier conclusion that the OneDrive/upload path was the bottleneck was wrong.
- Owner's 30 MB comparison: buffering off + HTTP/2 ~0.5 MB/s; buffering off + HTTP/1.1 ~5.7 MB/s; buffering on + HTTP/2 ~4.1 MB/s; buffering on + HTTP/1.1 ~3.8 MB/s.
- After removing `proxy_request_buffering off` (buffering on, HTTP/2): 100 MB uploaded in 23 s (~4.5 MB/s), checksum OK.
- During the slow 100 MB run the storage host's free disk stayed at ~6.1 GB; with buffering on, nginx may stage request bodies in its temp directory, so re-check free disk during large uploads.
- Download of 100 MB: ~12 MB/s.

Guidance: keep request buffering on. Verify packs before deleting local files ([storage design](storage-design.md)); re-measure sustained throughput over several days.

## Constraints from the current hosts

- Verified in code: current recording excludes depth and quotes; capacity for seven venues remains unmeasured ([storage design](storage-design.md)).
- The recorder and the storage gateway are single points of failure. Recorded raw files must not be deleted until the upload is verified ([data-recording.md](data-recording.md)).
- The storage gateway runs unrelated services; do not change them without the owner's approval.
