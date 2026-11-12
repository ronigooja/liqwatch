# Infrastructure

Hosts and storage. Credentials are never written in this repo. Provider choice is the owner's; region decision: [decision 0005](decisions/0005-japan-region.md).

## Hosts

| Role | Address | Spec (probed 2026-11-01) | Notes |
|---|---|---|---|
| Recorder (P0) | 136.0.11.159, Japan, hostname EV02OK | 2 vCPU, ~3.9 GB RAM, 30 GB disk (~25 GB free), Debian 12, 2 GB swap | Reinstalled clean by the owner on 2026-11-01 and confirmed as the recorder. Still to do: timezone is Asia/Shanghai, uses systemd-timesyncd (no chrony), open-file limit 1024, no Go. Docker, python3, curl, zstd present |
| Storage gateway | 45.45.217.235 | 2 vCPU, ~3.9 GB RAM, 19 GB disk (~6 GB free), Debian 12 | Runs OpenList (127.0.0.1:5244, behind nginx), Docker services (including a MySQL port), and other processes; shared host |
| Processing and training | Vultr, on demand. `liq-proc` (LA, 4 vCPU, 16 GB) was used 2026-11-09 to 2026-11-12 for Parquet conversion and the liquidation-model runs, and deleted on 2026-11-12 (owner). Nothing is running there. Results and the liquidation Parquet (about 15 MB) are kept on the Mac in `$HOME/Agents/wicker-results/liqproc/` (not in the repo); trades, mark and depth Parquet and the raw files were not kept and can be rebuilt from the archive (raw hourly files for 2026-11-04..10, now on the unreachable recorder and WebDAV). The block storage `liq-data` (100 GB, about $2.5 per month) was not touched: the API key is restricted to another IP, so its state is unknown | Full conversion to Parquet, labels, LightGBM. Until a new instance is opened the Mac runs small samples |

Network from the recorder to Binance (3 curl probes on 2026-11-02, not a benchmark): `fapi.binance.com` TCP connect 20–49 ms, TLS done 40–74 ms; `fstream.binance.com` TCP connect 11–13 ms, TLS done 33–37 ms. Proper latency measurement is part of the P0 trial ([data-recording.md](data-recording.md)).

## Recorder host requirements

- Debian 12 (or equivalent), dedicated to the recorder; no other services.
- Region Japan ([decision 0005](decisions/0005-japan-region.md)).
- Clock sync running (chrony preferred); offset is monitored.
- Open-file limit for the recorder service raised above the default 1024 (systemd `LimitNOFILE`).
- Swap enabled as a safety margin, not as working memory.
- Disk: hot buffer for raw logs until upload is verified. Size is fixed from the trial; with 2 vCPU and 4 GB RAM, the first stage is the light tier plus 10–20 depth symbols and may need reduction after the trial.
- Outbound access to Binance REST and WebSocket, and to the storage WebDAV endpoint.
- SSH key access for the owner and the working agent; no credentials stored in this repo.

## Storage design

- OpenList exposes WebDAV; the recorder uploads finished daily archives only. WebDAV is unsuitable for appends or many small files.
- Local disk on the recorder is the hot buffer. Its free space bounds how long uploads can be delayed; size this from the trial.
- The storage gateway's own free disk is small. Real capacity must come from an added storage server or a mounted cloud drive.
- Whether a cloud drive can be served through OpenList WebDAV is untested. Required test before relying on it: upload, download, checksum round-trip, and sustained throughput of a multi-GB file.

## Storage account and re-test (2026-11-02)

The owner created a dedicated WebDAV account `wicker`. Its credentials are in `/root/pass/openlist` on the recorder host (line 1 user, line 2 password, file mode 0644, should be 0600). Scope (changed by the owner on 2026-11-03): the account's root is `/dav/`, which contains the mount `ronigooja_onedrive/`; writes go inside that mount (creating a folder directly under the root returns 405).

Tests from the recorder: MKCOL 201; PUT 1 MB 201; PUT 100 MB 201 in 22 s (~4.8 MB/s); GET 100 MB 8.3 s (~12.7 MB/s); checksums match; DELETE 204; the test folder was removed. Wrong password returns 401.

The archiver remote path is therefore `https://icandrinkmore.beer/dav/ronigooja_onedrive/wicker/...`.

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

Guidance: keep request buffering on. Upload large archives in chunks (suggested 64–128 MB) with per-chunk checksums and retry; confirm after upload before deleting local files. Re-measure sustained throughput over several days.

## Constraints from the current hosts

- First stage records 10–20 depth symbols plus the light tier on 2 vCPU / 4 GB; upgrade only if the trial shows it is needed ([universe.md](universe.md)).
- The recorder and the storage gateway are single points of failure. Recorded raw files must not be deleted until the upload is verified ([data-recording.md](data-recording.md)).
- The storage gateway runs unrelated services; do not change them without the owner's approval.
