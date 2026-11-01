# Infrastructure

Hosts and storage. Credentials are never written in this repo. Provider choice is the owner's; region decision: [decision 0005](decisions/0005-japan-region.md).

## Hosts

| Role | Address | Spec (probed 2026-11-01) | Notes |
|---|---|---|---|
| Recorder (P0) | 136.0.11.159, Japan | 2 vCPU, ~3.9 GB RAM, 30 GB disk (~21 GB free), Debian 12, NTP synchronized | Already ~1.5 GB RAM in use by unrelated processes; not yet identified |
| Storage gateway | 45.45.217.235 | 2 vCPU, ~3.9 GB RAM, 19 GB disk (~6 GB free), Debian 12 | Runs OpenList (127.0.0.1:5244, behind nginx), Docker services (including a MySQL port), and other processes; shared host |
| Training (later) | Vultr, on demand | TBD | Large-scale training only |

Network from the recorder host to Binance (single curl probe, not a benchmark): `fapi.binance.com` TCP connect ~48 ms, TLS done ~65 ms; `fstream.binance.com` connect ~13 ms. Proper latency measurement is part of the P0 trial ([data-recording.md](data-recording.md)).

## Storage design

- OpenList exposes WebDAV; the recorder uploads finished daily archives only. WebDAV is unsuitable for appends or many small files.
- Local disk on the recorder is the hot buffer. Its free space bounds how long uploads can be delayed; size this from the trial.
- The storage gateway's own free disk is small. Real capacity must come from an added storage server or a mounted cloud drive.
- Whether a cloud drive can be served through OpenList WebDAV is untested. Required test before relying on it: upload, download, checksum round-trip, and sustained throughput of a multi-GB file.

## Storage probe (2026-11-01)

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

- The recorder may be upgraded (owner approved). First stage records 10–20 depth symbols plus the light tier; size the new spec from the trial ([universe.md](universe.md)).
- Recorder and storage gateway are single points of failure. Recorded raw files must not be deleted until the upload is verified ([data-recording.md](data-recording.md)).
- The storage gateway runs unrelated services; do not change them without the owner's approval.
