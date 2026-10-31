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

nginx change (owner approved), file `/etc/nginx/sites-available/icandrinkmore.beer` on the storage host, backup saved beside it as `.bak-<timestamp>`: `client_max_body_size` 50m to 1g; `proxy_request_buffering off`; `proxy_send_timeout` and `proxy_read_timeout` 60s to 600s. Other sites were not touched.

Findings:
- Upload speed is low and varies (2.8 MB/s on 20 MB, 0.56 MB/s on 150 MB), so the upload path (OneDrive side or the host) is the bottleneck, not nginx. At ~0.5 MB/s a 10 GB daily archive would take over 5 hours; this must be measured over several days before relying on it.
- Download is much faster than upload.
- Large single PUTs are fragile. Upload in chunks (suggested 64–128 MB) with per-chunk checksums and retry, and verify after upload before deleting local files. Chunk size is to be decided with more measurements.
- The storage host has ~6 GB free disk; OpenList is passing uploads to OneDrive, so confirm it does not stage large files locally.

## Constraints from the current hosts

- The recorder may be upgraded (owner approved). First stage records 10–20 depth symbols plus the light tier; size the new spec from the trial ([universe.md](universe.md)).
- Recorder and storage gateway are single points of failure. Recorded raw files must not be deleted until the upload is verified ([data-recording.md](data-recording.md)).
- The storage gateway runs unrelated services; do not change them without the owner's approval.
