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

## Constraints from the current hosts

- The recorder is small for tier B depth at 100 ms. Load and size it in the trial before committing to tier counts ([universe.md](universe.md)).
- Recorder and storage gateway are single points of failure. Recorded raw files must not be deleted until the upload is verified ([data-recording.md](data-recording.md)).
- The storage gateway runs unrelated services; do not change them without the owner's approval.
