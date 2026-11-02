# 0008 Hourly archive and short local retention

Date: 2026-11-03. Supersedes the daily archive run and the "keep at least 3 days locally" rule in the earlier [p0-support-design.md](../p0-support-design.md) Archiver text.

## Decision

1. The archiver runs hourly and uploads each closed hour file.
2. A local file is deleted 6 hours after it is `verified`. An unverified file is never deleted.

## Why

- Compressed growth is ~18 GB/day ([trial-plan.md](../trial-plan.md)); free disk is ~21 GB. A daily run would hold today's open files plus yesterday's unuploaded day, ~36 GB peak. A 3-day local window would be ~54 GB.
- Hourly files are ~0.75 GB. Measured upload speed is ~4.5 MB/s ([infrastructure.md](../infrastructure.md)), about 16 GB/hour of capacity, so the upload keeps up.
- Expected local use is ~5–8 GB, leaving room for upload failures.

## Consequences

- Raw data older than 6 hours lives only on remote storage, so verification before deletion matters more. First 7 days use download-and-hash verification.
- A storage outage longer than ~15 hours at full rate would approach the 80% disk alert; the unverified-file alert is therefore set at 3 h (was 24 h).
