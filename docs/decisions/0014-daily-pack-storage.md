# 0014 Raw files per venue, symbol and channel; daily packs; no chunking

Date: 2026-11-13. Supersedes [0008](0008-hourly-archive-and-retention.md) (hourly archive, 128 MB chunks, local deletion 6 h after verification). Design: [storage-design.md](../storage-design.md).

Verified: deployment enablement is qualified by [0021](0021-gate-archiver-deletion.md); archiver remains disabled pending its safety gate. Storage format and retention policy are not superseded.

Verified: [0027](0027-pack-of-compressed-members.md) supersedes the container compression choice for days containing live-compressed members; the upload/retention unit remains daily.

## Decision

1. Seven centralized venues are recorded for the 522 Binance USDT perpetual coins; `TRADIFI_PERPETUAL` contracts are not (owner, 2026-11-12 and 2026-11-13). DEXes are excluded by [0013](0013-no-dex-recording.md).
2. Raw files are split by venue, symbol and channel, one file per UTC day. No hourly rotation, no size limit. The collector appends uncompressed text; no compressor runs in it.
3. After the UTC day boundary, each symbol's channel files are packed and compressed into one pack per venue, symbol and day. One PUT per pack to WebDAV; no chunking.
4. A local file is deleted only after its pack is verified; no buffer days.
5. The archive keeps every pack without limit and is the only authoritative copy. The research layer is temporary and may be deleted at any time.
6. Chart state (aggregated per symbol, time bucket and price cell) is a cache that can be rebuilt from packs; it is not stored as the only copy.

## Why

- Different channels and coins differ by orders of magnitude in size (trial, Binance, 10 minutes: median per-symbol trade file 4.3 KB, largest 495 KB compressed), so a shared hourly rotation made no sense for them.
- The coin-level model reads one coin at a time; per-symbol files and packs serve that. Splitting trades by symbol cost +3% compressed size in the trial ([storage-design.md](../storage-design.md#measured-binance-only-10-minutes-522-symbols-2026-11-13)).
- Compressing inside the collector (zstd frame every 5 s) kept one core near full at 522 symbols in the trial; appending text avoids it.
- Per-pack upload keeps object counts near 3,200 per day instead of tens of thousands of small objects, which [infrastructure.md](../infrastructure.md) says WebDAV handles badly.

## Consequences

- Local disk must hold about one day of raw plus one day of compressed, up to two days of raw if packing lags. Only Binance is estimated (about 16 to 30 GB, extrapolated from 10 minutes); the other six venues are unmeasured.
- Open: compression unit and level; upload time and failure rate of about 3,200 packs per day; descriptors and IO for hundreds of open append files per process; OneDrive quota; chart-state snapshot location; recorder host.
- Verified in code: the raw writer, packer, pack archiver, venue adapters and Go poller exist. Poll failure gap records remain not built; see [storage design](../storage-design.md).
