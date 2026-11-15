# Storage Design (multi-venue recording)

Design of how recorded market data is written, packed, uploaded and kept. Verified in code: raw writer, packer and archiver are built; deployment is separate ([rollout](p0-rollout.md)). Governing records: [0009](decisions/0009-zero-gap-handoff.md), [0011](decisions/0011-no-alert-narrower-recording.md), [0013](decisions/0013-no-dex-recording.md), [0014](decisions/0014-daily-pack-storage.md). Venue facts: [venue-apis.md](venue-apis.md).

## Scope

| In | Out |
|---|---|
| Seven venues: Binance, Bybit, OKX, Bitget, Gate, MEXC, KuCoin | DEXes, order books, quotes (`bookTicker`) |
| Per venue, only the coins that match the 522 Binance USDT perpetuals in TRADING status | `TRADIFI_PERPETUAL` contracts (gold, oil, stocks) |

Raw venue symbol names are kept in paths. Mapping (`1000PEPE` to `PEPE`, `XBT` to `BTC`, scale prefixes) is a separate reviewed table and is not applied at recording time.

## Layers

| Layer | Where | Holds | Role |
|---|---|---|---|
| Hot | Recorder local disk | Today's raw files; closed packs awaiting upload | Buffer; a file is deleted once its pack is verified |
| Archive | WebDAV (OneDrive mount behind OpenList) | Every pack, kept without limit | The only authoritative copy |
| Research | Mac or on-demand machine | Parquet converted from downloaded packs, for modelling | Temporary; may be deleted at any time; holds nothing that must be kept |

## Writing

| Item | Rule |
|---|---|
| Path | `raw/<venue>/<symbol>/<UTC date>/<channel>.<slot>.jsonl` |
| Split | Every channel is split by symbol: trades, mark (with index and funding), liquidations, open interest, long/short ratios, leverage tiers, index constituents. A liquidation file exists only if that symbol had a liquidation that day |
| All-market streams | Binance mark price (array) and liquidation streams are cut into one record per symbol, each keeping the original envelope shape. See [adapter filtering](venue-adapters.md#recorded-content) for other transformations |
| Content | Filtered payload appended uncompressed; see [recorded content](venue-adapters.md#recorded-content). Envelope: existing fields (receive wall and monotonic time, channel, symbol, connection id, sequence) plus `venue`. Exchange timestamps stay in the payload; MEXC `t` and `cts` must be kept |
| Rotation | UTC day boundary only. One file per symbol per channel per day; no size limit, no sequence number |
| Processes | One collector process per venue (own reconnects, limits, token handshakes); slots a/b and handoff per [0009](decisions/0009-zero-gap-handoff.md). Polled data is written by the poller, with `venue` added |
| Gaps | One gap file per venue per day: disconnects, queue drops, reconnects. Poll failures and history gaps are recorded by the [poller](poller.md) |

The collector only appends text and opens no compressor. Measured in the trial: writing zstd per file with a frame closed every 5 s kept one core near full at 522 symbols plus splitting (Binance only).

## Pack and compress (after the UTC day boundary)

When all channel files of a symbol for the day are closed, they are packed into `raw/<venue>/<symbol>/<date>.tar.zst`. Compression unit and level are open (see Open items).

## Upload

| Item | Rule |
|---|---|
| Unit | One pack per venue, symbol and day; one PUT; no chunking, no `.partNNNN` |
| Remote | `<date>.tar.zst` plus `<date>.meta.json` (size, whole-pack sha256, member list with each member's sha256) |
| Verify | Compare remote size after upload (the storage once accepted a PUT and kept a zero-byte file). First 7 days: download and re-hash; afterwards a sampled fraction |
| Local delete | Raw files and the pack are deleted only after the pack is verified; unverified packs are never deleted |
| Manifest | SQLite, one row per pack, member checksums stored separately; copied to remote daily |
| Basis for no chunking | Inferred: the largest Binance coin was about 70 MB compressed per day for trades, extrapolated from 10 minutes. Measured upload: 100 MB in about 22 s, nginx limit raised to 1 GB; a 500 MB single PUT did not finish ([infrastructure.md](infrastructure.md)). Packs of other venues are unmeasured; re-evaluate if packs exceed a few hundred MB |

## Retention

| Where | Rule |
|---|---|
| Local | Delete after verification; no buffer days |
| Archive | No limit; storage governs ([0011](decisions/0011-no-alert-narrower-recording.md)) |

## Local disk size

```
floor ~ one day of raw + one day of compressed
peak  ~ two days of raw + one day of compressed (when compression or upload lags a day)
```

| Source | Raw per day | Compressed per day | Floor | Peak |
|---|---:|---:|---:|---:|
| Binance, 10-minute trial scaled by 144 (extrapolation, not a full day) | about 14 GB | about 1.4 GB | about 16 GB | about 30 GB |

Measured: the [seven-venue 5-minute sample](recording-results.md#seven-venue-load-and-recording) linearly extrapolates to about 138 GB raw per day; Gate is unreliable due to missing symbols. The earlier roughly 160 GB/day estimate from an earlier 5-minute sample is superseded (owner report, 2026-11-14). Full-day volume and staging-disk sizing remain unmeasured; the compressed figure above uses 5-second frames, not whole-file compression.

## Charting needs no raw window

A liquidation map needs aggregated state, not the raw window: volume per symbol, time bucket and price cell; per bucket mark-price high and low; latest open interest, long/short ratios and tier tables. A resident program updates it incrementally, drops expired buckets as the window slides, saves snapshots and reloads them on restart. Where snapshots live is open. Bucket length, cell width and the open/close treatment are model parameters that are not frozen, so aggregates are a cache; if lost or if parameters change, rebuild from archive packs.

## Measured (Binance only, 10 minutes, 522 symbols, 2026-11-13)

Host: 1 core, 1.8 GB, Singapore (not a latency reference). zstd default level, frame closed every 5 s. Layout A is one file per channel, B one file per symbol, same messages.

| Channel | A (zstd) | B (zstd) | B vs A | B file sizes |
|---|---:|---:|---:|---|
| `aggTrade` (244,163 messages) | 5.97 MB, ratio 8.24 | 6.17 MB, 522 files, ratio 7.97 | +3% | median 4.3 KB, p90 16.9 KB, max 495 KB (`AGTUSDT`); 423 of 522 below 10 KB |
| Mark price (928 messages, 241,686 records after split) | 5.18 MB, ratio 14.9 | 3.55 MB, 522 files, ratio 14.0 | -31%, not like for like | median 6.4 KB, max 13.0 KB |
| Liquidations (84 messages, 36 symbols) | 3.1 KB | 8.0 KB | +155% | a few hundred bytes each |

Mark-price A included contracts outside the 522 (TRADIFI and others) and B did not, so the -31% is not a pure split effect; it was not re-run. The liquidation B figure was recomputed on the raw records with `zstd -3`, because the first run's splitter had a bug. One of six connections reconnected once; gap size was not checked. Tool: `cmd/sizetest` (not committed). Scaled to a day (x144, extrapolation): trades about 0.9 GB, mark about 0.8 GB, liquidations about 1 MB, compressed.

## Open items

| Item | Note |
|---|---|
| Compression unit | A: each channel file compressed, then tarred. B: tar first, then compress as `.tar.zst`. B may compress better; unmeasured |
| Compression level | Offline, so above default is possible; unmeasured |
| Chart-state snapshot location | Not on the recorder, not in the research layer |
| Other six venues' volume | Full-day volume remains unmeasured; adapters are built |
| Upload of about 3,200 packs per day | Per-day time and failure rate unmeasured on WebDAV; [infrastructure.md](infrastructure.md) states WebDAV suits few large files |
| Many open append files per process | Descriptor and IO load unmeasured |
| Peak and busy days | Unmeasured; short-run samples are linked in [recording results](recording-results.md#seven-venue-load-and-recording) |
| Recorder host | Japan host not chosen; the 2026-11-02 recorder is down ([p0-rollout.md](p0-rollout.md)) |
| OneDrive quota | Unchecked; the archive is a single copy |

## Code status

| Module | Change |
|---|---|
| `internal/rawlog`, `internal/pack`, `cmd/liqwatch-archiver` | Verified in code: daily raw files, packs and unchunked upload |
| Collectors | Verified in code: venue adapters registered; deployment is separate |
| Poller | Verified in code: All seven venue pollers and gap records are built; see [poller](poller.md) |
