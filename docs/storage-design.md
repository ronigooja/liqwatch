# Storage Design (multi-venue recording)

Design of recording, packing, upload and retention. Verified: writer, packer and archiver are built; [deployment status](infrastructure.md#vt01ok-preparation-status).
Records: [0009](decisions/0009-zero-gap-handoff.md), [0014](decisions/0014-daily-pack-storage.md), [0021](decisions/0021-gate-archiver-deletion.md), [0023](decisions/0023-packer-streaming.md), [0024](decisions/0024-pack-output-dir.md), [0025](decisions/0025-packer-drop-page-cache.md).
Venue scope: [adapters](venue-adapters.md).

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

Verified: governed by [0026](decisions/0026-live-zstd-recording.md); root-disk live compression is implemented but deployment is not performed by this change.

| Item | Rule |
|---|---|
| Path | Verified: `raw/<venue>/<symbol>/<UTC date>/<channel>.<slot>.jsonl`; append `.zst` when `[record].compress=true` |
| Split | Every channel is split by symbol: trades, mark (with index and funding), liquidations, open interest, long/short ratios, leverage tiers, index constituents. A liquidation file exists only if that symbol had a liquidation that day |
| All-market streams | Binance mark price (array) and liquidation streams are cut into one record per symbol, each keeping the original envelope shape. See [adapter filtering](venue-adapters.md#recorded-content) for other transformations |
| Content | Verified: filtered payload and unchanged v2 JSON lines; optional zstd container under [0026](decisions/0026-live-zstd-recording.md). [Recorded content](venue-adapters.md#recorded-content) |
| Rotation | UTC day boundary only. One file per symbol per channel per day; no size limit, no sequence number |
| Processes | One collector process per venue (own reconnects, limits, token handshakes); slots a/b and handoff per [0009](decisions/0009-zero-gap-handoff.md). Poller files use slot `p` |
| Gaps | Verified: one gap file per venue per day: WebSocket lifecycle below, adapter diagnostics and unchanged queue drops. Poll failures and history gaps remain in the [poller](poller.md) |

Verified in [rawlog](../internal/rawlog/writer.go) and [poller](../internal/poller/sink.go): new envelopes use `v:2`, `rw` (wall ns), `rm` (process monotonic ns), and `p`. Collector adds `wseq` and `cseq`; poller retains `meta`.

Verified: identity exists only in the path: venue, raw symbol and channel (strip optional `.zst`, `.jsonl`, then optional lowercase slot). `_all` means a bulk payload; `gap/gap` identifies venue-level gaps.

Verified: affected symbols/channels in gaps and individual symbols in bulk responses remain in payloads. Exchange timestamps remain in `p`, including MEXC `t` and `cts`. `conn` is absent from the envelope; lifecycle gaps keep it in `p`. Existing version 1 files are unchanged.

## WebSocket gap payloads

Verified in [collector](../internal/collector/collector.go); governed by [0022](decisions/0022-ws-gap-records.md). The v2 envelope is unchanged; connection identity is diagnostic payload `conn`, not an envelope key.

| Event | Verified payload fields and interpretation |
|---|---|
| `conn_closed` | Existing `cause`, `err`, `shard`, `streams`; adds `conn`, `prev` (preceding close or null), `last_rw` (last message wall ns), `last_rm` (process monotonic ns), `up_ms` (dial success to close), `msgs` (including controls). Last times are zero if none |
| `conn_opened` | `shard`, `conn`, `prev` (preceding closed connection or null), `initial` (first process connection), `attempt` (process shard dial ordinal), `backoff_ms` (last retry sleep), `down_ms` (preceding close to completed subscription writes; null without close), `subscribed`, `subs_ok` |
| `conn_first` | `shard`, `conn`; envelope `rw`/`rm` give the first received message on this connection, including controls; may precede completed subscription writes |

Verified: `conn_opened` is emitted only after every subscription write succeeds; `subscribed` counts intended `Shard.Streams`, including URL subscriptions. `subs_ok=true` is not server acknowledgement; adapters retain `subscribe_error` diagnostics. Failed writes emit no successful-open event.

Verified in [Bybit](../internal/venue/bybit/bybit.go): ticker snapshots rebuild per-symbol state but do not emit gap events. A per-connection snapshot summary would require new aggregation state; no snapshot/resync completion or replay guarantee is recorded.

Verified: correlate by `shard` and payload `conn`/`prev`, not file order (subscription and read goroutines can interleave). Each failed dial closes separately; follow `prev` across failures with zero last times to the last connection that received messages.

Verified: the observable silence interval is `[conn_closed.last_rw/last_rm, conn_first.rw/rm]`. No first event means its end is unknown. Planned overlapping replacement emits open/first without a preceding close; shutdown does not add a reconnect close.

Inferred: silence is not proof of lost exchange data; controls also count as messages, and process monotonic times are comparable only within one process. No server reset cause or completeness claim is inferred from these records.

Inferred overhead (not benchmarked): reuse the existing receive `time.Now`, message counter and stall atomic; add one reader-local `time.Time` assignment and first-message branch per message. Monotonic conversion and JSON/sink work occur only at lifecycle boundaries.

Verified: pack paths preserve venue/symbol/date and tar member names preserve channel/slot; extract into that layout before reading. Compact files renamed outside this layout cannot recover identity. Reader compatibility is specified in [0018](decisions/0018-path-based-raw-envelope.md).

Verified in [research poller](../research/liqwatch_research/poll/__init__.py): its legacy mixed-channel hourly files lack path identity and retain version 1. This is not the production Go poller format; migrating that writer requires a separate layout change.

Verified: compression defaults off. When enabled, collector and poller each share one level-1 encoder with a 64 KiB window, concurrency 1 and lower-memory mode; `[record].level` accepts 1..22.

Verified: each 4 KiB buffered output publishes independent finished frames on writes/flushes; oversized writes can be larger. Collector ticks remain 5 s and poller ticks 1 s. LRU close, shutdown and append reopen preserve concatenated-frame validity.

Verified: compression state is independent of file count; collector retains at most `MaxOpenFiles` buffers (default 4,096). Compressed poller flushes also close/release idle descriptors. [Measurements](packing-results.md#live-writer-compression) quantify the ratio tradeoff.

Verified: every-byte-cut tests require a whole-record prefix plus an incomplete-tail indication except at complete frame boundaries. Inferred: unsynced writes may still be lost on host crash; no recorder crash trial is claimed.

## Pack and compress (after the UTC day boundary)

Verified in [raw reader](../internal/rawlog/read.go): `ReadLines(path, visit) (ReadInfo, error)` streams `.jsonl` and `.jsonl.zst`, checks suffix against zstd magic, and handles concatenated frames with bounded decoder memory and a 16 MiB record limit.

Verified: callbacks borrow newline-free complete records; an unterminated final line is discarded. `ReadInfo` reports compression, stored size, delivered records/decoded bytes and `TruncatedTail`; unexpected final-frame EOF is recoverable, other decoder errors fail the file.

Verified: [poller dedup reconstruction](../internal/poller/scheduler.go) reads both suffixes through this API; non-missing read errors emit `raw_read_failed` gap diagnostics. [Stale checks](../internal/checks/checks.go) discover both suffixes without decoding.

Verified in [config](../internal/config/config.go): `[pack].dir` selects the output root; omitted or empty uses `data_dir`. A set value must be absolute and lexically outside `data_dir/raw`; raw JSONL paths and recorder state are unchanged. Designed: use real directories, not symlink aliases into raw.

Verified: output is `<output root>/packs/<venue>/<symbol>/<date>.tar.zst` for outer zstd, otherwise `.tar`; SQLite is `<output root>/manifest/manifest.db`. Packer, archiver and backlog checks share this resolution.

Verified: days containing `.jsonl.zst` use `stored_tar`: every member is copied unchanged, including plain members on mixed days; hashes/sizes cover stored bytes. Remote uses the same container suffix and `.meta.json` records unit/level 0. Basis: [0027](decisions/0027-pack-of-compressed-members.md).

Verified in [checks](../internal/checks/checks.go): disk thresholds apply independently to `data_dir` and a distinct output root, with path-qualified findings; default configuration checks only `data_dir`.

Verified in [packer](../internal/pack/pack.go): temporary packs and compressed-member spools are created beside the final pack, so publication by rename stays on the output filesystem; directories retain mode `0750`.

Verified in [packer](../internal/pack/pack.go): members stream through SHA-256; byte count, size, modification time and quiet-period checks reject changed files before publication. Raw member size has no configured cap; see Writing.

Verified: one reusable zstd encoder uses concurrency 1, a 1 MiB window and lower-memory mode; the default level remains 3. `tar_then_zstd` streams directly; `zstd_then_tar` spools one compressed member on disk to obtain its tar header size.

Verified: encoder reuse retains only bounded compression state, not whole packs; member metadata and bounded compression/copy buffers occupy heap. Manifest paths/member names identify stored containers without a schema change; streaming rationale: [0023](decisions/0023-packer-streaming.md).

Verified in [packer](../internal/pack/pack.go): Linux best-effort `FADV_DONTNEED` follows each successful member read before close; sources are never synced. Non-Linux builds use a no-op. Only past days passing quiet-period checks are eligible.

Verified: output keeps same-directory temp → sync → hash → rename publication; cache release follows the final hash read on the already-synced temporary pack. No extra output sync or source read-ahead tuning is added.

Measured: bounded anonymous memory did not bound recorder cgroup cache; see [recorder baseline](packing-results.md#recorder-baseline) and [build-machine comparison](packing-results.md#build-machine-cache-release). Inferred: cache release is not a slab or collector-drop fix; it is not yet measured on the recorder.

## Upload

| Item | Rule |
|---|---|
| Unit | One pack per venue, symbol and day; one PUT; no chunking, no `.partNNNN` |
| Remote | Verified: container suffix follows the pack rule above; `<date>.meta.json` holds size, whole-pack sha256, member hashes, compression unit and level |
| Verify | Compare remote size after upload (the storage once accepted a PUT and kept a zero-byte file). First 7 days: download and re-hash; afterwards a sampled fraction |
| Local delete | Raw files and the pack are deleted only after the pack is verified; unverified packs are never deleted |
| Manifest | Verified: SQLite, one row per pack, member checksums stored separately. Designed remote daily copy: not built; [gate](open-questions.md#archive-deployment-gates) |
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

Measured: the newer [seven-venue 5-minute sample](recording-results.md#seven-venue-load-and-recording) supersedes the earlier sample. Inferred: full-day volume and staging-disk sizing remain unmeasured; the compressed figure above uses 5-second frames, not whole-file compression.

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
| Recorder host | Verified: [VT01OK prepared, not deployed](infrastructure.md#vt01ok-preparation-status); sustained capacity remains unmeasured |
| OneDrive quota | Unchecked; the archive is a single copy |

## Code status

| Module | Change |
|---|---|
| `internal/rawlog`, `internal/pack`, `cmd/liqwatch-archiver` | Verified in code: daily raw files, packs and unchunked upload |
| Collectors | Verified in code: venue adapters registered; deployment is separate |
| Poller | Verified in code: All seven venue pollers and gap records are built; see [poller](poller.md) |

## Recorder install layout and enable order

Verified: repository preparation only; no live service installation. Policy: [0021](decisions/0021-gate-archiver-deletion.md). Host state: [infrastructure](infrastructure.md#vt01ok-preparation-status).

| Path | Prepared deployment contract (verified in units/config example unless marked designed) |
|---|---|
| `/opt/liqwatch/current/` | Release binaries: collector, poller, packer, archiver, checks; release artifact also includes mapgen for reviewed map maintenance. Verified: no `cmd/trialrec` remains. |
| `/etc/liqwatch/config.toml` | Copy [example](../config/config.toml.example); `data_dir = "/data/liqwatch"`; set archive URL to the [verified root](infrastructure.md#storage-account-and-re-test-2026-11-02). |
| `/etc/liqwatch/venue-map.toml` | Designed: install the reviewed [venue map](venue-map.md); readable by `liqwatch`. |
| `/etc/liqwatch/secrets.env` | Designed: root-owned mode `0600`; keys `LIQWATCH_WEBDAV_USER` and `LIQWATCH_WEBDAV_PASS`, values supplied out of band; systemd reads this only for archiver. Never copy credentials into the repository or release artifact. |
| `/etc/liqwatch/collector-<venue>.env` | Required by venue collector; one `LIQWATCH_METRICS_ADDR=127.0.0.1:<port>` line per venue using the port table below. |
| `/data/liqwatch/` | Designed: mounted HDD data volume, `liqwatch` write access; raw and recorder/check state remain here. Keep `data_dir` aligned with the units' fixed mount guard; output placement is defined above. |
| `/var/lib/liqwatch/pack/` | Designed root-disk output: pre-create with `install -d -o liqwatch -g liqwatch -m 0750 /var/lib/liqwatch/pack`, then set `[pack].dir` to this path. No symlink is needed. |
| `/etc/systemd/system/` | Designed: install [deploy units](../deploy/) and run `systemctl daemon-reload`; create the unprivileged `liqwatch` user first. No service installation is performed by the release workflow. |

| Venue instance | Collector metrics port (verified deployment assignment) |
|---|---:|
| binance | 9101 |
| bybit | 9102 |
| okx | 9103 |
| bitget | 9104 |
| gate | 9105 |
| mexc | 9106 |
| kucoin | 9107 |

Verified: use `liqwatch-recorder@<venue>.service` with `-venue %i` and its required metrics environment file; use `liqwatch-poller@<venue>.service` with `-venue %i`.
Verified: `liqwatch-collector@.service` remains the a/b slot template used by [handoff](../deploy/liqwatch-handoff.sh); it is NOT a venue template. The legacy unnumbered collector uses `collect.venue`.
Designed: do not run legacy/handoff collectors alongside venue collectors; their ports and writers can conflict. Per-venue zero-gap handoff is not built; do not use the legacy script for this layout.

Verified: every recorder service uses `RequiresMountsFor=/data/liqwatch` (mount requirement and ordering even with `nofail`) and `ConditionPathIsMountPoint=/data/liqwatch` (skip start if the directory is not mounted).
Verified: packer, archiver and checks units have no `ProtectSystem` sandbox; no `ReadWritePaths` change is needed. The designed output lives on `/`, not a separate mount; there is no additional mount guard. Changing a populated output root requires a separately reviewed migration.
Verified: [systemd.unit](https://www.freedesktop.org/software/systemd/man/latest/systemd.unit.html) specifies the mount dependency/ordering and mount-point condition; these guards do not validate a particular UUID or protect against all runtime unmounts.
Verified: collector limits are 192 MiB, poller 128 MiB, packer 512 MiB, archiver 128 MiB and checks 64 MiB; units impose no CPU quota. Collectors/pollers restart after 5 s, limited to 5 starts per 300 s.
Inferred: these are ceilings, not reservations or proof of aggregate fit. Bounded buffers alone did not prevent [recorder cache pressure](packing-results.md#recorder-baseline); keep the packer ceiling unchanged pending recorder measurement of cache release.

| Order | Designed operator action after separate deployment authorization |
|---|---|
| 1 | Verify mounted volume/UUID, permissions, config, reviewed map and unique metrics files. Keep both archive timer and service disabled and stopped. |
| 2 | Start one venue's recorder and poller, inspect raw files, gaps, metrics and memory, then enable those instances. Add remaining venues incrementally only after checking host pressure. |
| 3 | Enable `liqwatch-checks.timer`; set metrics targets to exactly the running collectors (example lists all seven). Checks also reports unverified age while archiving is disabled; review rather than suppress it. |
| 4 | Validate a closed-day pack with `liqwatch-packer.service`, then enable `liqwatch-packer.timer`; monitor disk growth while uploads are disabled. |
| 5 | Leave `liqwatch-archiver.timer` and `liqwatch-archiver.service` DISABLED by default. Do not invoke archiver manually on production data: it deletes files in the same run. |
| Archive gate | Verify upload, size, full hash readback and metadata using realistic pack sizes on disposable copies; resolve or explicitly accept [archive safety questions](open-questions.md#archive-deployment-gates) before separate authorization to enable the timer. A small connectivity probe is insufficient. |
