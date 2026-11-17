# Storage Design (multi-venue recording)

Design of recording, packing, upload and retention. Verified: writer, packer and archiver are built; [deployment status](infrastructure.md#vt01ok-preparation-status).
Records: [0009](decisions/0009-zero-gap-handoff.md), [0014](decisions/0014-daily-pack-storage.md), [0021](decisions/0021-gate-archiver-deletion.md), [0023](decisions/0023-packer-streaming.md), [0024](decisions/0024-pack-output-dir.md), [0025](decisions/0025-packer-drop-page-cache.md).
Local destination: [0029](decisions/0029-local-archive-no-webdav.md); deletion, retention and mount safety: [0030](decisions/0030-archive-retention-and-delete-safety.md).
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
| Hot | Verified: recorder root disk | Raw files | Verified: local mode removes a past-day directory only after full verification; report-only switch below |
| Archive | Verified: local HDD | Packs and SQLite manifest | Inferred: sole archive copy; no offsite durability; host paths in [infrastructure](infrastructure.md#vt01ok-preparation-status) |
| Research | Mac or on-demand machine | Parquet converted from downloaded packs, for modelling | Temporary; may be deleted at any time; holds nothing that must be kept |

## Writing

Verified: governed by [0026](decisions/0026-live-zstd-recording.md) and [0028](decisions/0028-record-flush-interval.md); root-disk live compression is implemented but deployment is separate.

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

Verified in [config](../internal/config/config.go): `[record].flush` accepts a positive Go duration string up to 5m, used in both modes. Unset: compressed collector/poller 30 s; plain collector 5 s and poller 1 s.

Verified: collector, poller and [replay](../cmd/liqwatch-replay/main.go) all use `bufio.NewWriter` (Go default 4 KiB), not interval-sized buffers. A full buffer writes a finished frame; oversized writes can publish larger frames before a tick.

Inferred: frames for busy files are NOT 30 s long; buffer-fill frames limit ratio gains from longer ticks. Replay has no buffer-size mismatch, but record-time ticks, LRU pressure and live arrival timing can change realized ratios.

Verified in the writer/sink sources linked above: collector LRU close and shutdown flush; poller shutdown flushes and compressed ticks close/release descriptors. Append reopen preserves concatenated-frame validity; poller request scheduling is unchanged.

Verified: collector tick also publishes drop markers and cleans up past-day files. The disk-full check remains in record handling, at most once per 10 s; queue drops keep their existing thresholds and shutdown drains the queue.

Verified: `rawlog` defaults to a 200,000-item queue; collector supplies `collect.queue_size` (default 10,000). Queue consumption is continuous, independent of file flush; increasing the tick does not accumulate 30 s of records in that queue.

Verified: compression state is independent of file count; collector retains at most `MaxOpenFiles` buffers (default 4,096). [Measurements](packing-results.md#flush-interval-sweep) quantify the ratio tradeoff.

Verified: every-byte-cut tests require a whole-record prefix plus an incomplete-tail indication except at complete frame boundaries.
Inferred: nominal buffered-data crash-loss window equals the flush interval; this is not a durability measurement or a bound on queued records or unsynced kernel writes. No recorder crash trial is claimed.

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

## Archive modes

Verified in [local archiver](../cmd/liqwatch-archiver/local.go) and [verification](../internal/pack/verify.go): `archive.mode="local"` makes no network client and reads no WebDAV credentials or `remote_root`. Default mode remains `webdav` for compatibility.

| Local rule | Verified implementation |
|---|---|
| Whole pack | Reread output-root file, streaming SHA-256 and byte count must match manifest |
| Tar members | Read tar without extraction; every manifest member must occur exactly once with matching source-byte hash/size; reject extra members; decode legacy compressed members only |
| Sources | Reread every source still under `data_dir/raw`; match hash/size and optional pack-time mtime, check identity/size/mtime across the read |
| Closure | Only past UTC days, all entries regular and outside pack quiet period, no source-day leftovers; recheck directory before returning verification success |
| State | Verified: `closed → local_verified → local_deleting → local_deleted → pruned`; local states never enter remote verification/deletion. Reverify `local_verified` each run. Protocol: [0030](decisions/0030-archive-retention-and-delete-safety.md). |
| Deletion | Verified: `delete_sources=true` by default; false verifies/reports only. Persist evidence, rename to sibling `.deleting`, recheck size/mtime/member set, remove; changed content blocks and no-replace restore protects recreated original paths. |
| Resources | Sequential days, bounded decode memory/32 KiB read buffer; Linux best-effort cache release on pack and source reads, no source sync; `max_kbps=0` unlimited, otherwise pace physical reads |
| Reporting | One deletion line with member/source-byte counts; summary verified/would-delete/blocked/deleted. Rule exclusions exit 0; integrity/I/O errors nonzero |

Verified: `local_deleting` recovery runs first: neither path means deleted; original-only returns to verification; renamed paths recheck and resume. Missing members are accepted only after durable removal-start evidence. [Remaining open-handle race and tick limits](decisions/0030-archive-retention-and-delete-safety.md#consequences).

Verified: packer day parsing, raw Sweep and exact-day checks/poller reconstruction exclude `.deleting`; replay skips those directories. No separate integrity-scan command exists in this repository; verification accepts only strict UTC day paths.

## Upload

Verified: retained `webdav` mode is not selected for the local recorder. Its verification and raw/pack deletion behavior is unchanged; local `delete_sources` does not gate WebDAV deletion. WebDAV remains unvalidated at scale.

| Item | Rule |
|---|---|
| Unit | One pack per venue, symbol and day; one PUT; no chunking, no `.partNNNN` |
| Remote | Verified: container suffix follows the pack rule above; `<date>.meta.json` holds size, whole-pack sha256, member hashes, compression unit and level |
| Verify | Compare remote size after upload (the storage once accepted a PUT and kept a zero-byte file). First 7 days: download and re-hash; afterwards a sampled fraction |
| WebDAV local delete | Verified: raw files and the local pack are deleted after remote verification; local-only states are never processed |
| Manifest | Verified: SQLite, one row per pack, member checksums stored separately. Designed remote daily copy: not built; [gate](open-questions.md#archive-deployment-gates) |
| Current destination | Verified: upload dropped for this recorder; latest measured request overhead and rationale are in [0029](decisions/0029-local-archive-no-webdav.md#why) |

## Retention

| Where | Rule |
|---|---|
| Raw root | Verified: local mode deletes after full verification unless report-only is configured |
| Archive HDD | Verified: local capacity retention prunes the only copy oldest-day-first; settings, cap, protected newest day and loss consequences: [0030](decisions/0030-archive-retention-and-delete-safety.md). Unknown packs warn and remain untouched. |

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
| Archive sustained capacity | Inferred: local retention headroom and IO impact need full-day measurement; [0029](decisions/0029-local-archive-no-webdav.md#why) is only a short sample |
| Many open append files per process | Descriptor and IO load unmeasured |
| Peak and busy days | Unmeasured; short-run samples are linked in [recording results](recording-results.md#seven-venue-load-and-recording) |
| Recorder host | Verified: [VT01OK recording status](infrastructure.md#vt01ok-preparation-status); sustained capacity remains unmeasured |
| Offsite recovery | Verified: no offsite copy or archive-manifest backup is built |

## Code status

| Module | Change |
|---|---|
| `internal/rawlog`, `internal/pack`, `cmd/liqwatch-archiver` | Verified in code: daily raw files, packs and unchunked upload |
| Collectors | Verified in code: venue adapters registered; deployment is separate |
| Poller | Verified in code: All seven venue pollers and gap records are built; see [poller](poller.md) |

## Recorder install layout and enable order

Verified: this change prepares units only, without installation or enabling. WebDAV policy: [0021](decisions/0021-gate-archiver-deletion.md); local upload-gate supersession: [0029](decisions/0029-local-archive-no-webdav.md). Host state: [infrastructure](infrastructure.md#vt01ok-preparation-status).

| Path | Prepared deployment contract (verified in units/config example unless marked designed) |
|---|---|
| `/opt/liqwatch/current/` | Release binaries: collector, poller, packer, archiver, checks; release artifact also includes mapgen for reviewed map maintenance. Verified: no `cmd/trialrec` remains. |
| `/etc/liqwatch/config.toml` | Verified: [example](../config/config.toml.example) retains compatibility defaults; local recorder paths/settings are in [host status](infrastructure.md#vt01ok-preparation-status); select local mode explicitly. |
| `/etc/liqwatch/venue-map.toml` | Designed: install the reviewed [venue map](venue-map.md); readable by `liqwatch`. |
| `/etc/liqwatch/secrets.env` | Verified: only WebDAV archiver unit references it; local archive unit has no EnvironmentFile. Never copy credentials into repository or release artifact. |
| `/etc/liqwatch/collector-<venue>.env` | Required by venue collector; one `LIQWATCH_METRICS_ADDR=127.0.0.1:<port>` line per venue using the port table below. |
| Raw and pack roots | Verified: current host paths in [infrastructure](infrastructure.md#vt01ok-preparation-status). Designed: directories owned by `liqwatch`, no symlink aliases; local archive unit guards the archive mount. |
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

Verified: packer/local archive/checks require `/data/liqwatch-archive` and `/var/lib/liqwatch`, with mount-point condition only on the HDD; recorder/poller require the root path without that condition. Legacy collector/WebDAV units remain unchanged. Local archive requires `-mode local`.
Verified: packer, archiver and checks units have no `ProtectSystem` sandbox; no `ReadWritePaths` change is needed. Changing a populated output root requires a separately reviewed migration.
Verified: [systemd.unit](https://www.freedesktop.org/software/systemd/man/latest/systemd.unit.html) specifies the mount dependency/ordering and mount-point condition; these guards do not validate a particular UUID or protect against all runtime unmounts.
Verified: collector limits are 192 MiB, poller 128 MiB, packer/local archiver 512 MiB, WebDAV archiver 128 MiB and checks 64 MiB; units impose no CPU quota. Collectors/pollers restart after 5 s, limited to 5 starts per 300 s.
Inferred: these are ceilings, not reservations or proof of aggregate fit. Bounded buffers alone did not prevent [recorder cache pressure](packing-results.md#recorder-baseline); keep the packer ceiling unchanged pending recorder measurement of cache release.

| Order | Designed operator action after separate deployment authorization |
|---|---|
| 1 | Verify mounted volume/UUID, permissions, config, reviewed map and unique metrics files. Keep both archive timer and service disabled and stopped. |
| 2 | Start one venue's recorder and poller, inspect raw files, gaps, metrics and memory, then enable those instances. Add remaining venues incrementally only after checking host pressure. |
| 3 | Enable `liqwatch-checks.timer`; set metrics targets to exactly the running collectors (example lists all seven). Checks also reports unverified age while archiving is disabled; review rather than suppress it. |
| 4 | Validate a closed-day pack with `liqwatch-packer.service`, then enable `liqwatch-packer.timer`; monitor disk growth while uploads are disabled. |
| 5 | Verified repository preparation: both archive modes' units remain disabled; no deployment/enablement is performed here. Local runs delete verified raw by default; select `delete_sources=false` for report-only. |
| Archive gate | Verified: local mode does not inherit the upload gate; [remaining safety/retention questions](open-questions.md#archive-deployment-gates) still apply. WebDAV retains the realistic upload/readback checks in 0021. |
