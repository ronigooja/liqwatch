# Packing results

Purpose: measured packing and live compression. Decisions: [0023](decisions/0023-packer-streaming.md), [0025](decisions/0025-packer-drop-page-cache.md), [0026](decisions/0026-live-zstd-recording.md). Design: [storage](storage-design.md).

## Second-pass compression

Verified: real-data measurement ran locally on VT01OK (`64.176.35.180`), kernel `6.1.0-50-amd64`; no active `liqwatch*` service. Raw files stayed on that machine, read-only. Decision: [0027](decisions/0027-pack-of-compressed-members.md).

| Question | Method | Measured result (sample/spread) | Date | Reproducible source |
|---|---|---|---|---|
| Does outer level 3 help real live-compressed members? | Four largest JSONL files; level 1 per file, Flush at each 5 s `rw` interval; tar then level 3, concurrency 1/window 1 MiB/lower-memory | 22,371,644 raw B; 59,691 records; 134 flushes; 2,705,634 stored B; tar 2,709,504 B → 2,709,577 B; gain −0.002694%; n=3 identical bytes, elapsed 5.237–6.020 ms | 2026-11-17 | Base `823e42a`; source selection and harness below |
| Does the synthetic control agree? | 16 ×5,000 randomized envelope-v2 aggTrade-like records; seeds 1700–1702; Flush per 500 records (100/s); same codecs | Raw 18,308,330–18,308,828 B; tar 3,523,584–3,525,120 B; outer adds 91 B; gain −0.002583% to −0.002581%; n=3 seeds | 2026-11-17 | Synthetic only; not a representativeness claim |

Verified source root: `/var/lib/dirtest3-src-20261117T043405Z`; selected files below are relative to it, sorted by descending size then path, stopping after ≥20 MiB.

| Verified file | Bytes |
|---|---:|
| `binance/EVAAUSDT/20261116/aggTrade.jsonl` | 8,181,920 |
| `okx/_all/20261116/positionTiers.p.jsonl` | 5,550,910 |
| `bybit/_all/20261116/riskLimit.p.jsonl` | 4,631,278 |
| `binance/ARIAUSDT/20261116/aggTrade.jsonl` | 4,007,536 |

Verified method: klauspost/compress v1.20.3; stream each source line plus newline through one level-1 encoder; Flush before a record when `rw-lastFlushRW >= 5e9`; close per file; tar stored members, then stream tar through level 3 three times.
Verified execution: build-only binary transferred to VT01OK; `systemd-run --wait --pipe -p Nice=19 -p IOSchedulingClass=idle -p MemoryMax=1G`; output under `/data/liqwatch/passtest-20261117T080000Z`; all outputs/binary removed afterward.
Inferred: below the decision's 1% minimum useful reduction; one largest-first mixed-channel sample does not establish all-day/all-venue ratios. No recorder services were enabled or deployed.

## Recorder baseline

Verified provenance: owner-supplied measurements, not re-run or independently inspected in this change. Source: `root@VT01OK:/root/liqwatch-packnew2-20261117T052232Z/`; recorder kernel `6.1.0-50-amd64`, 2 vCPU/2 GB; streaming packer [0023](decisions/0023-packer-streaming.md).

| Question | Method | Measured result (sample/spread) | Date | Reproducible source |
|---|---|---|---|---|
| Does bounded heap bound the cgroup? | One 5-minute run, `MemoryMax=512M`; process/cgroup sampling | RssAnon peak 20.67 MiB; VmHWM 28.51 MiB; cgroup peak 512.004 MiB; file peak 465.83 MiB; slab peak 124.49 MiB; 4,261 `memory.events max`; n=1, spread unavailable | 2026-11-17 | Evidence directory above; individual peaks need not coincide |
| Does throughput hold? | Same constrained run, first/last 60 s | 5.84 → 1.47 MB/s; n=1, spread unavailable | 2026-11-17 | Same evidence |
| Does cache grow without the cap? | 60-second `MemoryMax=infinity` control, different input subset | RssAnon 19.56 MiB; file 629.77 MiB; 0 max events; n=1, not paired; spread unavailable | 2026-11-17 | Same evidence |
| What else remains unresolved? | Same constrained run, metadata/drop inspection | About 13k file metadata entries; 3,654 Binance `markPrice_all` drops; n=1, spread unavailable; IO versus CPU cause unknown | 2026-11-17 | Same evidence |
| Is output cache material? | Owner's approximate input/output volume | About 160 MB output per 2.5 GB input; n=1, spread unavailable; smaller than input cache, not zero | 2026-11-17 | Same evidence |

## Build-machine cache release

Verified environment: NB03HK (`root@64.118.133.169`), kernel `6.1.0-21-amd64`, XFS; not the recorder. Tests use synced synthetic inputs; no global cache drop. Source hook alone is toggled; output release is enabled in both arms.

Verified source revision: this decision's change on base `16916f7`; committed harness: [Linux tests](../internal/pack/fadvise_linux_test.go). Synthetic file names, byte generator, member count and sizes are defined there.

| Question | Method | Measured result (sample/spread) | Date | Reproducible source |
|---|---|---|---|---|
| Does full-read release evict source pages? | Two equivalent 256 MiB files, `mincore` after `Pack`; filesystem preflight | Control 268,435,456 resident bytes (100%); hook 0 (0%); n=1 pair, spread unavailable; control ≥80%, hook <10% | 2026-11-17 | `go test -count=1 -v -run '^TestPackSourceCacheResidency$' ./internal/pack` |
| Does mixed-file cache growth fall? | 600 MiB, 1,028 files; 10 ms cgroup sampling, `MemoryMax=2G` | Control/hook: source resident 623,706,112/0 B; file peak growth 623,808,512/139,739,136 B; n=1 pair, spread unavailable | 2026-11-17 | Opt-in command below |
| What were the cache endpoints and runtime? | Same mixed-file run, control/hook | File baseline 3,915,776/3,907,584 B; peak 627,724,288/143,646,720 B; final 627,609,600/3,907,584 B; 2.937/2.916 s; output 113,889 B in each arm; n=1 pair | 2026-11-17 | Same test; highly compressible synthetic bytes, not representative compression ratio |

Verified reproduction: run commands under `flock /tmp/build.lock`, source `/root/Agents/environment/go-env.sh`, set `GOFLAGS=-p=1`; measurement uses `systemd-run --wait --pipe -p MemoryAccounting=yes -p MemoryMax=2G --setenv=HOME=/root` with the test working directory.

Verified opt-in command: `LIQWATCH_CACHE_MEASURE=1 go test -count=1 -v -run '^TestPackCacheMeasurement$' ./internal/pack`; members are 1,024 ×64 KiB plus 4 ×134 MiB.

Inferred: wide residency thresholds distinguish eviction from incidental readahead; preflight skips unsupported filesystems and a control below 80% skips memory-pressure cases. After successful preflight/control, hooked residency ≥10% fails, never skips. Recorder validation remains unperformed.

## Live writer compression

Verified scope: 2026-11-17; [frame writer](../internal/rawlog/compress.go), [writer experiment](../internal/rawlog/measure_test.go), [offline replay](../cmd/liqwatch-replay/main.go).
Verified: replay uses production `FrameWriter`, 4 KiB buffering, record-time five-second flushes and output LRU; original envelopes are preserved rather than reassigning sequences or discarding poller metadata.
Verified: replay is not live collection; it retains one source scanner per file and performs full decode/hash verification. Its cgroup peak includes input/output cache and verification, not just production writer state.

| Question | Method | Measured result (sample/spread) | Date | Reproducible source |
|---|---|---|---|---|
| Input and exact outputs? | Hash sources before/after; decode/SHA-256 every output | 13,380 files / 228,508,012 B; 13,373 JSONL / 228,439,737 B / 1,112,797 records; 13,373/13,373 outputs match; all sources unchanged; n=2, zero count/hash spread | 2026-11-17 | VT01OK `64.176.35.180`, `/var/lib/dirtest3-src-20261117T043405Z`, day 20261116; seven state JSON excluded |
| Default-LRU ratio? | Level 1, max-open 4,096 replay | 70,473,731 B; 3.24149:1; n=2, byte spread zero | 2026-11-17 | VT01OK, EPYC-Rome 2 vCPU, kernel `6.1.0-50-amd64`; command below |
| Replay CPU and throughput? | Rusage around replay/compression, excluding initial/final hashes | 13.1807–13.3607 CPU s; 13.0078–13.4464 wall s; 17.0–17.6 MB/s; 0.05770–0.05849 CPU s/MB; n=2 | 2026-11-17 | Same source/default LRU; decimal MB |
| Full-open replay? | Max-open 13,380, otherwise identical | 70,012,958 B; 3.26282:1; 10.6847 CPU s; 10.6197 wall s; 21.51 MB/s; 0.04677 CPU s/MB; n=1, spread unavailable | 2026-11-17 | Same source/host; 13,373 raw outputs, seven state files excluded |
| Replay cgroup memory? | Read `memory.peak` after verification while helper held alive | 4,096: 392,949,760 B (374.75 MiB); full-open: 469,274,624 B (447.54 MiB); n=1 each, spread unavailable | 2026-11-17 | Same host; `Nice=19`, `IOSchedulingClass=idle`, `MemoryMax=1G`; not per-venue production memory |
| Replay process memory? | Rusage HWM at end of replay, before final verification | 4,096: 193,488 KiB; full-open: 271,816 KiB; n=1 each, spread unavailable | 2026-11-17 | Same source/host; includes source scanners |
| Total experiment CPU? | `cpu.stat usage_usec` after report, includes hashes/verification | 4,096: 19.1691 s; full-open: 16.4311 s; n=1 each, spread unavailable | 2026-11-17 | Same transient units |
| Encoder footprint? | Warm shared encoder, GC, heap delta | 582,672 B; n=1, spread unavailable; concurrency 1, 64 KiB window, lower-memory | 2026-11-17 | Build host `38.47.101.8`, EPYC-Rome 1 CPU, kernel `6.1.0-42-cloud-amd64`; `TestFramePolicyMeasurement` |
| Production writer state? | Fresh processes; actual `Writer`, max-open 4,096, 10k queue, GC/RSS after sentinel | 4,096 paths: HWM 32,156 KiB, heap 21,789,304 B; 13,380 paths: HWM 55,616 KiB, heap 22,483,840 B; n=1 each | 2026-11-17 | Same build host; `LIQWATCH_MEASURE_FILES=4096` or `13380`, `TestWriterMeasurement`; synthetic records, not venue runtime |
| Frame-policy ratio? | Same 6,000 synthetic lines / 60 five-second batches / 834,680 B | Stream flush: 24,085 B (34.656:1); independent 5s: 28,716 B (29.067:1); independent 4 KiB: 59,780 B (13.962:1); n=1, spread unavailable | 2026-11-17 | Same build host; `LIQWATCH_MEASURE_FILES=4096 go test -count=1 -v -run '^TestFramePolicyMeasurement$' ./internal/rawlog` |

Verified source fingerprint: sorted absolute-path/SHA-256-list hash `bb5d2fdaee21d110332e13d1c0c5184be114f8ef7ef0753e5488789c9a05e8de` in both replay runs.
Inferred: 4,096 independent encoders would retain about 2.22 GiB at the measured footprint, exceeding `MemoryMax=192M`; sharing removes that multiplier. Writer-only HWM is below the ceiling but excludes live venue queues, adapters and cgroup cache.
Inferred: independent 5s frames cost 19.2% bytes versus synthetic stream flush; 4 KiB frames cost 148.2%. Constant compression state and complete frames are the chosen tradeoff; real-input ratios above govern capacity, not synthetic ratios.

| Venue | Raw bytes | Compressed bytes (4,096 LRU) | Measured ratio | Sample / date / reproducible source |
|---|---:|---:|---:|---|
| Binance | 66,384,880 | 16,131,007 | 4.115:1 | 1,567 files; two identical runs; 2026-11-17; replay source/command above |
| Bitget | 17,483,790 | 5,620,603 | 3.111:1 | 2,113 files; same runs/date/source |
| Bybit | 35,137,570 | 9,629,152 | 3.649:1 | 939 files; same runs/date/source |
| Gate | 10,820,096 | 3,973,441 | 2.723:1 | 1,976 files; same runs/date/source |
| KuCoin | 30,016,697 | 10,237,426 | 2.932:1 | 2,406 files; same runs/date/source |
| MEXC | 21,423,751 | 12,215,923 | 1.754:1 | 3,036 files; same runs/date/source |
| OKX | 47,172,953 | 12,666,179 | 3.724:1 | 1,336 files; same runs/date/source |

| Question | Method | Inferred answer | Date / source |
|---|---|---|---|
| CPU at recording rate? | Replay CPU s/MB × owner-supplied 0.7 MB/s | Default LRU 4.04–4.09% of one core; full-open 3.27%; not live-adapter CPU | 2026-11-17; measurements above |
| Root capacity? | Owner-supplied 54–65 GB raw/day ÷ measured ratio | Default 16.66–20.05 GB/day; full-open 16.55–19.92 GB/day versus owner-reported 46 GB free; packs/backlog/shared-root use consume further space | 2026-11-17; measured root preflight 47,094,849,536 B free |
| Offline reference? | Live 3.24149:1 versus owner-supplied pack 5.79:1 | Live bytes about 78.6% larger; different framing/workloads, not like-for-like pack measurement | 2026-11-17; owner reference and replay above |

Verified reproduction: linux/amd64 `./cmd/liqwatch-replay -src /var/lib/dirtest3-src-20261117T043405Z -dst /var/lib/livezstd-<unique> -max-open 4096 -hold 5m`; repeat with 13380. Require no active liqwatch units and root free ≥5 GB.
Verified: data remained on VT01OK; only the helper binary was transferred. Build-host SSH failed strict host-key verification; permitted local cross-compilation was used without SSH configuration/key changes.
Verified: replay outputs, helper and temporary reports were removed; final source count/size remained 13,380 / 228,508,012 B. No task path was created under `/data/liqwatch`; no recorder service was started.

| Mutation tried | Expected failing test | Measured exit | Date / reproducible source |
|---|---|---:|---|
| Skip buffered flush, including tick | `TestCompressedTickPublishesBeforeClose` | 1 | 2026-11-17; build host above; replace `o.buf.Flush()` with nil; `go test -count=1 -run '^<test>$' ./internal/rawlog` |
| Skip final frame end | `TestCompressedWholeFileGolden` | 1 | Same host/date; replace `return n, w.Encoder.Close()` with `return n, nil` |
| No fresh header on reopen/write | `TestCompressedWholeFileGolden` | 1 | Same host/date; omit `w.Encoder.Reset(w.Dst)` |
| Ignore compression flag | `TestCompressedWholeFileGolden` | 1 | Same host/date; replace `if w.opts.Compress` with false |
| Test helper suppresses tail error | `TestEveryFrameTruncation` | 1 | Same host/date; helper `return p, err` → `return p, nil`; production reader mutation belongs to reader verification |

| Verification question | Method | Measured result | Date / reproducible source |
|---|---|---|---|
| Linux static/build/full tests? | `go vet ./...`, `go build ./...`, `go test -count=1 ./...` | Exit 0 each; n=1 final run, no spread | 2026-11-17; `38.47.101.8`, Go from `/root/Agents/environment/go-env.sh`, `GOFLAGS=-p=1`, `flock /tmp/build.lock` |
| Darwin portability? | `GOOS=darwin go build ./...` | Exit 0; n=1 final run | Same date/build host/environment |
| Race coverage? | `CGO_ENABLED=1 go test -race -count=1` on rawlog, poller, config, collector and collector/poller/replay commands | All six tested packages passed; replay has no tests; n=1 final run | 2026-11-17; `64.118.133.169`, kernel `6.1.0-21-amd64`; same Go environment/lock |
| Diff and documentation links? | `git diff --check`; relative paths and heading anchors in six changed documents | Exit 0; zero missing paths/anchors; n=1 | 2026-11-17; worktree based on `823e42a`; live-writer implementation accompanying this table |
