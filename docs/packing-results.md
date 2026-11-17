# Packing results

Purpose: measured packer memory and cache behavior. Governing decisions: [0023](decisions/0023-packer-streaming.md), [0025](decisions/0025-packer-drop-page-cache.md). Design: [storage](storage-design.md#pack-and-compress-after-the-utc-day-boundary).

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
