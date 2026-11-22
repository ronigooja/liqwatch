# Recording results

Measured adapter profiling and capture results; recording scope: [0015](decisions/0015-recording-scope-and-granularity.md).

| Question | Method | Numbers, sample size and spread | Date | Reproducible source |
|---|---|---|---|---|
| How did Bitget filtering optimization change CPU? | Measured: one-core CPU over a 60 s live run after 90 s warm-up, before and after. | 57.83% before; 35.19% after; one run each; spread not reported and traffic varies. | 2026-11-13 | Commit `558c48d`, commit message; exact command and traffic not recorded. |
| What did adapter captures observe? | Measured: live venue captures; details and sample sizes in each source. | See source; completeness is not established. | 2026-11-13 | [Bybit](../internal/venue/bybit/testdata/README.txt), [OKX](../internal/venue/okx/testdata/README.txt), [Bitget](../internal/venue/bitget/testdata/README.txt), [Gate](../internal/venue/gate/testdata/README.txt), [MEXC](../internal/venue/mexc/testdata/README.txt), [KuCoin](../internal/venue/kucoin/testdata/README.txt). |

## Poller field review

| Question | Method | Numbers, sample size and spread | Date | Reproducible source |
|---|---|---|---|---|
| What poller sample supported field review? | Measured: sampled REST poller data across venues; initial lookback 26 h. | 40,770,860 bytes, 12 symbols per venue; one sample, spread unreported. | 2026-11-15 | `root@64.118.133.169:/root/ps/data`, not in repo; exact command and commit not reported. |

## REST production-scheduler trial

Verified source: `root@64.118.133.169:/root/liqwatch-rest5-20261115T102846Z/` (trial root below); commit `f2110dea0e94230ed66173d36ddd01e468ecf140`.
Verified reproduction: `evidence/liqwatch-rest5-methodology.txt` records build/launch commands; `evidence/run.json` records exact `liqwatch-poller -config <root>/config/config.toml -venue <venue>` commands, including Binance's full mapped `-syms` selection.
Verified evidence: `evidence/report.md`, `channels.csv`, `analysis.json`, `files.json`, `manifest.json`, `hashes.txt`; retained `config/`, `state/`, `logs/`, `data/raw/` and `evidence/source-reproduction.tar.gz`.
Verified SHA-256: config `81f8ceed64a62de931f9ee30829d5125614804697c6d2d9ee8e7f42ac12cf808`; venue map `eedf704c793768fd04827e514ac3875f35e27c54b326fd453d42ce4e972fb8df`.

| Question | Method | Numbers, sample size and spread | Date | Reproducible source |
|---|---|---|---|---|
| What did startup record? | Measured: seven REST-only production schedulers, fresh state, 10:29:39.568–10:34:39.613 UTC; initial lookback and overlap both 2 h; default rates/periods/weights, no manual retry or rerun. | 113,376 rows; 47,957,146 logical JSONL bytes; 5,975 files; one run, between-run spread unavailable. Six venues ran 299.954–299.999 s; Bitget 19.232 s. | 2026-11-15 | Trial root: `evidence/report.md`, `run.json`, `channels.csv`, retained config; commit above. |
| What was Binance's startup output? | Measured: mapped selection; rows/bytes and unique successful/expected jobs from final state, not HTTP totals. | 520 symbols; 19,994 rows; 6,540,652 bytes; 1,428/3,642 jobs; initial sweep incomplete; one run. | 2026-11-15 | Trial `channels.csv`, `state/`, `files.json`; row-size min/median/max retained per channel. |
| What was Bybit's startup output? | Measured: same method. | 470 symbols; 10,383 rows; 6,573,789 bytes; 472/472 jobs; initial sweep complete; one run. | 2026-11-15 | Same trial evidence and commit. |
| What was OKX's startup output? | Measured: same method. | 266 symbols; 21,875 rows; 8,545,060 bytes; 1,221/1,332 jobs; initial sweep incomplete; one run. | 2026-11-15 | Same trial evidence and commit. |
| What was Bitget's startup output? | Measured: same method; safety-stop after second 429. | 453 symbols; 77 rows; 163,696 bytes; 75/2,266 jobs; initial sweep incomplete; one run. | 2026-11-15 | Same trial evidence and commit; `run.json` stop reason. |
| What was Gate's startup output? | Measured: same method. | 500 symbols; 60,052 rows; 25,045,886 bytes; 502/502 jobs; initial sweep complete; one run. | 2026-11-15 | Same trial evidence and commit. |
| What was MEXC's startup output? | Measured: one map-filtered bulk detail call. | 513 selected; 508 rows; 206,610 bytes; 1/1 job complete; missing `CROSS_USDT`, `CTK_USDT`, `DASH_USDT`, `RIF_USDT`, `XMR_USDT`; one run. | 2026-11-15 | Trial `data/raw/mexc/`, `symbols.json`, `channels.csv`; commit above. |
| What was KuCoin's startup output? | Measured: same final-state method. | 486 symbols; 487 rows; 881,453 bytes; 487/487 jobs; initial sweep complete; one run. | 2026-11-15 | Same trial evidence and commit. |
| Which Bitget rate responses were retained? | Measured: gap records, first attempts on distinct jobs; no retry before safety-stop. | Two HTTP 429s: `/api/v2/mix/market/long-short`, `1000000MOGUSDT`; `/api/v2/mix/market/taker-buy-sell`, `AEROUSDT`. HTTP 418/403: zero; one run. | 2026-11-15 | Trial `data/raw/bitget/gap/20261115/gap.p.jsonl`, `run.json`; limits and unknown cause: [facts](open-questions.md#poller-field-policy-questions). |
| Which other failures occurred? | Measured: gap records; events are not distinct jobs. | Binance constituents: nine invalid-symbol 400 events on one job; OKX currency account ratio: seven `51012` events on `CT-USDT-SWAP`; one stop-canceled OKX taker job, not a source failure. One run. | 2026-11-15 | Trial `data/raw/{binance,okx}/gap/20261115/gap.p.jsonl`; subsequent [fix scope](poller.md#operation). |
| How many MEXC unknown fields were observed? | Measured: cumulative production counter on idle callbacks, not a distinct-fields-per-log count. | Count 3 repeated on 300 callbacks; not 900 unknown fields. One run; field names were not logged; subsequent [bounded field audit](open-questions.md#mexc-contract-detail-audit). | 2026-11-15 | Trial `logs/`, `evidence/report.md`; commit above. |
| Were output and recurring behavior validated? | Measured: JSONL parsing and state inspection; no hourly/daily boundary, recurring cycle or restart. | All 113,376 lines parsed and newline-terminated; zero history duplicate/revision rows observed. Null result: recurring-cycle and restart/overlap dedup were not exercised; one run. | 2026-11-15 | Trial `analysis.json`, `files.json`, `run.json`; commit above. |
| Can this startup be extrapolated to daily storage? | Inferred: initial histories and snapshots dominate; three venue sweeps incomplete and no recurring samples. | No daily extrapolation supported; one startup run only. Per-channel row-size spread and schedules retained, not a full-day measurement. | 2026-11-15 | Trial `channels.csv`, `report.md`; commit above. |

## Bitget ratio coverage

| Question | Method | Numbers, sample size and spread | Date (UTC) | Reproducible source |
|---|---|---|---|---|
| Which contracts does the live support list contain? | Measured: public GET, one snapshot. | HTTP 200, 538 bytes; `futureList` has 20 entries, 15 of which intersect the 453 `auto` or `reviewed` Bitget mappings. | 2026-11-22 13:45 | `curl -sS https://api.bitget.com/api/v2/spot/market/support-symbols`; `config/venue-map.toml` at `515cca5`. |
| Do mapped ratio requests return rows outside that list? | Measured: one public GET per symbol/endpoint, 1.1 s pause, from the Linux build hosts. | Listed: all 9 requests for 3 mapped symbols returned 30 rows. Unlisted: all 21 requests for 7 mapped symbols returned `40054`, `data:null`. See per-request table. | 2026-11-22 13:45–13:46 | `curl -sS 'https://api.bitget.com/api/v2/mix/market/<endpoint>?symbol=<symbol>&period=5m'`. |
| Does the same error occur on `taker-buy-sell`? | Measured: same request with that endpoint, one GET per symbol. | BTCUSDT: HTTP 200, 2120 bytes, 30 rows. AAVEUSDT: HTTP 400 `40054`, 102 bytes, zero rows. | 2026-11-22 | Same command as ratio probes. |
| Did the fresh recorder mark empty ratio jobs? | Measured: one read-only snapshot 13 min after the live floor. | `longShort`: 0/125 with cursor; `positionLongShort`: 2/163; neither marked no-data. No `accountLongShort` state yet. `positionTiers`: 453 successful jobs. | 2026-11-22 13:47 | On the recorder, count nonzero `row_timestamp` and `no_data_at` by `jobs` key prefix in `/var/lib/liqwatch/data/raw/poll-state-bitget.json`. |

Measured values below are HTTP status / response body bytes / rows. `SOXLUSDT` is a listed control outside the mapping. See [verified scope](open-questions.md#poller-field-policy-questions).

| Symbol | In `futureList` | `long-short` | `account-long-short` | `position-long-short` |
|---|---|---|---|---|
| BTCUSDT | yes | 200 / 2820 / 30 | 200 / 3455 / 30 | 200 / 3523 / 30 |
| AKEUSDT | yes | 200 / 2819 / 30 | 200 / 3448 / 30 | 200 / 3544 / 30 |
| EVAAUSDT | yes | 200 / 2819 / 30 | 200 / 3451 / 30 | 200 / 3538 / 30 |
| SOXLUSDT | yes | 200 / 2814 / 30 | 200 / 3450 / 30 | 200 / 3543 / 30 |
| AAVEUSDT | no | 400 / 102 / 0 | 400 / 102 / 0 | 400 / 102 / 0 |
| DOGEUSDT | no | 400 / 102 / 0 | 400 / 102 / 0 | 400 / 102 / 0 |
| TOSHIUSDT | no | 400 / 103 / 0 | 400 / 103 / 0 | 400 / 103 / 0 |
| 1MCHEEMSUSDT | no | 400 / 106 / 0 | 400 / 106 / 0 | 400 / 106 / 0 |
| ACUUSDT | no | 400 / 101 / 0 | 400 / 101 / 0 | 400 / 101 / 0 |
| 2ZUSDT | no | 400 / 100 / 0 | 400 / 100 / 0 | 400 / 100 / 0 |
| 0GUSDT | no | 400 / 100 / 0 | 400 / 100 / 0 | 400 / 100 / 0 |

## Seven-venue load and recording

| Question | Method | Numbers, sample size and spread | Date | Reproducible source |
|---|---|---|---|---|
| What was the all-venue load? | Measured: one Singapore host run, 00:54:05–00:59:05 UTC, starting 60 s after the last process; CPU sampled about every 2 s. | Whole machine CPU 54.4% mean, 100.0% max; one run, other spread unreported. | 2026-11-15 | Measurement sub-agent report, 2026-11-15; no result files were kept; commit `650f432`. |
| What was each collector's load and coverage? | Measured: same run; per venue: universe; symbols with trade/liquidation/mark/ticker records; mean CPU %, max RSS MiB, window MB, first record seconds. Dash means not reported. | Binance: 522; 522/0/522/–; 4.8, 31.3, 116.9, 0.60. Bybit: 469; 465/14/469/–; 6.9, 85.5, 59.0, 0.65. One run; spread unreported. | 2026-11-15 | Measurement sub-agent report, 2026-11-15; no result files were kept; commit `650f432`. |
| What was each collector's load and coverage? | Measured: same run and units. | OKX: 266; 265/15/266/–; 7.7, 40.6, 109.6, 0.75. Bitget: 453; 350/7/–/453; 18.3, 50.0, 24.9, 0.70. One run; spread unreported. | 2026-11-15 | Measurement sub-agent report, 2026-11-15; no result files were kept; commit `650f432`. |
| What was each collector's load and coverage? | Measured: same run and units. | Gate: 500; 447/10/–/500; 2.4, 43.4, 29.0, 1.24. MEXC: 508; 508/–/507/508; 3.9, 49.7, 39.8, 0.65. KuCoin: 486; 470/–/486/–; 5.0, 37.5, 53.2, 1.64. One run; spread unreported. | 2026-11-15 | Measurement sub-agent report, 2026-11-15; no result files were kept; commit `650f432`. |
| Were records valid and throttled? | Measured: same run; throttle checks on 20 sampled symbols per venue. | All lines parsed, no truncated line; throttle check passed; zero reconnects, stalls, dropped records or subscription errors in logs across seven venues. Subscription-error counters were not exposed; one run, spread unreported. | 2026-11-15 | Measurement sub-agent report, 2026-11-15; no result files were kept; commit `650f432`. |
| Which older load and validation results were superseded? | Measured: 2026-11-14 seven-venue run, superseded by the 2026-11-15 run because it preceded read-during-subscribe fix `98654c7` and Gate spacing of 100 ms. | The following five 2026-11-14 rows are superseded; their date is retained. | 2026-11-14 | Measurement sub-agent report (2026-11-14); exact commands, commit and logs unavailable. |
| What was the all-venue load? | Measured: one 5-minute run, Singapore host clock; poller merged, before read-during-subscribe and Gate spacing changes. | Whole machine CPU 55.7% mean, 67.2% max; per venue below; spread otherwise unreported. | 2026-11-14 | Measurement sub-agent report (2026-11-14); exact commands, commit and logs unavailable. First attempt discarded after SSH failures and DNS timeouts. |
| What was each collector's load? | Measured: same run; CPU mean, RSS maximum, raw bytes; MB/day is a linear 5-minute extrapolation, not a daily measurement. | Binance: 522 symbols, 6.0%, 31 MiB, 138.4 MB, 39,858 MB/day; Bybit: 469, 8.6%, 33 MiB, 71.4 MB, 20,552 MB/day. | 2026-11-14 | Measurement sub-agent report; exact commands, commit and logs unavailable; one run, spread unreported. |
| What was each collector's load? | Measured: same run and units. | OKX: 266, 8.6%, 37 MiB, 130.2 MB, 37,496 MB/day; Bitget: 453, 19.0%, 45 MiB, 29.7 MB, 8,567 MB/day. | 2026-11-14 | Measurement sub-agent report; exact commands, commit and logs unavailable; one run, spread unreported. |
| What was each collector's load? | Measured: same run and units; Gate extrapolation unreliable because symbols were missing. | Gate: 500, 0.7%, 24 MiB, 6.8 MB, 1,957 MB/day; MEXC: 508, 4.3%, 43 MiB, 42.4 MB, 12,198 MB/day; KuCoin: 486, 5.3%, 34 MiB, 54.8 MB, 15,769 MB/day. | 2026-11-14 | Measurement sub-agent report; exact commands, commit and logs unavailable; one run, spread unreported. |
| Were records valid and throttled? | Measured: same run; 20 sampled symbols per venue for throttle checks. | Zero dropped records on all seven; at most one mark/ticker/index per symbol per second and funding per minute in sample; Bybit 37,790 marks, zero partial; all lines parsed, no truncated final line. | 2026-11-14 | Measurement sub-agent report; exact commands, commit, total record counts and logs unavailable; one run, spread unreported. |
| What did the poller recheck establish? | Measured: second-run check during the same measurement. | No records appended because jobs were not yet due; a real second fetch and deduplication were not tested; counts and spread unreported. | 2026-11-14 | Measurement sub-agent report; exact commands, timing and logs unavailable. |
| What happened to Gate before the fix? | Measured: same all-venue run; subscription phase was not read while writing at 700 ms spacing. | 200 of 500 symbols had no ticker data; 21 reconnects in 5 minutes; subscription phase 71 s; other pre-fix observations 19–22 resets per 10 minutes. | 2026-11-14 | Measurement sub-agent report; exact commands, commit and logs unavailable; reset sender unknown, see [open issue](open-questions.md#recording-open-issues). |
| Did reading during subscription improve coverage? | Measured: post-fix Gate run with 700 ms spacing restored; separate four-collector run. | Gate 500/500 and zero reconnects in 10 minutes; four together for 5 minutes: Bitget 453/453, MEXC 508/508, KuCoin 486/486, Gate 500/500; zero reconnects or stalls. | 2026-11-14 | Measurement sub-agent report; [fix](../internal/collector/collector.go), commit `d559adc`; one run per configuration, spread unreported; exact commands and logs unavailable. |
| How consistent was Bybit overlap? | Measured: live 3-minute run, 50 symbols, two replacements per shard. | 2,271 mark records, zero partial; zero price differences above 1% in 242 overlap comparisons. Consistency signal, not proof; one run, spread unreported. | 2026-11-15 | Measurement sub-agent report; [implementation](../internal/venue/bybit/bybit.go), commit `2f80c01`; exact commands and logs unavailable. |
| How long does Bitget mutation testing take? | Measured: exhaustive and default sampled test modes. | 166 s exhaustive; about 7.7 s sampled; run counts and spread unreported. | 2026-11-15 | Measurement sub-agent report; [test](../internal/venue/bitget/split_diff_test.go), `LIQWATCH_SLOW_TESTS=1` enables exhaustive mode; exact commands and logs unavailable. |
## Production channel completeness

Measured on VT01OK, 2026-11-22. Scope and cadence: [required-channel design](storage-design.md#required-channel-freshness). Counts are nonempty members per symbol (not records); `*` means the sole `_all` slot. Ages are newest nonempty live file, in seconds; `—` means no file today.

| Venue / question | Method | Channel: covered symbols on Nov 20 / Nov 21; Nov 22 age | Date / source |
|---|---|---|---|
| Binance WS, 522 symbols | Tar member names/sizes; raw stat | `aggTrade` 522/522; 0s. `markPrice_all` 522/522; 10s. `forceOrder_all` 0/0; —. | [Audit source](#production-channel-audit-method) |
| Binance REST | Same | `brackets` 522/522; 47918s. `exchangeInfo` 1/1*; 47759s. `fundingRate` 522/522; 108s. `openInterestHist` 522/522; 5066s. | Same |
| Binance REST ratios | Same | `globalLongShortAccountRatio` 522/522; 26666s. `topLongShortAccountRatio` 522/522; 18s. `topLongShortPositionRatio` 522/522; —. | Same |
| Bybit WS, 470 mapped | Same | `trade`, `mark` each 469/469; both 0s. `liquidation` 467/466; 130s. | Same |
| Bybit REST | Same | `accountRatio` 469/469; 62s. `riskLimit` 1/1*; 4689s. `instruments` 1/1*; 47923s. | Same |
| OKX WS, 266 mapped | Same | `trade`, `mark`, `oi`, `funding` each 265/265; 0/10/10/40s respectively. `index` 266/266; 10s. `liquidation` 262/264; 10s. | Same |
| OKX REST ratios | Same | `longShortAccountRatio` 264/264 currencies; 26706s. Contract, top-trader account and top-trader position ratios each 265/265; 5104s, 70s and — respectively. | Same |
| OKX REST snapshots | Same | `positionTiers` 1/1*; 4651s. `instruments` 1/1*; 47922s. | Same |
| Bitget WS, 453 mapped | Same | `trade`, `ticker`, `funding`, `tier` each 453/453; 0/10/40/1240s respectively. `liquidation` 396/392; 40s. | Same |
| Bitget REST | Same | `positionTiers` 453/453; 158s. `contracts` 1/1*; 47925s. `accountLongShort`, `longShort`, `positionLongShort` each 17/16; 262/19/1462s respectively. | Same |
| Gate WS, 500 mapped | Same | `trades`, `tickers`, `contract_stats` each 500/500; 0/10/10s respectively. `liquidations` 448/382; 10s. Optional `contract_info` 3/4; 1270s. | Same |
| Gate REST | Same | `contractStats` 500/500; 11s. `riskLimitTiers` 500/500; 4718s. `contracts` 1/1*; 47922s. | Same |
| MEXC WS, 508 mapped | Same | `trade`, `mark`, `index`, `ticker`, `funding` each 508/508; 0/10/10/10/40s respectively. Optional `contract` 74/46; 2680s. | Same |
| MEXC REST | Same | `contractDetail` 508/508; 47925s. | Same |
| KuCoin WS, 486 mapped | Same | `trade`, `mark`, `utamark`, `funding` each 486/486; 0/10/10/40s respectively. | Same |
| KuCoin REST | Same | `contracts` 1/1*; 47925s. `riskLimit` 486/486; 24s. | Same |
| Venue-wide liquidation totals | Sum compressed liquidation member bytes; Nov 20 / 21 | Binance 0/0; Bybit 2675838/1797327; OKX 3351172/2295836; Bitget 1286531/879279; Gate 1419953/943357. MEXC/KuCoin not applicable. | Same |
| Liquidation envelope | One BTC member per active venue, first 600 bytes only | Bybit, OKX, Bitget and Gate each yielded one parseable v2 record; `p.data` on the first three, `p.result` on Gate. No Binance sample was available. | Same |
| Earlier Binance coverage | All 524 packs/day including `_all` and `gap`, no decompression | `forceOrder_all` also 0 on Nov 17–19. | Same |
| Bitget ratio breadth | Loaded poller job state; three endpoints, 453 jobs each | Each had 33 with a row cursor and zero marked no-data. Inferred: endpoint returns no usable rows for most symbols (code accepts empty responses / selected 400s); not proved from recorder journal. | Same; [source](../internal/poller/bitget.go) |
| Bybit/OKX coverage shortfalls | Usable map minus archive member keys | Bybit `HAJIMIUSDT` lacks trade/mark/account ratio; OKX `RVN-USDT-SWAP` has index only; currency ratios absent for `CT` and `RVN`. | Same; [public follow-up](#channel-coverage-follow-up) |
| Binance cause | Inspect split code, sampled journal and separate fix | Verified at `3927f79`: case-insensitive decode let order side `S` overwrite symbol `s`, breaking routing. One journaled dial timeout on the shared shard does not explain liquidation-only absence. | Same; [recording issues](open-questions.md#recording-open-issues) |
| Daily ratio freshness | Today-only stat and poller cursor state | Binance and OKX position ratio absent at sample time; all relevant jobs had cursors. Inferred: pending daily phases, not a whole-day outage; Nov 21 coverage was normal. | Same; [schedule](poller.md#scheduling-and-pacing) |

### Production channel audit method

Verified source: read-only SSH to VT01OK at 2026-11-22 13:16–13:25 UTC; code baseline `ef7dabb`. Filter the configured map to `auto`/`reviewed`, group by venue; Binance used its recorded universe. Production had no poll period overrides.
Verified reproduction: list `/data/liqwatch-archive/packs/<venue>/*/202611{20,21}.tar` with `tar tvf`; group positive member bytes by venue, symbol and channel, excluding `gap`. For ages, `stat` `/var/lib/liqwatch/data/raw/<venue>/*/20261122/*` and subtract newest mtime from scan-end time.
Verified reproduction: inspect `tar xOf … <liquidation member> | zstd -dc | head -c 600` for `BTCUSDT`, `BTC-USDT-SWAP` or `BTC_USDT` as applicable. Read `poll-state*.json` cursor presence and bounded recorder/poller unit journals only; no archive or raw data copied.

### Channel coverage follow-up

Verified source: 2026-11-22 UTC, public GETs at 13:45–13:50 from the build host and live VT01OK raw files after reinstall, with recorder startup at 13:34. Map and adapter baseline: `515cca5`. Each API response and live count is one snapshot unless noted.

| Question | Method | Result | Reproducible source |
|---|---|---|---|
| Which mapped markets were live? | Measured: intersect each adapter's eligible instrument list with `auto`/`reviewed` map keys. | Bybit 469/470 eligible (831 API); OKX 265/266 (531 API); Bitget 453/453 (892 API); Gate 500/500 (1,037 API). | The adapters' `Symbols` URLs and filters; [map](../config/venue-map.toml). |
| Bybit HAJIMI? | Measured: symbol lookup and ratio GET. | Only Bybit map miss. `Closed`; delivery time Nov 13 09:00 UTC. Ratio code `0`, zero rows. | `api.bybit.com`, `/v5/market/instruments-info?category=linear&symbol=HAJIMIUSDT`; `/v5/market/account-ratio` with same category/symbol, `period=5min&limit=1`. |
| OKX RVN and CT? | Measured: targeted swap/index and currency ratio GETs. | RVN: sole OKX map miss; swap `51001`, index `0` with a price. CT swap live. Both currency ratios `51012`; BTC control `0` with rows. Live RVN files absent. | `www.okx.com`: `/api/v5/public/instruments?instType=SWAP&instId=RVN-USDT-SWAP`; `/api/v5/market/index-tickers?instId=RVN-USDT`; `/api/v5/rubik/stat/contracts/long-short-account-ratio?period=5m&ccy=RVN` (also CT, BTC). |
| Why did archived RVN have an index but fresh raw does not? | Verified: [OKX shards](../internal/venue/okx/okx.go) subscribe to the underlying index for each discovered mapped live swap at startup. | Inferred: the earlier recorder still had the RVN index subscription; fresh discovery excludes the swap and hence its index. | Compare the archived and fresh RVN rows above. |
| Did liquidations and optional updates arrive? | Measured: count positive-length members at 13:46 and 13:51 UTC. | Liquidation: Bybit 15→33, OKX 16→25, Bitget 9→16, Gate 9→25. Gate `contract_info`: 4→4, all first records `event=all`. MEXC `contract`: 0→0. | `find /var/lib/liqwatch/data/raw -path '*/20261122/*' -size +0c`; group exact channel filenames. For Gate snapshots, `zstd -dc <member> | head -c 360`. |
| Do supported frames route and retain expected values? | Verified: compare literal whole records and complete samples, including out-of-scope items in global feeds. | Inferred: variable symbol breadth fits event-driven feeds; no routing bug found. OKX/Bitget sample upstream; the deleted packs cannot establish all-event completeness. | [Coverage goldens](../cmd/liqwatch-collector/venue_coverage_test.go); [feed inventory](venue-apis.md#recordable-data-per-venue-liquidation-model-inputs). |
| What Gate field triggered the warning? | Measured: diff union of live API row keys against policy; inspect one production snapshot and unit journal. | Null: zero unknown keys among 53 keys across 1,037 API rows; one live snapshot had only declared keys. No warning in available journal for the queried window. | `GET https://api.gateio.ws/api/v4/futures/usdt/contracts`; [policy](../internal/poller/gate_policy.go); `_all/20261122/contracts.p.jsonl.zst`; `journalctl -u liqwatch-poller@gate --utc --since '2026-11-22 12:15:00 UTC'`. |

### Required-channel freshness after reinstall

| Question | Method | Result | Date and source |
|---|---|---|---|
| Why were three Binance and three OKX histories missing? | Verified: replay scheduler ranks using live universe and floor. | Inferred: all six were not yet eligible at 13:50Z; earliest phases for these channels spanned 18:00Z through 06:00Z next day (six channels). | 2026-11-22; `7e868ca`, `raw/poll-state*.json`, `/etc/liqwatch/venue-map.toml`, [scheduler](../internal/poller/scheduler.go). |
| Had any other missing polls already run? | Measured: count successful jobs, row cursors and nonempty raw files. | At 14:16Z: Binance funding 15 successes/0 cursors; Bitget `longShort` 125/0; `accountLongShort` 367/11, first raw mtime 14:00:58Z. One snapshot, no repeat spread. | 2026-11-22; `7e868ca`, `raw/poll-state*.json`, `stat raw/<venue>/*/20261122/<channel>.p.jsonl*` on VT01OK. |
| What did the new check report? | Measured: `nice -n 19 liqwatch-checks -config <temp config>`, live raw symlink, scratch alert and pack roots. | At 14:18Z: zero findings across seven venues; 171ms wall, 155ms CPU, n=1 on the final binary. | 2026-11-22, VT01OK; this change; [`liqwatch-checks`](../cmd/liqwatch-checks/). |
