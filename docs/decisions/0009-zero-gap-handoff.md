# 0009 Planned restarts use overlapping handoff

Date: 2026-11-04. Extends the restart behavior in [p0-core-design.md](../p0-core-design.md) (Components 2 and 4).

## Decision

1. The collector runs as a named slot (`a` or `b`) under the template unit `wicker-collector@.service`. A planned release or config change starts the idle slot on the new release, waits until it is ready, then stops the old slot. The two slots overlap for about one to two minutes.
2. Every file name carries the slot: `<group>-<YYYYMMDDHH>.<slot>[.rN].jsonl.zst`. Crash recovery only renames `.part` files of its own slot. An exclusive file lock `state/instance-<slot>.lock` prevents two processes in one slot.
3. The new slot opens its connections spaced by `collect.open_spacing_ms`, takes its own initial depth snapshots, and counts as ready only when every shard is open and every depth symbol has a snapshot written. The handoff script requires readiness to hold for 60 s and messages to be flowing.
4. Any failure before the old slot stops leaves the old slot untouched; the new slot is stopped. This is also the rollback.
5. Unplanned depth reconnects trigger fresh snapshots for all depth symbols ([collector](../../internal/collector/collector.go) `OnReconnect`).
6. `RegisterClosed` is idempotent, and the collector skips already registered paths, so two slots never overwrite manifest state.
7. `wicker-checks` reads every configured metrics address; the collector is down only if none is reachable.

## Replay de-duplication keys (for the replay engine, P1)

Fields were checked against recorded data on 2026-11-04. Per symbol: `aggTrade` by `a`; `bookTicker` by `u`; `depth` by `(U, u)`; `markPrice` by `E` with `s`; `forceOrder` by `E` with `o.s`. Overlap of two slots produces duplicates that the replay reader drops by these keys; order is `(rm, wseq)` per slot and event keys across slots. `rm` is a per-process clock, so it is not comparable across slots; across slots use `rw`.

## Why

- A restart for an upgrade lost seconds of data and started new `.rN` segments; depth needed up to 5 minutes for the next snapshot. Doubling connections for a short overlap removes that loss for planned work.
- The futures docs state no per-IP connection cap or connection-attempt rate ([open-questions.md](../open-questions.md)). Handoff opens up to nine extra connections for a short time, so they are spaced and failures back off.

## Not covered

- Crashes and network cuts still lose the time to reconnect. `bookTicker` has no replay; `aggTrade` can be backfilled over REST (separate work); depth recovers through the new snapshot.
- Whole-host failure. Always-on dual recording would cover more but doubles cloud storage (~36 GB/day); not chosen.
