# 0024: Separate pack output from raw recording

Purpose: choose independent pack placement under [0014](0014-daily-pack-storage.md); retention remains governed by [0021](0021-gate-archiver-deletion.md).

Date: 2026-11-17.

Verified: [0029](0029-local-archive-no-webdav.md) supersedes the designed recorder disk placement; output-root configuration remains unchanged.

## Decision

- Verified in [config](../../internal/config/config.go): optional `[pack].dir` selects one root for `packs/` and `manifest/manifest.db`; empty preserves existing paths. Raw JSONL stays on HDD `data_dir` with its current layout.
- Designed recorder placement: root disk output, pre-created and owned by `liqwatch`; [install layout](../storage-design.md#recorder-install-layout-and-enable-order). No deployment is authorized by this record.
- Inferred: colocating manifest and packs gives packer, archiver and checks one consistent root; the manifest indexes both raw members and packs, while deletion still targets raw under `data_dir` only after verification.

## Why

| Question | Method / result | Date / reproducible source |
|---|---|---|
| Does root output remove recorder contention? | Measured: one 5-minute concurrent recording/packing run; root wrote about 241 MB with about 2 s I/O time; HDD mean iowait about 51% versus about 20% in the earlier 5-minute recording-only HDD run; 0 drops; spread unavailable. | 2026-11-17; VT01OK, kernel 6.1.0-50, 2 vCPU / 2 GB; owner-supplied evidence below. |
| What staging capacity is available? | Measured: root free about 46 GB; compression ratio 5.36:1 in the same single run, spread unavailable. Inferred: about 65 GB/day raw and about 12 GB/day compressed, partly extrapolated. | Same date/host/evidence. |

Verified source supplied by owner, not re-fetched: `root@VT01OK:/root/liqwatch-pack5b-20261117T031936Z/summary.txt`.
Inferred: separating output avoids compressed writes on HDD but does not eliminate HDD reads or prove sustained recorder capacity.

## Consequences

- Inferred from capacity above: root holds only a few days of unuploaded packs. Archive upload/verification is not yet validated; keep the [archive gate](../open-questions.md#archive-deployment-gates).
- Inferred: root-disk loss loses both packs and manifest; raw may remain on HDD, but verified/deleted raw cannot be recovered locally. Remote manifest backup remains not built; no new durability claim.
- Verified in [units](../../deploy/): `/data/liqwatch` retains its mount guard; root output has no analogous mount guard and units have no `ProtectSystem` sandbox to amend.
- Verified in code: pack naming, manifest schema, remote paths and deletion rules are unchanged; same-directory temporary files preserve atomic publication across different raw/output filesystems.
