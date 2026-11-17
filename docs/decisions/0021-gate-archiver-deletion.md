# 0021 Gate archiver deletion

Verified deployment-preparation policy; design: [storage](../storage-design.md#recorder-install-layout-and-enable-order).

Date: 2026-11-16. Qualifies deployment of [0014](0014-daily-pack-storage.md); pack format and retention code are unchanged.

Verified: local mode [0029](0029-local-archive-no-webdav.md) supersedes this record's upload gate for local mode only.

## Decision

- When deploying the prepared recorder layout, leave both archiver service and timer disabled and stopped by default.
- Before enabling archiving, verify realistic-size uploads and full readback on disposable copies, review the [safety questions](../open-questions.md#archive-deployment-gates), and obtain separate authorization.
- Use a distinct `liqwatch-recorder@.service` venue template; preserve the existing collector a/b handoff interface.

## Why

- Verified: current archiver deletes raw-day directories and packs immediately after verification; [code evidence and unresolved risks](../open-questions.md#archive-deployment-gates).
- Inferred: connectivity alone does not demonstrate safe archival of real daily packs.

## Consequences

- Designed: packing and recording can be validated without enabling deletion; operators must monitor local disk growth.
- Verified: aggregate seven-venue capacity on the [prepared host](../infrastructure.md#vt01ok-preparation-status) remains unmeasured.
- Designed: per-venue zero-gap handoff remains not built; the legacy handoff script is not used for venue instances.
