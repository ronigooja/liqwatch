# AGENTS.md

Applies to any agent editing files in this repository.

## Rules

- Write docs in English, concisely. Define each fact in one file only; link to it elsewhere.
- Current design is in the files directly under `docs/`. `docs/archive/` is superseded; do not copy facts from it without checking the current docs.
- Facts about exchanges (limits, endpoints, field names, prices) are unverified until recorded as verified in [docs/open-questions.md](docs/open-questions.md). Verify against official docs before stating them as fact.
- Any new modeling layer must pass the ablation rule in [docs/models.md](docs/models.md) before being added to the roadmap as committed work.
- A change that alters a decision must add or supersede a record in `docs/decisions/` instead of silently editing history.
- Do not state target win rates or promised accuracy anywhere.
- No code until the roadmap phase that needs it is started ([docs/roadmap.md](docs/roadmap.md)).
