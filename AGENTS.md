# AGENTS.md

Applies to any agent editing files in this repository. Read this file before editing any document.

## Document architecture

Every document has exactly one type. The type decides what it holds and where new content goes.

| Type | Location | Holds | Form |
|---|---|---|---|
| Entry | `README.md` | Map of all documents: type, one-line purpose, when to read it | One table |
| Rules | `AGENTS.md` | Editing rules and this architecture | Lists and tables |
| Design | `docs/<topic>.md` | What the system is or will be: purpose, inputs, definitions, build order | Sections, current state only |
| Results | `docs/<topic>-results.md` | Measured results of one topic | One table per question |
| Facts | `docs/open-questions.md` | Verified external facts with source and date; open questions | Tables |
| Decisions | `docs/decisions/NNNN-slug.md` | One decision: Date, Decision, Why, Consequences | At most about 30 lines |
| Operations | `docs/infrastructure.md`, `docs/p0-rollout.md` | Current status of hosts and rollouts | Status tables |
| Archive | `docs/archive/` | Superseded material, read only | Untouched |

Where new content goes:

| Content | File |
|---|---|
| A measured number | Results file of its topic |
| An exchange fact (limit, endpoint, field, price) | `docs/open-questions.md` |
| A change of direction or scope | New record in `docs/decisions/` |
| Current state of a host or rollout | Operations file |
| An unresolved question | `docs/open-questions.md` |
| A definition or build step | Design file of its topic |

Each document opens with its title, one line stating its purpose, and links to the decisions that govern it. A new document is added to the `README.md` table in the same change.

## Writing rules

- Write in English, concisely, for AI agents reading selectively.
- State the current state; history lives in git and in decision records.
- Define each fact once, in the file of its type, and link to it from everywhere else.
- Search for a number before adding it; if it exists, link to it.
- Replace a result when a newer measurement supersedes it, and state the date of the kept one.
- Give every result as a table row: question, method in one line, numbers with sample size and spread, date, and a reproducible source (command, parameters, data dates, commit).
- Label every statement as measured, verified (with source) or inferred.
- Record a null result in one row: scope, outcome, location of the evidence.
- Mark designed but unbuilt parts as "not built".
- Record a change of decision as a new record that names the record it supersedes; mark the older record as superseded.
- Keep lines under about 300 characters; use a table or list for dense content.
- Check that relative links and anchors resolve before committing.
- Keep scratch output, logs and temporary files outside the repository and delete them when the task ends.

## Project rules

- Exchange facts are unverified until recorded as verified in [docs/open-questions.md](docs/open-questions.md) with an official source; verify against official documentation before stating one as fact.
- Add a modeling layer to the roadmap as committed work after it passes the ablation rule in [docs/models.md](docs/models.md).
- State accuracy only as measured values with sample size and spread.
- Start code in the roadmap phase that needs it ([docs/roadmap.md](docs/roadmap.md)).
- `docs/archive/` is superseded; check current docs before reusing a fact from it.
