# wicker

Design repository for a crypto perpetual-futures price-spike risk and in-spike tracking system, aimed at semi-automated or automated trading. Status: design only, no code.

## Document map

Read only what the task needs.

| File | Contents | Read when |
|---|---|---|
| [AGENTS.md](AGENTS.md) | Rules for AI agents working in this repo | Always first |
| [docs/00-overview.md](docs/00-overview.md) | Goals, products, non-goals, success criteria | Need scope or intent |
| [docs/01-architecture.md](docs/01-architecture.md) | Layers and data flow | Designing any component |
| [docs/02-data-recording.md](docs/02-data-recording.md) | P0 recording design | Working on data capture or storage |
| [docs/03-universe.md](docs/03-universe.md) | Instrument tiers and selection rules | Choosing symbols or tiers |
| [docs/04-labels-and-evaluation.md](docs/04-labels-and-evaluation.md) | Labels, leakage rules, metrics, trading-cost evaluation | Building labels, backtests, or metrics |
| [docs/05-models.md](docs/05-models.md) | Model tasks, baselines, feature layers, ablation rule | Working on modeling |
| [docs/06-trading-and-risk.md](docs/06-trading-and-risk.md) | Fill simulation, execution, risk controls | Working on strategy or execution |
| [docs/07-roadmap.md](docs/07-roadmap.md) | Phases P0–P5, gates, stop rules | Planning or checking progress |
| [docs/08-open-questions.md](docs/08-open-questions.md) | Unresolved inputs and unverified facts | Before relying on any external fact |
| [docs/decisions/](docs/decisions/) | Decision records (ADRs) | Need the reason behind a choice |
| [docs/archive/v1-architecture-zh.md](docs/archive/v1-architecture-zh.md) | v1 design (Chinese), superseded | Historical reference only |
