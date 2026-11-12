# wicker

Research and recording repository; main direction is the [liquidation model](docs/liquidation-model.md), see also [roadmap](docs/roadmap.md).

## Document map

Read only what the task needs.

| File | Contents | Read when |
|---|---|---|
| [AGENTS.md](AGENTS.md) | Rules for AI agents working in this repo | Always first |
| [docs/liquidation-model.md](docs/liquidation-model.md) | Main model direction | Working on liquidation research |
| [docs/poller.md](docs/poller.md) | REST poller design and status | Working on polling |
| [docs/p0-rollout.md](docs/p0-rollout.md) | Recorder rollout and status | Checking recording operations |
| [docs/p1-data-pipeline.md](docs/p1-data-pipeline.md) | Research data pipeline | Replaying or converting data |
| [docs/fill-simulator.md](docs/fill-simulator.md) | Fill simulator design and results | Evaluating simulated fills |
| [docs/trial-plan.md](docs/trial-plan.md) | Historical recording trial and measurements | Reviewing trial evidence |
| [docs/overview.md](docs/overview.md) | Goals, products, non-goals, success criteria | Need scope or intent |
| [docs/architecture.md](docs/architecture.md) | Layers and data flow | Designing any component |
| [docs/components.md](docs/components.md) | Component list per phase and design order | Deciding what to design or build |
| [docs/p0-core-design.md](docs/p0-core-design.md) | Design of collector, snapshot scheduler, log writer, validator, replay | Building or reviewing P0 components |
| [docs/p0-support-design.md](docs/p0-support-design.md) | Design of symbol manager, archiver, manifest, monitoring, quality report, deployment | Building or reviewing P0 support components |
| [docs/infrastructure.md](docs/infrastructure.md) | Hosts, storage, constraints | Deploying or sizing |
| [docs/data-recording.md](docs/data-recording.md) | P0 recording design | Working on data capture or storage |
| [docs/universe.md](docs/universe.md) | Instrument tiers and selection rules | Choosing symbols or tiers |
| [docs/labels-and-evaluation.md](docs/labels-and-evaluation.md) | Labels, leakage rules, metrics, trading-cost evaluation | Building labels, backtests, or metrics |
| [docs/models.md](docs/models.md) | Model tasks, baselines, feature layers, ablation rule | Working on modeling |
| [docs/trading-and-risk.md](docs/trading-and-risk.md) | Fill simulation, execution, risk controls | Working on strategy or execution |
| [docs/roadmap.md](docs/roadmap.md) | Phases P0–P5, gates, stop rules | Planning or checking progress |
| [docs/open-questions.md](docs/open-questions.md) | Unresolved inputs and unverified facts | Before relying on any external fact |
| [docs/decisions/](docs/decisions/) | Decision records (ADRs) | Need the reason behind a choice |
| [docs/archive/v1-architecture-zh.md](docs/archive/v1-architecture-zh.md) | v1 design (Chinese), superseded | Historical reference only |
