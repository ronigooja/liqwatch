# Architecture

## Layers

```
Data:       recording -> raw log -> replayable data lake          (data-recording.md)
Features:   price / order flow / book / impact / spot basis /
            liquidations / data quality                           (models.md)
Models:     risk-state model + in-spike models                    (models.md)
Trading:    fill simulator -> strategy rules -> risk layer -> execution   (trading-and-risk.md)
Governance: prediction logging, shadow run, monitoring,
            degrade and halt                                      (roadmap.md)
```

## Invariants

- **Visibility:** a feature at time t may use only messages whose `local_receive_time` ≤ t. Replay orders by local receive time, not exchange event time.
- **Raw first:** raw payloads are stored before parsing; parsers are versioned and re-runnable.
- **Logging:** every prediction stores model version, feature snapshot, and emit time. Labels are generated later and never feed back into the original prediction.
- **Degrade, don't guess:** unhealthy data suppresses high-confidence output and blocks new entries.

## Technology (initial choice, revisable)

- Recording and online path: Go.
- Research and training: Python (DuckDB/Polars, LightGBM).
- Storage: local NVMe for hot data, Parquet+zstd archive in object storage.
- Update cadence and latency targets are set by the trading use: see [trading-and-risk.md](trading-and-risk.md).
