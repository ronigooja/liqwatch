# REST poller

Verified in [code](../cmd/liqwatch-poller/main.go): the Go poller records REST data separately from the collector, under [decision 0014](decisions/0014-daily-pack-storage.md). Deployment status: [infrastructure](infrastructure.md).

## Operation

- Verified in code: run `liqwatch-poller -config /etc/liqwatch/config.toml`; `-venue` defaults to `binance`, `-once` runs one sweep, and `-syms` restricts symbols.
- Verified in code: only Binance polling is implemented; the scheduler uses `<data_dir>/raw/poll-state.json` and writes through the daily raw writer. Poll failure gap records are not built.
- Verified in code: scheduling, retries and weight limits are implemented in [internal/poller](../internal/poller/); source caveats are in [open questions](open-questions.md#verified).
