# Recording results

Measured adapter profiling and capture results; recording scope: [0015](decisions/0015-recording-scope-and-granularity.md).

| Question | Method | Numbers, sample size and spread | Date | Reproducible source |
|---|---|---|---|---|
| How did Bitget filtering optimization change CPU? | Measured: one-core CPU over a 60 s live run after 90 s warm-up, before and after. | 57.83% before; 35.19% after; one run each; spread not reported and traffic varies. | 2026-11-13 | Commit `558c48d`, commit message; exact command and traffic not recorded. |
| What did adapter captures observe? | Measured: live venue captures; details and sample sizes in each source. | See source; completeness is not established. | 2026-11-13 | [Bybit](../internal/venue/bybit/testdata/README.txt), [OKX](../internal/venue/okx/testdata/README.txt), [Bitget](../internal/venue/bitget/testdata/README.txt), [Gate](../internal/venue/gate/testdata/README.txt), [MEXC](../internal/venue/mexc/testdata/README.txt), [KuCoin](../internal/venue/kucoin/testdata/README.txt). |
