# 0003: Judge by cost-adjusted trading expectancy

Status: accepted

Context: the system is for trading. Probability gains do not imply profit once fees, spread, slippage, latency, and adverse selection are included.

Decision: the acceptance criterion is expectancy after realistic fill simulation. Calibration metrics are diagnostics. A fill simulator is built in P1 before models are judged.

Consequences: stop rules in [07-roadmap.md](../07-roadmap.md); details in [06-trading-and-risk.md](../06-trading-and-risk.md).
