Gate futures live capture, 2026-11-13 UTC. frames.jsonl contains one unmodified
WebSocket frame per line, selected from parallel header, no-header and query
connections, plus a separate contract_stats probe. Connections subscribed
BTC_USDT, ETH_USDT, SOL_USDT, PEPE_USDT, 龙虾_USDT and !all liquidations.
The ETH trade id 973234083 appears with size "0.1" (header) and 0 (no
header and query). The stats error demonstrates that batching symbols in the
stats payload is invalid; the later acknowledgements use [contract,"1m"].
The three-minute stats probe received no data updates. Subscribe responses and
pongs are control frames, not data. Liquidations observed in the capture do
not independently prove completeness.
Source: https://www.gate.com/docs/developers/futures/ws/en/
REST source: https://www.gate.com/docs/developers/apiv4/en/futures/
