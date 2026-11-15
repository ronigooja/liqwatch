Trimmed live Gate API responses, captured 2026-11-14 from the Linux build host.
Each file retains the first two array entries, unchanged:
fixture-contract_stats.json: contract_stats, BTC_USDT, interval=1m, limit=5, bounded seconds.
fixture-risk_limit_tiers.json: risk_limit_tiers, offset=0, limit=1000.
fixture-contracts.json: contracts, no parameters.

Verified: *.sample.jsonl contain the first payload from the 2026-11-15 raw
archive on root@64.118.133.169, /root/ps/data/raw/gate/{BTC_USDT,_all}/20261115/.
contractStats is one history object; riskLimitTiers and contracts are arrays.
Verified: Gate's REST and WebSocket descriptions conflict; both values are kept
unchanged. REST: lsr_taker "Long/short taker ratio", lsr_account "Long/short
position user ratio": https://github.com/gateio/gateapi-go/blob/master/docs/ContractStat.md
WebSocket: lsr_taker "Long/short account number ratio", lsr_account "Long/short
taker size ratio": https://www.gate.com/docs/developers/futures/ws/en/
