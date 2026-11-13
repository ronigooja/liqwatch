MEXC futures samples, 2026-11-13 UTC

Measured: frames.jsonl contains unmodified JSON WebSocket frames, one per line,
from wss://contract.mexc.com/edge; samples include BTC_USDT, ETH_USDT,
BARD_USDT and PLAY_USDT, subscription acknowledgements, pong, errors, all five
per-symbol data channels, and both aggregated and compress=false deal payloads.
The sub.contract acknowledgement is from a separate 30-second probe; no contract
update was observed. detail.json is a subset of one successful response from
https://contract.mexc.com/api/v1/contract/detail, retaining full chosen objects.

Measured: 100 single-ticker subscriptions at 150 ms spacing on one connection
all received data in 90 seconds. This does not establish a maximum or lifetime.
Measured: the contract-detail response had 1,087 enabled USDT-settled USDT
perpetuals and no non-ASCII symbols. Error samples use invalid requests.
Verified: official futures documentation describes deal compression, ping and
contract updates at https://www.mexc.com/api-docs/futures/websocket-api/ .
