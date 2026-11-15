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
Measured: over 610 s the ticker delivered 246-247 frames per symbol (four
symbols), funding 11 per symbol, and the connection stayed open with 15 s pings.
BTC received 1,601 deal, 470 fair and 559 index frames; PLAY received 68, 83
and 74 respectively. Counts are not guarantees of completeness or cadence.
Verified: the official single-ticker page states a 1 s push when trades occur;
the observed approximately 2.45 s average differs from that statement.
Verified: official futures documentation describes deal compression, ping and
contract updates at https://www.mexc.com/api-docs/futures/websocket-api/ .

In ordinary contract records, `tempMaxLeverageLimited` and
`regularMaxLeverage` are retained and counted as unknown: their meaning is not
defined by the official documentation. `riskLimitCustom` is kept unchanged.
