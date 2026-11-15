Bybit live fixtures, captured 2026-11-13 UTC from the Linux build host.

Verified: recorded-20261115.jsonl contains AVAXUSDT payloads from
/root/env-sample/data/raw/bybit/AVAXUSDT/20261115/ on 2026-11-15.
The mark snapshot and delta are reconstructed ordinary recorded payloads, not
original websocket frames. Trade and liquidation are complete sample frames.

Measured: frames.jsonl retains up to three frames per topic/type/control from a
9m59s connection to wss://stream.bybit.com/v5/public/linear, subscribed to
publicTrade, tickers and allLiquidation for BTCUSDT, ETHUSDT, SOLUSDT,
DOGEUSDT, 0GUSDT and 1000PEPEUSDT; an invalid-topic error is from a separate
connection. There were 29 pongs, 1 successful subscription acknowledgement,
17,897 trade, 25,598 ticker and 26 liquidation frames in the full capture.
The fixture includes snapshots, deltas, pongs, an acknowledgement and error.

Measured: tickers carry markPrice, indexPrice, fundingRate, openInterest and
singleOpenInterest as well as best bid/ask; deltas omit unchanged fields.
For the first three saved deltas, intervals were 99-101 ms for four symbols,
100-200 ms for 0GUSDT and 200-300 ms for DOGEUSDT (two intervals per symbol). Trades are event-driven;
liquidations were intermittent, not sufficient to measure 500 ms batching.
One BTC snapshot had both-sides openInterest 69827.276 and singleOpenInterest
34913.638. Completeness and units are not established by these observations.

Measured: instruments.json is a trimmed real response to GET
https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000;
only 0GUSDT, BTCUSDT and ETHUSDT entries are retained. The full response had
930 instruments, no next cursor, and 822 Trading USDT-settled USDT-quoted
linear perpetuals. Do not use this trimmed fixture as a universe snapshot.

Verified against Bybit documentation, not independently established live:
https://bybit-exchange.github.io/docs/v5/websocket/public/ticker
https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation
https://bybit-exchange.github.io/docs/v5/market/open-interest
Ticker cadence is 100 ms; liquidation pushes are 500 ms and p is bankruptcy
price; linear BTC open interest is in BTC, and openInterest sums both sides.
