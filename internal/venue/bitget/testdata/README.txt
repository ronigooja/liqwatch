Bitget live fixture capture, 2026-11-13, Linux host.

Verified in adapter code: ordinary records retain the original envelope fields except
arg.instType, arg.topic and arg.symbol. An empty arg is omitted; unknown arg fields
remain. Array data becomes a one-element array per record; object data remains an
object. data.symbol is omitted; the record symbol is derived first. Unknown item and
arg fields are retained and counted. Tier data retains the entire list. Hourly sample
records contain the complete original frame in {"sample":...}. Ticker dedup compares
filtered data excluding timestamps and forces emission after 60 seconds.

contracts.jsonl: trimmed V2 GET /api/v2/mix/market/contracts?productType=USDT-FUTURES response, three original entries. The complete response contained 885 normal perpetual entries.
frames.jsonl: one unmodified WebSocket frame per line, selected from V2 and V3 public connections. Includes text pong, subscribe acknowledgements, V3 parameter errors, snapshots, updates, Unicode symbols, and global liquidation. The V3 initial probe used V2 parameter names and generated errors; subsequent V3 probes used instType=usdt-futures, topic, symbol.

Measured: V3 publicTrade, ticker and funding-rate delivered BTC/ETH and other snapshots; publicTrade also delivered updates. Position-tier delivered BTC/ETH snapshots with an object in data and symbol inside that object, contrary to the documented array example. Global liquidation returned per-symbol array data. V2 trade and ticker also delivered data. Acknowledgements alone do not establish delivery.
Verified: V3 trade snapshot is scheduled to disappear on 2026-11-17: https://www.bitget.com/docs/uta/websocket/public/New-Trades-Channel
Verified: V3 docs state ticker 200 ms on change, funding heartbeat 60 s, position tiers on change, liquidation at most two per pair per second: https://www.bitget.com/docs/uta/websocket/public/Tickers-Channel ; https://www.bitget.com/docs/uta/websocket/public/Funding-Rate-Channel ; https://www.bitget.com/docs/uta/websocket/public/Position-Tier-Channel ; https://www.bitget.com/docs/uta/websocket/public/Liquidation-Channel
Verified: V3 docs recommend fewer than 50 channels per connection, specify 100 connections/IP and 300 connection requests/IP/5 min, and text ping every 30 s: https://www.bitget.com/docs/uta/quick-start
