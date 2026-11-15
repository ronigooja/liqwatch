KuCoin public classic futures capture, 2026-11-13, Linux recorder host.
classic.jsonl contains selected unmodified frames, one per line, from a 610 s
capture of XBT, ETH, SOL, WIF, PEPE, DOGE, XRP and 0G USDT perpetuals.
Sixteen subscriptions were acknowledged; no error frame was observed.
Mark/index updates were change-dependent (37–378 per symbol); funding was
10 per symbol, and execution counts were 81–566 per symbol. 66 pongs arrived.
The active-contract response contained 722 entries and no non-ASCII symbols.
The token response specified pingInterval=18000 ms and pingTimeout=10000 ms.
Official classic introduction documents a 24-hour token/connection lifetime:
https://www.kucoin.com/docs-new/websocket-api/base-info/introduction
Official instrument documentation lists mark/index and funding, not OI:
https://www.kucoin.com/docs-new/3470087w0
UTA V2 mark-price separately returned mp, ip, oi, ts on a keyless connection:
https://www.kucoin.com/docs-new/3470358w0
utamark.jsonl contains three unmodified p payloads from the 2026-11-15
recording at root@64.118.141.231:/root/env-sample/data/raw/kucoin/
{SOLUSDTM,ETHUSDTM,XBTUSDTM}/20261115/utamark.jsonl.
