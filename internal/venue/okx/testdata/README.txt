OKX live fixtures, captured 2026-11-13 from the Linux build host.
public.jsonl and business.jsonl contain up to four frames per channel or control type,
one frame per line; the instrument response is trimmed to ten original entries.
Capture: six swaps (BTC, ETH, SOL, DOGE, PEPE, WIF), SWAP liquidations;
one subscription request per endpoint, text ping every 20 seconds, 10 minutes.
Counts: trades 77613, mark 17844, index 8655, OI 366, funding 66,
liquidations 69, pongs 29 per connection; each connection also sent a notice.
No error frame occurred. Index instId is the underlying index, not the swap ID.
REST www.okx.com returned 200; aws.okx.com did not resolve on the build host.
