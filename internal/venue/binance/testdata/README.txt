Recorded 2026-11-15: first three lines per channel from
root@64.118.141.231:/root/env-sample/data/raw/binance/AAVEUSDT/20261115/.
The mark records contain one element already split from the original array; tests
reconstruct an array frame. Tests add an unknown field to each recorded payload.
Ordinary aggTrade and markPrice_all payloads retain a {"data":{...}} wrapper;
the symbol and channel are in the record envelope. Binance does not currently
emit periodic sample frames or implement adapter-level throttling.
