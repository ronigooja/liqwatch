import argparse
import re
from datetime import datetime, timezone
from pathlib import Path

import requests

from . import fetch_symbol, list_symbols, missing_steps


def _date(value):
    return int(datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)


def _stamp(value):
    if value is None:
        return "none"
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description="Fetch public Binance open-interest history (UTC, exclusive end).")
    parser.add_argument("--syms", required=True)
    parser.add_argument("--start", required=True, type=_date)
    parser.add_argument("--end", required=True, type=_date)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if args.start >= args.end:
        parser.error("--start must precede --end")
    with requests.Session() as session:
        symbols = list_symbols(session) if args.syms == "all-usdt-perp" else list(dict.fromkeys(
            symbol.strip().upper() for symbol in args.syms.split(",")))
        if not symbols or any(not re.fullmatch(r"[A-Z0-9_]+", symbol) for symbol in symbols):
            parser.error("--syms must contain comma-separated symbols or all-usdt-perp")
        args.out.mkdir(parents=True, exist_ok=True)
        for symbol in symbols:
            frame = fetch_symbol(session, symbol, args.start, args.end)
            frame.write_parquet(args.out / f"oi_{symbol}.parquet")
            print(f"{symbol}: rows={len(frame)} first={_stamp(frame['timestamp'].min())} "
                  f"last={_stamp(frame['timestamp'].max())} "
                  f"missing_5m_steps={missing_steps(frame, args.start, args.end)}", flush=True)


if __name__ == "__main__":
    main()
