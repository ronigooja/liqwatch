"""Compare surviving end-of-day map notionals using the default eight-hour window.

Run with day's roots, --date YYYY-MM-DD, --sym SYMBOL, --oi FILE --brackets CSV.
The cutoff is next UTC midnight; input trades are restricted to the requested day.
The ratio is surviving long notional / surviving short notional (nan if both zero).
"""
import argparse
from datetime import date, datetime, timedelta, timezone

import polars as pl

from wicker_research.liqcal import load_day
from wicker_research.liqmap import liquidation_map, load_brackets


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('roots', nargs='+')
    parser.add_argument('--date', required=True, type=date.fromisoformat)
    parser.add_argument('--sym', required=True)
    parser.add_argument('--oi', required=True, help='OI parquet with symbol, timestamp, sum_oi')
    parser.add_argument('--brackets', required=True)
    args = parser.parse_args(argv)
    start = int(datetime.combine(args.date, datetime.min.time(), timezone.utc).timestamp())
    end = start + int(timedelta(days=1).total_seconds())
    trades, _, _ = load_day(args.roots, args.sym)
    trades = trades.filter((pl.col('rw') >= start * 1_000_000_000)
                           & (pl.col('rw') < end * 1_000_000_000))
    samples = pl.read_parquet(args.oi)
    if not {'symbol', 'timestamp', 'sum_oi'} <= set(samples.columns):
        parser.error('OI parquet requires symbol, timestamp, sum_oi')
    if samples['symbol'].null_count() or samples.filter(pl.col('symbol') != args.sym).height:
        parser.error('OI parquet contains another symbol')
    tables = load_brackets(args.brackets)
    if args.sym not in tables:
        parser.error(f'bracket table has no symbol {args.sym}')
    oi = (samples['timestamp'].to_numpy(), samples['sum_oi'].to_numpy())
    print('mode,long_notional,short_notional,long_short_ratio')
    for mode in ('bucket', 'bucket_both'):
        result = liquidation_map(trades, end * 1_000_000_000, oi=oi, oi_mode=mode,
                                 brackets=tables[args.sym])
        long = float(result.down_notional.sum())
        short = float(result.up_notional.sum())
        ratio = long / short if short else float('inf') if long else float('nan')
        print(f'{mode},{long:.12g},{short:.12g},{ratio:.12g}')


if __name__ == '__main__':
    main()
