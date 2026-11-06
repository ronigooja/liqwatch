"""Read raw sample directly for exploratory checks, not a replacement for replay."""
import argparse
import json

import polars as pl
import zstandard

from . import HORIZONS, labels


def read_raw(path, symbols):
    rows = {sym: [] for sym in symbols}
    with open(path, 'rb') as source, zstandard.ZstdDecompressor().stream_reader(source) as stream:
        import io
        for line in io.TextIOWrapper(stream, encoding='utf-8'):
            record = json.loads(line)
            if record['ch'] == 'aggTrade' and record['sym'] in rows:
                data = record['p']['data']
                rows[record['sym']].append((record['rw'], float(data['p']), float(data['q']), data['m'], data['a']))
    result = {}
    for sym, values in rows.items():
        if not values:
            raise ValueError(f'no trades for {sym}')
        result[sym] = pl.DataFrame(values, schema=['rw', 'price', 'qty', 'buyer_is_maker', 'a'], orient='row').sort('rw', maintain_order=True).unique(subset=['a'], keep='first', maintain_order=True)
    return result


def report(frame):
    for h in HORIZONS:
        print(f'h={h} valid={frame[f"valid_{h}"].sum()}', end='')
        for col in ('risk_up', 'risk_down', 'refresh', 'ambiguous'):
            value = frame[f'{col}_{h}'].mean()
            print(f' {col}={value:.6f}' if value is not None else f' {col}=NA', end='')
        for value in ('extension', 'retrace', 'neither'):
            col = frame[f'first_touch_{h}'].drop_nulls()
            rate = (col == value).mean() if len(col) else None
            print(f' {value}={rate:.6f}' if rate is not None else f' {value}=NA', end='')
        print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trades', required=True)
    parser.add_argument('--sym', required=True)
    parser.add_argument('--k', type=float, default=3)
    args = parser.parse_args()
    report(labels(read_raw(args.trades, [args.sym])[args.sym], k=args.k))


if __name__ == '__main__':
    main()
