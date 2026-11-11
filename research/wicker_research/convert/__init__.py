"""Replay output to partitioned Parquet. Layout: docs/p1-data-pipeline.md (Columnar layout)."""

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from wicker_research.replay import Replay

PARSER_VERSION = 'replay-1'

_LEVELS = pa.list_(pa.list_(pa.float64()))
# kind -> (dataset, partition by symbol, columns in order)
DATASETS = {
    'aggTrade': ('trades', True, [('T', pa.int64()), ('a', pa.int64()), ('price', pa.float64()), ('qty', pa.float64()),
                                  ('buyer_is_maker', pa.bool_()), ('backfilled', pa.bool_())]),
    'markPrice': ('mark', True, [('E', pa.int64()), ('mark', pa.float64()), ('index', pa.float64()),
                                 ('funding_rate', pa.float64()), ('next_funding_T', pa.int64())]),
    'bookTicker': ('quotes', True, [('T', pa.int64()), ('u', pa.int64()), ('bid', pa.float64()), ('bid_qty', pa.float64()),
                                    ('ask', pa.float64()), ('ask_qty', pa.float64())]),
    'depth': ('depth', True, [('E', pa.int64()), ('U', pa.int64()), ('u', pa.int64()), ('pu', pa.int64()),
                              ('bids', _LEVELS), ('asks', _LEVELS)]),
    'snapshot': ('book_snapshots', True, [('lastUpdateId', pa.int64()), ('bids', _LEVELS), ('asks', _LEVELS)]),
    'forceOrder': ('liquidations', False, [('E', pa.int64()), ('sym', pa.string()), ('side', pa.string()),
                                           ('price', pa.float64()), ('avg_price', pa.float64()), ('qty', pa.float64()),
                                           ('filled_qty', pa.float64()), ('status', pa.string())]),
    'gap': ('quality', False, [('stream', pa.string()), ('sym', pa.string()), ('start_rw', pa.int64()),
                               ('end_rw', pa.int64()), ('kind', pa.string())]),
    'drop': ('quality', False, [('stream', pa.string()), ('sym', pa.string()), ('start_rw', pa.int64()),
                                ('end_rw', pa.int64()), ('kind', pa.string())]),
}
for _channel in ('brackets', 'exchangeInfo', 'constituents', 'openInterest', 'brackets_error'):
    DATASETS[_channel] = ('poll_' + _channel, True,
                          [('payload_json', pa.string()), ('meta_json', pa.string())])


def _schema(kind):
    name, _, cols = DATASETS[kind]
    return name, pa.schema([('rw', pa.int64())] + cols, metadata={'wicker_parser': PARSER_VERSION, 'dataset': name})


def _row(kind, ev):
    f = ev.fields
    if kind in ('brackets', 'exchangeInfo', 'constituents', 'openInterest', 'brackets_error'):
        return [ev.rw, json.dumps(f['payload'], sort_keys=True), json.dumps(f['meta'], sort_keys=True)]
    if kind in ('gap', 'drop'):
        return [ev.rw, f['stream'], ev.sym, f['start_rw'], f['end_rw'], kind]
    if kind == 'forceOrder':
        return [ev.rw] + [ev.sym if c == 'sym' else f[c] for c, _ in DATASETS[kind][2]]
    return [ev.rw] + [f[c] for c, _ in DATASETS[kind][2]]


def _date(rw):
    return datetime.fromtimestamp(rw // 1_000_000_000, tz=timezone.utc).strftime('%Y%m%d')


class Writer:
    """Buffers rows per (dataset, date, symbol) and writes one Parquet file per flush."""

    def __init__(self, out, cap_rows=6_000_000):
        self.out = Path(out)
        self.cap = cap_rows
        self.buf = defaultdict(list)
        self.rows = 0
        self.written = defaultdict(int)
        self.files = 0

    def add(self, ev):
        kind = 'gap' if ev.kind == 'gap' else ev.kind
        if kind not in DATASETS:
            return
        name, by_sym, _ = DATASETS[kind]
        self.buf[(kind, _date(ev.rw), ev.sym if by_sym else '')].append(_row(kind, ev))
        self.rows += 1
        if self.rows >= self.cap:
            self.flush()

    def _write(self, key):
        kind, date, sym = key
        rows = self.buf.pop(key)
        name, schema = _schema(kind)
        cols = list(zip(*rows))
        table = pa.table({f.name: pa.array(cols[i], type=f.type) for i, f in enumerate(schema)}, schema=schema)
        table = table.sort_by('rw')
        d = self.out / name / f'date={date}'
        if sym:
            d = d / f'sym={sym}'
        d.mkdir(parents=True, exist_ok=True)
        first = table['rw'][0].as_py()
        pq.write_table(table, d / f'part-{first}-{len(rows)}.parquet', compression='zstd', row_group_size=250_000)
        self.written[name] += len(rows)
        self.rows -= len(rows)
        self.files += 1

    def flush(self, partial=False):
        """Write everything, or with partial=True only the largest partitions until half the buffer is gone,
        so memory stays bounded while files stay large."""
        target = self.cap // 2 if partial else 0
        for key in sorted(self.buf, key=lambda k: -len(self.buf[k])):
            if self.rows <= target:
                break
            self._write(key)


def convert(paths, out, **replay_kwargs):
    replay = Replay(paths, **replay_kwargs)
    w = Writer(out)
    for ev in replay:
        w.add(ev)
    w.flush()
    return dict(w.written), replay.stats, w.files
