import json
import os

import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest
import zstandard

from wicker_research.convert import PARSER_VERSION, convert

DAY = 1_793_923_200_000_000_000  # 2026-11-06 00:00:00 UTC in ns


def raw(path, records):
    with open(path, 'wb') as f, zstandard.ZstdCompressor().stream_writer(f) as w:
        for r in records:
            w.write((json.dumps(r) + '\n').encode())


def rec(ch, sym, rw, data, conn='c1', seq=1):
    return {'v': 1, 'wseq': seq, 'conn': conn, 'cseq': seq, 'rw': rw, 'rm': rw, 'ch': ch, 'sym': sym,
            'p': {'stream': f'{sym.lower()}@x', 'data': data}}


def trade(a, rw, sym='BTCUSDT', conn='c1'):
    return rec('aggTrade', sym, rw, {'e': 'aggTrade', 'E': 1, 'a': a, 's': sym, 'p': '100.5', 'q': '2', 'T': 7, 'm': True}, conn, a)


def test_layout_columns_dedup_and_parser_version(tmp_path):
    raw(tmp_path / 'trade-a.jsonl.zst', [trade(1, DAY + 10), trade(2, DAY + 20), trade(1, DAY + 30, conn='c2')])
    raw(tmp_path / 'trade-b.jsonl.zst', [trade(2, DAY + 25, conn='c2'), trade(3, DAY + 40, sym='ETHUSDT')])
    written, stats, _ = convert([tmp_path / 'trade-a.jsonl.zst', tmp_path / 'trade-b.jsonl.zst'], tmp_path / 'out')
    assert written == {'trades': 3} and stats.duplicates == 2
    t = ds.dataset(tmp_path / 'out' / 'trades', partitioning='hive').to_table()
    assert sorted(t['a'].to_pylist()) == [1, 2, 3]
    assert set(t.column_names) >= {'rw', 'T', 'a', 'price', 'qty', 'buyer_is_maker', 'backfilled', 'date', 'sym'}
    assert (tmp_path / 'out' / 'trades' / 'date=20261106' / 'sym=BTCUSDT').is_dir()
    f = next((tmp_path / 'out' / 'trades').rglob('*.parquet'))
    assert pq.read_schema(f).metadata[b'wicker_parser'].decode() == PARSER_VERSION
    assert pq.read_table(f)['price'].type == 'double'


def test_midnight_split_and_markprice_expansion(tmp_path):
    entry = lambda s, e=5: {'e': 'markPriceUpdate', 'E': e, 's': s, 'p': '1.5', 'i': '1.4', 'r': '0.0001', 'T': 99}
    raw(tmp_path / 'm.jsonl.zst', [
        rec('markPrice_all', '', DAY - 5, [entry('AAAUSDT'), entry('BBBUSDT')]),
        rec('markPrice_all', '', DAY + 5, [entry('AAAUSDT', 6)]),
    ])
    written, _, _ = convert([tmp_path / 'm.jsonl.zst'], tmp_path / 'out')
    assert written == {'mark': 3}
    assert (tmp_path / 'out' / 'mark' / 'date=20261105' / 'sym=BBBUSDT').is_dir()
    assert (tmp_path / 'out' / 'mark' / 'date=20261106' / 'sym=AAAUSDT').is_dir()


def test_liquidations_quality_and_depth_lists(tmp_path):
    liq = rec('forceOrder_all', '', DAY + 1, {'e': 'forceOrder', 'E': 3, 'o': {'s': 'XUSDT', 'S': 'BUY', 'p': '2', 'ap': '2.1', 'q': '5', 'z': '4', 'X': 'FILLED'}})
    depth = rec('depth', 'XUSDT', DAY + 2, {'E': 1, 'U': 5, 'u': 6, 'pu': 4, 'b': [['1.0', '2']], 'a': [['1.1', '3']]})
    drop = {'v': 1, 'wseq': 1, 'conn': 'writer', 'cseq': 0, 'rw': DAY + 9, 'rm': 1, 'ch': 'gap', 'sym': '',
            'p': {'event': 'drop', 'channel': 'bookTicker', 'count': 4, 'since_ns': DAY + 3}}
    raw(tmp_path / 'x.jsonl.zst', [liq, depth, drop])
    written, _, _ = convert([tmp_path / 'x.jsonl.zst'], tmp_path / 'out')
    assert written == {'liquidations': 1, 'depth': 1, 'quality': 1}
    q = ds.dataset(tmp_path / 'out' / 'quality', partitioning='hive').to_table().to_pylist()[0]
    assert (q['kind'], q['stream'], q['start_rw'], q['end_rw']) == ('drop', 'bookTicker', DAY + 3, DAY + 9)
    d = ds.dataset(tmp_path / 'out' / 'depth', partitioning='hive').to_table().to_pylist()[0]
    assert d['bids'] == [[1.0, 2.0]] and d['asks'] == [[1.1, 3.0]]
    l = ds.dataset(tmp_path / 'out' / 'liquidations', partitioning='hive').to_table().to_pylist()[0]
    assert l['sym'] == 'XUSDT' and l['filled_qty'] == 4.0


def test_output_is_deterministic(tmp_path):
    raw(tmp_path / 't.jsonl.zst', [trade(i, DAY + i * 10) for i in range(1, 30)])
    for name in ('o1', 'o2'):
        convert([tmp_path / 't.jsonl.zst'], tmp_path / name)
    names = lambda n: sorted(str(p.relative_to(tmp_path / n)) for p in (tmp_path / n).rglob('*.parquet'))
    assert names('o1') == names('o2')
    f1, f2 = (next((tmp_path / n).rglob('*.parquet')) for n in ('o1', 'o2'))
    assert pq.read_table(f1).equals(pq.read_table(f2))


@pytest.mark.real
def test_real_liq_and_depth(tmp_path):
    base = '/tmp/wicker-sample/'
    if not os.path.exists(base + 'liq-2026110617.b.jsonl.zst'):
        pytest.skip('sample absent')
    written, stats, _ = convert([base + 'liq-2026110617.b.jsonl.zst', base + 'depth-2026110617.b.jsonl.zst'], tmp_path)
    assert written['liquidations'] > 1000 and written['depth'] > 100000 and written['book_snapshots'] > 100
    assert stats.order_violations == 0
