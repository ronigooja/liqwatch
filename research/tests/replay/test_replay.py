import glob
import json

import pytest
import zstandard

from liqwatch_research.replay import Replay, Stats, _IdSet, read_records


def record(rw, ch='aggTrade', sym='BTCUSDT', conn='a', p=None):
    if p is None:
        p = {'stream': 'btcusdt@aggTrade', 'data': {'T': 1, 'a': rw, 'p': '2', 'q': '3', 'm': False}}
    return dict(v=1, wseq=rw, conn=conn, cseq=rw, rw=rw, rm=rw, ch=ch, sym=sym, p=p)


def write(path, records, tail=b''):
    raw = b''.join(json.dumps(r).encode() + b'\n' for r in records) + tail
    path.write_bytes(zstandard.ZstdCompressor().compress(raw))
    return path


def test_overlap_and_order(tmp_path):
    a = write(tmp_path / 'a', [record(1), record(3)])
    duplicate = record(2, p=record(1)['p'])
    b = write(tmp_path / 'b', [duplicate, record(4)])
    replay = Replay([a, b])
    events = list(replay)
    assert [e.rw for e in events] == [1, 3, 4]
    assert replay.stats.duplicates == 1
    assert replay.stats.order_violations == 0


def test_wall_time_reordering_and_expiry(tmp_path):
    # depth diffs are never backfilled, so their dedup keys still expire
    def diff(rw, u):
        return record(rw, 'depth', p={'stream': 'btcusdt@depth', 'data': dict(E=1, U=1, u=u, pu=0, b=[], a=[])})
    path = write(tmp_path / 'a', [diff(3, 7), diff(1, 2), diff(8, 2)])
    replay = Replay([path], window_ns=4, reorder_ns=5)
    assert [e.rw for e in replay] == [1, 3, 8]
    assert replay.stats.duplicates == 0


def test_mark_expansion(tmp_path):
    data = [dict(s=s, E=2, p='1', i='2', r='0.1', T=5) for s in ('BTCUSDT', 'ETHUSDT')]
    path = write(tmp_path / 'a', [record(1, 'markPrice_all', '', p={'stream': '!markPrice@arr', 'data': data})])
    events = list(Replay([path]))
    assert [(e.sym, e.mark, e.index) for e in events] == [('BTCUSDT', 1., 2.), ('ETHUSDT', 1., 2.)]


def test_compact_plain_and_legacy_compressed(tmp_path):
    root = tmp_path / 'binance' / 'BTCUSDT' / '20260101'
    root.mkdir(parents=True)
    path = root / 'aggTrade.a.jsonl'
    path.write_text('{"v":2,"wseq":1,"cseq":2,"rw":7,"rm":3,"p":{"data":{"T":5,"a":42,"p":"2","q":"3","m":false}}}\n')
    assert list(read_records([path])) == [
        {'v': 2, 'wseq': 1, 'cseq': 2, 'rw': 7, 'rm': 3,
         'venue': 'binance', 'ch': 'aggTrade', 'sym': 'BTCUSDT',
         'p': {'data': {'T': 5, 'a': 42, 'p': '2', 'q': '3', 'm': False}}}]
    events = list(Replay([path]))
    assert [(e.rw, e.sym, e.a, e.price, e.qty) for e in events] == [(7, 'BTCUSDT', 42, 2., 3.)]
    legacy = write(tmp_path / 'legacy', [record(9)])
    assert list(read_records([legacy])) == [record(9)]


def test_compact_mark_uses_path_symbol(tmp_path):
    root = tmp_path / 'binance' / 'BTCUSDT' / '20260101'
    root.mkdir(parents=True)
    path = root / 'markPrice_all.a.jsonl'
    path.write_text('{"v":2,"rw":7,"rm":3,"p":{"data":{"E":2,"p":"1","i":"2","r":"0.1","T":5}}}\n')
    assert [(e.sym, e.mark) for e in Replay([path])] == [('BTCUSDT', 1.)]


def test_compact_gap_and_bulk_payload_identity(tmp_path):
    bulk = tmp_path / 'kucoin' / '_all' / '20260101' / 'contracts.p.jsonl'
    bulk.parent.mkdir(parents=True)
    bulk.write_text('{"v":2,"rw":1,"rm":0,"meta":{"snapshot":true},"p":{"data":[{"symbol":"XBTUSDTM"},{"symbol":"ETHUSDTM"}]}}\n')
    assert list(read_records([bulk])) == [
        {'v': 2, 'rw': 1, 'rm': 0, 'meta': {'snapshot': True},
         'venue': 'kucoin', 'ch': 'contracts', 'sym': '',
         'p': {'data': [{'symbol': 'XBTUSDTM'}, {'symbol': 'ETHUSDTM'}]}}]
    gap = tmp_path / 'binance' / 'gap' / '20260101' / 'gap.a.jsonl'
    gap.parent.mkdir(parents=True)
    gap.write_text('{"v":2,"rw":4,"rm":0,"p":{"event":"conn_closed","streams":["btcusdt@aggTrade"]}}\n')
    trades = tmp_path / 'binance' / 'BTCUSDT' / '20260101' / 'aggTrade.a.jsonl'
    trades.parent.mkdir(parents=True)
    trades.write_text(''.join(json.dumps({'v': 2, 'rw': rw, 'rm': rw,
        'p': {'stream': 'btcusdt@aggTrade', 'data': {'T': rw, 'a': rw, 'p': '2', 'q': '3', 'm': False}}}) + '\n'
        for rw in (2, 6)))
    events = list(Replay([gap, trades]))
    holes = [e for e in events if e.kind == 'gap']
    assert [(e.sym, e.stream, e.start_rw, e.end_rw) for e in holes] == [('BTCUSDT', 'btcusdt@aggTrade', 2, 6)]


def test_backfill(tmp_path):
    row = {'a': 1, 'T': 1, 'p': '2', 'q': '3', 'm': False}
    payload = dict(req_start_ns=1, req_end_ns=3, status=200, from_id=1, body=[row, dict(row, a=2)])
    path = write(tmp_path / 'a', [record(1), record(3, 'aggTrade_backfill', p=payload)])
    replay = Replay([path])
    events = list(replay)
    assert [(e.a, e.rw, e.backfilled) for e in events] == [(1, 1, False), (2, 3, True)]
    assert replay.stats.duplicates == 1


def test_backfill_twin_far_outside_window_is_deduplicated(tmp_path):
    # Backfill fills a hole, so its live twin can have arrived much longer ago than the dedup window.
    row = {'a': 5, 'T': 1, 'p': '2', 'q': '3', 'm': False}
    live = record(10, p={'stream': 'btcusdt@aggTrade', 'data': dict(row)})
    payload = dict(req_start_ns=1, req_end_ns=1000, status=200, from_id=5, body=[dict(row), dict(row, a=6)])
    path = write(tmp_path / 'a', [live, record(1000, 'aggTrade_backfill', p=payload)])
    replay = Replay([path], window_ns=100, reorder_ns=5)
    events = list(replay)
    assert [(e.a, e.rw, e.backfilled) for e in events] == [(5, 10, False), (6, 1000, True)]
    assert replay.stats.duplicates == 1


def test_live_duplicate_far_outside_window_is_deduplicated(tmp_path):
    path = write(tmp_path / 'a', [record(10), record(1000, p=record(10)['p'])])
    replay = Replay([path], window_ns=100, reorder_ns=5)
    assert [e.rw for e in replay] == [10]
    assert replay.stats.duplicates == 1


def test_gap_and_drop(tmp_path):
    close = record(2, 'gap', '', p=dict(event='conn_closed', streams=['btcusdt@aggTrade']))
    drop = record(4, 'gap', '', p=dict(event='drop', channel='aggTrade', count=2, since_ns=1))
    path = write(tmp_path / 'a', [record(1), close, record(3, conn='b'), drop])
    events = list(Replay([path]))
    gap, = [e for e in events if e.kind == 'gap']
    assert (gap.start_rw, gap.end_rw, gap.sym) == (1, 3, 'BTCUSDT')
    drop_event, = [e for e in events if e.kind == 'drop']
    assert (drop_event.start_rw, drop_event.end_rw, drop_event.count) == (1, 4, 2)


def test_torn_tail(tmp_path):
    path = write(tmp_path / 'a', [record(1)], b'{"v":')
    stats = Stats()
    assert len(list(read_records([path], stats))) == 1
    assert stats.torn_lines == 1


@pytest.mark.real
def test_real():
    paths = glob.glob('/tmp/liqwatch-sample/trade-*.zst') + glob.glob('/tmp/liqwatch-sample/liq-*.zst')
    if len(paths) < 2:
        pytest.skip('real sample unavailable')
    replay = Replay(paths)
    count = sum(1 for _ in replay)
    assert count > 100
    assert replay.stats.order_violations == 0
    assert replay.stats.channels['aggTrade'] > 0
    assert replay.stats.channels['forceOrder_all'] > 0


def test_idset_membership_and_merging():
    s = _IdSet()
    for n in (5, 7, 6, 1, 2, 10, 3):
        s.add(n)
    assert [n for n in range(0, 12) if n in s] == [1, 2, 3, 5, 6, 7, 10]
    assert (s.starts, s.ends) == ([1, 5, 10], [3, 7, 10])
    s.add(4)
    s.add(8); s.add(9)
    assert (s.starts, s.ends) == ([1], [10])
    s.add(5)
    assert (s.starts, s.ends) == ([1], [10])


def test_idset_memory_is_proportional_to_holes():
    s = _IdSet()
    for n in range(1, 100_001):
        if n % 1000:
            s.add(n)
    assert len(s.starts) == 100
