import glob
import json

import pytest
import zstandard

from wicker_research.replay import Replay, Stats, read_records


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
    path = write(tmp_path / 'a', [record(3), record(1), record(8, p=record(1)['p'])])
    replay = Replay([path], window_ns=4, reorder_ns=5)
    assert [e.rw for e in replay] == [1, 3, 8]
    assert replay.stats.duplicates == 0


def test_mark_expansion(tmp_path):
    data = [dict(s=s, E=2, p='1', i='2', r='0.1', T=5) for s in ('BTCUSDT', 'ETHUSDT')]
    path = write(tmp_path / 'a', [record(1, 'markPrice_all', '', p={'stream': '!markPrice@arr', 'data': data})])
    events = list(Replay([path]))
    assert [(e.sym, e.mark, e.index) for e in events] == [('BTCUSDT', 1., 2.), ('ETHUSDT', 1., 2.)]


def test_backfill(tmp_path):
    row = {'a': 1, 'T': 1, 'p': '2', 'q': '3', 'm': False}
    payload = dict(req_start_ns=1, req_end_ns=3, status=200, from_id=1, body=[row, dict(row, a=2)])
    path = write(tmp_path / 'a', [record(1), record(3, 'aggTrade_backfill', p=payload)])
    replay = Replay([path])
    events = list(replay)
    assert [(e.a, e.rw, e.backfilled) for e in events] == [(1, 1, False), (2, 3, True)]
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
    paths = glob.glob('/tmp/wicker-sample/trade-*.zst') + glob.glob('/tmp/wicker-sample/liq-*.zst')
    if len(paths) < 2:
        pytest.skip('real sample unavailable')
    replay = Replay(paths)
    count = sum(1 for _ in replay)
    assert count > 100
    assert replay.stats.order_violations == 0
    assert replay.stats.channels['aggTrade'] > 0
    assert replay.stats.channels['forceOrder_all'] > 0
