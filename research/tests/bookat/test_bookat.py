import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from wicker_research.bookat import BookView, SymbolBooks

L = pa.list_(pa.list_(pa.float64()))


def write(tmp, diffs, snaps, quality=()):
    def put(name, schema, rows, sym=True):
        d = tmp / name / 'date=20261106' / ('sym=X' if sym else '')
        d.mkdir(parents=True, exist_ok=True)
        cols = list(zip(*rows)) if rows else [[] for _ in schema]
        pq.write_table(pa.table({f.name: pa.array(cols[i], type=f.type) for i, f in enumerate(schema)}, schema=schema), d / 'p.parquet')
    put('depth', pa.schema([('rw', pa.int64()), ('E', pa.int64()), ('U', pa.int64()), ('u', pa.int64()), ('pu', pa.int64()), ('bids', L), ('asks', L)]), diffs)
    put('book_snapshots', pa.schema([('rw', pa.int64()), ('lastUpdateId', pa.int64()), ('bids', L), ('asks', L)]), snaps)
    put('quality', pa.schema([('rw', pa.int64()), ('stream', pa.string()), ('sym', pa.string()), ('start_rw', pa.int64()), ('end_rw', pa.int64()), ('kind', pa.string())]), list(quality), sym=False)


def diff(rw, U, u, pu, bids=(), asks=()):
    return (rw, 0, U, u, pu, [list(map(float, x)) for x in bids], [list(map(float, x)) for x in asks])


def snap(rw, uid, bids, asks):
    return (rw, uid, [list(map(float, x)) for x in bids], [list(map(float, x)) for x in asks])


def books(tmp, diffs, snaps, quality=()):
    write(tmp, diffs, snaps, quality)
    return SymbolBooks(str(tmp), 'X')


S1 = snap(100, 10, [(10, 5), (9, 4)], [(11, 5), (12, 6)])


def test_before_first_snapshot_unavailable_then_snapshot_state(tmp_path):
    b = books(tmp_path, [], [S1])
    assert b.book_at(50) is None
    v = b.book_at(100)
    assert v.best_bid() == (10, 5) and v.best_ask() == (11, 5) and v.mid() == 10.5


def test_diffs_applied_up_to_t_only(tmp_path):
    ds = [diff(110, 10, 12, 8, bids=[(10, 7)]), diff(120, 13, 14, 12, asks=[(11, 0)])]
    b = books(tmp_path, ds, [S1])
    assert b.book_at(115).best_bid() == (10, 7) and b.book_at(115).best_ask() == (11, 5)
    assert b.book_at(125).best_ask() == (12, 6)


def test_random_access_backward_matches_forward(tmp_path):
    ds = [diff(110, 10, 12, 8, bids=[(10, 7)]), diff(120, 13, 14, 12, asks=[(11, 0)])]
    b = books(tmp_path, ds, [S1])
    late = b.book_at(125)
    early = b.book_at(105)
    again = b.book_at(125)
    assert early.best_bid() == (10, 5) and late.asks == again.asks and late.bids == again.bids


def test_stale_diffs_before_snapshot_id_are_dropped(tmp_path):
    ds = [diff(90, 5, 8, 4, bids=[(10, 99)]), diff(105, 9, 12, 8, bids=[(10, 6)])]
    b = books(tmp_path, ds, [S1])
    assert b.book_at(110).best_bid() == (10, 6)


def test_sequence_break_makes_book_unavailable_until_next_snapshot(tmp_path):
    ds = [diff(110, 10, 12, 8, bids=[(10, 7)]), diff(120, 20, 22, 18, bids=[(10, 1)]), diff(160, 31, 32, 30)]
    s2 = snap(150, 30, [(8, 3)], [(13, 3)])  # the first diff after it (U=31) does not contain 30: wait for pu chain
    ds[2] = diff(160, 29, 32, 28)
    b = books(tmp_path, ds, [S1, s2])
    assert b.book_at(115) is not None
    assert b.book_at(130) is None
    v = b.book_at(155)
    assert v.best_bid() == (8, 3)
    assert b.book_at(165).update_id == 32


def test_quality_interval_blocks_access(tmp_path):
    b = books(tmp_path, [diff(110, 10, 12, 8)], [S1], [(0, 'depth', 'X', 105, 140, 'unavailable')])
    assert b.book_at(120) is None
    assert b.book_at(104) is not None


def test_query_order_does_not_change_the_answer(tmp_path):
    ds = [diff(110, 10, 12, 8, bids=[(10, 7)]), diff(120, 13, 14, 12, asks=[(11, 0)]), diff(130, 14, 16, 14, bids=[(9, 1)])]
    s2 = snap(125, 14, [(10, 7), (9, 4)], [(12, 6)])
    b = books(tmp_path, ds, [S1, s2])
    times = [105, 118, 127, 135, 112, 122, 131, 101]
    seq = [b.book_at(t) for t in times]
    for t, got in zip(times, seq):
        fresh = SymbolBooks(str(tmp_path), 'X').book_at(t)
        assert (got is None) == (fresh is None), t
        if got is not None:
            assert (got.bids, got.asks) == (fresh.bids, fresh.asks), t


def test_walk_market_order():
    v = BookView(0, 'X', [(10, 5), (9, 4)], [(11, 5), (12, 6)], 1)
    qty, avg, short = v.walk('buy', qty=7)
    assert qty == 7 and not short and abs(avg - (5 * 11 + 2 * 12) / 7) < 1e-12
    qty, avg, short = v.walk('buy', notional_usd=55)
    assert abs(qty - 5) < 1e-9 and avg == 11 and not short
    qty, avg, short = v.walk('sell', qty=20)
    assert qty == 9 and short
    assert v.walk('sell', notional_usd=1e9)[2] is True


@pytest.mark.real
def test_real_book_matches_verification_snapshots():
    """At each snapshot's receive time the rebuilt book must equal the snapshot on the top 20 levels."""
    root = '/tmp/wicker-pq2'
    if not os.path.exists(root + '/depth'):
        pytest.skip('converted sample absent')
    import pyarrow.dataset as ds
    total_checked = total_bad = 0
    for sym in ('FLOCKUSDT', 'XANUSDT', 'BTCUSDT'):
        b = SymbolBooks(root, sym)
        s = ds.dataset(root + '/book_snapshots', partitioning='hive').to_table(filter=ds.field('sym') == sym).sort_by('rw').to_pydict()
        for i, rw in enumerate(s['rw']):
            v = b.book_at(rw)
            if v is None:
                continue
            want_b = dict((p, q) for p, q in s['bids'][i][:20])
            want_a = dict((p, q) for p, q in s['asks'][i][:20])
            have_b, have_a = dict(v.bids), dict(v.asks)
            bad = sum(have_b.get(p) != q for p, q in want_b.items()) + sum(have_a.get(p) != q for p, q in want_a.items())
            total_checked += 1
            total_bad += bad > 0
    assert total_checked >= 15
    assert total_bad == 0
