import json
import os

import pytest
import zstandard

from liqwatch_research.book import OrderBook, Validator


def diff(U, u, pu, bids=None, rw=None):
    return {'ch': 'depth', 'sym': 'X', 'rw': rw or u, 'p': {'data': {'U': U, 'u': u, 'pu': pu, 'b': bids or [], 'a': []}}}


def snap(uid, bids=None, rw=None):
    return {'ch': 'snapshot', 'sym': 'X', 'rw': rw or uid, 'p': {'status': 200, 'body': {'lastUpdateId': uid, 'bids': bids if bids is not None else [['10', '1']], 'asks': [['11', '2']]}}}


def test_clean_stale_removal_and_verify():
    v = Validator()
    v.feed(diff(1, 2, 0))
    v.feed(diff(3, 5, 2, [['10', '0'], ['9', '4']]))
    v.feed(snap(4, rw=6))
    v.feed(snap(5, [['9', '4']], rw=7))
    v.finish()
    assert v.states['X']['book'].best_bid() == (9., 4.)
    assert v.states['X']['book'].known_depth['bids'] == 10.
    assert v.summary()['X']['diffs'] == 1
    assert v.summary()['X']['passed'] == 2


def test_break_and_resync():
    v = Validator()
    v.feed(snap(5))
    v.feed(diff(5, 6, 4))
    v.feed(diff(7, 8, 1))
    v.feed(snap(8, rw=9))
    v.feed(diff(8, 9, 8, rw=10))
    v.finish()
    assert v.summary()['X']['breaks'] == 1
    assert len([e for e in v.events if e['kind'] == 'resync_done']) == 2


def test_verify_fail_and_parquet(tmp_path):
    v = Validator()
    v.feed(snap(5))
    v.feed(diff(5, 6, 4))
    v.feed(snap(6, [['10', '3']]))
    v.finish()
    assert v.summary()['X']['passed'] == 0
    assert v.summary()['X']['max_mismatch'] == 1
    v.write_parquet(tmp_path / 'q.parquet')


def test_book_removal_missing_and_imbalance():
    b = OrderBook({'bids': [['10', '2']], 'asks': [['11', '2']]})
    b.apply({'b': [['9', '0']], 'a': []})
    assert b.best_ask() == (11, 2)
    assert b.imbalance(1) == 0


def test_snapshot_inside_a_diff_is_checked_between_before_and_after():
    v = Validator()
    v.feed(snap(5, [['10', '1'], ['8', '5']]))
    v.feed(diff(5, 10, 4, [['10', '3'], ['9', '1']]))
    v.feed(snap(8, [['10', '2'], ['9', '1'], ['8', '5']]))
    v.feed(diff(11, 12, 10, [['9', '2']]))
    v.finish()
    r = v.summary()['X']
    assert r['inside'] == 2 and r['inside_passed'] == 2
    w = Validator()
    w.feed(snap(5, [['10', '1'], ['8', '5']]))
    w.feed(diff(5, 10, 4, [['10', '3'], ['9', '1']]))
    w.feed(snap(8, [['10', '3'], ['9', '1'], ['8', '9']]))
    w.feed(diff(11, 12, 10, [['9', '2']]))
    w.finish()
    assert w.summary()['X']['inside_passed'] == 0


@pytest.mark.real
def test_real_sample():
    path = '/tmp/liqwatch-sample/depth-2026110617.b.jsonl.zst'
    if not os.path.exists(path):
        pytest.skip('sample absent')
    v = Validator()
    with zstandard.open(path, 'rt') as f:
        for line in f:
            v.feed(json.loads(line))
    assert len(v.finish()) == 15
