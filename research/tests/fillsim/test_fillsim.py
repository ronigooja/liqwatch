import numpy as np
import pytest

from wicker_research.bookat import BookView
from wicker_research.fillsim import SECOND, Simulator, run_grid

MS = 1_000_000


class FakeBooks:
    """Piecewise-constant books: list of (from_rw, BookView-args)."""

    def __init__(self, states, diffs=None, unavail=()):
        self.states = sorted(states)
        self.diffs = diffs or {'bids': [], 'asks': []}
        self.diff_rw = diffs['rw'] if diffs else []
        self.unavail = list(unavail)

    def book_at(self, t):
        cur = None
        for rw, bids, asks in self.states:
            if rw <= t:
                cur = (bids, asks)
        if cur is None or any(s <= t < e for s, e in self.unavail):
            return None
        return BookView(t, 'X', list(cur[0]), list(cur[1]), 1)


def trades(rows):
    r = list(zip(*sorted(rows)))
    return {'rw': r[0], 'price': r[1], 'qty': r[2], 'buyer_is_maker': r[3]}


BOOK = [(0, [(100.0, 10), (99.0, 10)], [(101.0, 10), (102.0, 10)])]


def sim(states=BOOK, tr=((0, 100.0, 0.0, True),), **kw):
    return Simulator(FakeBooks(states, **kw), trades(tr), kw.get('mark'))


def test_market_walks_book_after_latency_and_charges_taker_fee():
    s = sim([(0, [(100.0, 10)], [(101.0, 3)]), (1000 * MS, [(100.0, 10)], [(101.0, 3), (110.0, 10)])])
    f = s.market(900 * MS, 'buy', qty=5, latency_ns=200 * MS, taker_fee=0.001)
    # executes at 1100 ms: reads the second book (3 @ 101, then 2 @ 110), not the earlier one
    assert f.outcome == 'filled' and f.fill_rw == 1100 * MS
    assert f.price == pytest.approx((3 * 101 + 2 * 110) / 5)
    assert f.fee_usd == pytest.approx(0.001 * 5 * f.price)
    assert f.slip_bp == pytest.approx((f.price - 100.5) / 100.5 * 1e4)  # mid at signal time (first book)


def test_market_rejects_when_book_too_shallow_or_unavailable():
    s = sim()
    assert s.market(10, 'buy', qty=500).reason == 'book_too_shallow'
    assert s.market(10, 'buy', qty=500).outcome == 'rejected'
    gap = sim(unavail=[(0, 10 * SECOND)])
    assert gap.market(SECOND, 'buy', qty=1).reason == 'book_unavailable'


def test_market_flags_impact_for_large_size():
    s = sim()
    assert s.market(10, 'buy', size_usd=50).impact is False
    assert s.market(10, 'buy', size_usd=500).impact is True  # > 10% of ~1010 usd within 10 bp


def test_limit_buy_needs_sell_aggressor_trades_and_queue_is_consumed_first():
    tr = [(10, 100.0, 4.0, True), (20, 100.0, 4.0, True), (30, 100.0, 4.0, True)]
    s = sim(tr=tr)
    # queue ahead 10 at 100; want 1 unit (100 usd). trades at the price eat 4 + 4 of the queue, then 2 left
    # of the third trade's 4 -> ahead is 10-4-4-4 = -2 -> remaining capacity 2 fills us 1 unit.
    f = s.limit(0, 'buy', 100.0, 100.0, latency_ns=0, ttl_ns=100)
    assert f.outcome == 'filled' and f.qty == pytest.approx(1.0) and f.fill_rw == 30
    assert f.fee_usd == pytest.approx(0.0002 * 100.0)


def test_limit_not_filled_by_wrong_aggressor_or_before_queue_clears():
    wrong = sim(tr=[(10, 100.0, 50.0, False)])  # buy aggressor cannot fill a resting buy
    assert wrong.limit(0, 'buy', 100.0, 100.0, latency_ns=0, ttl_ns=100).reason == 'no_fill_in_ttl'
    small = sim(tr=[(10, 100.0, 3.0, True)])  # eats only part of the 10-unit queue
    assert small.limit(0, 'buy', 100.0, 100.0, latency_ns=0, ttl_ns=100).outcome == 'missed'


def test_limit_filled_in_full_by_trade_through_the_price():
    s = sim(tr=[(10, 99.5, 5.0, True)])  # 5 units trade through; we want 2
    f = s.limit(0, 'buy', 200.0, 100.0, latency_ns=0, ttl_ns=100)  # 2 units wanted
    assert f.outcome == 'filled' and f.qty == pytest.approx(2.0)


def test_limit_partial_and_marketable_and_ttl():
    s = sim(tr=[(10, 99.5, 1.0, True)])
    f = s.limit(0, 'buy', 300.0, 100.0, latency_ns=0, ttl_ns=100)  # wants 3, only 1 trades through
    assert f.outcome == 'partial' and f.qty == pytest.approx(1.0)
    assert s.limit(0, 'buy', 100.0, 101.0, latency_ns=0).reason == 'marketable'
    late = sim(tr=[(500, 99.5, 5.0, True)])
    assert late.limit(0, 'buy', 100.0, 100.0, latency_ns=0, ttl_ns=100).outcome == 'missed'


def test_optimistic_bound_shrinks_queue_with_displayed_size():
    diffs = {'rw': [5], 'bids': [[[100.0, 2.0]]], 'asks': [[]]}  # displayed size at 100 drops 10 -> 2
    tr = [(10, 100.0, 3.0, True)]
    pess = sim(tr=tr, diffs=diffs).limit(0, 'buy', 100.0, 100.0, latency_ns=0, ttl_ns=100, bound='pessimistic')
    opt = sim(tr=tr, diffs=diffs).limit(0, 'buy', 100.0, 100.0, latency_ns=0, ttl_ns=100, bound='optimistic')
    assert pess.outcome == 'missed'
    # queue 10 -> 2 after the cancel; a 3-unit trade eats 2 and leaves 1 for us
    assert opt.outcome == 'filled' and opt.qty == pytest.approx(1.0)


def test_stop_triggers_on_mark_and_closes_after_latency():
    mark = {'rw': [0, 1000 * MS, 2000 * MS, 3000 * MS], 'mark': [100.0, 99.0, 97.0, 96.0],
            'funding_rate': [0.0] * 4, 'next_funding_T': [10 ** 15] * 4}
    states = [(0, [(100.0, 10)], [(101.0, 10)]), (2400 * MS, [(95.0, 10)], [(96.0, 10)])]
    s = Simulator(FakeBooks(states), trades([(0, 100.0, 0.0, True)]), mark)
    out = s.stop(0, 'long', 98.0, 2.0, latency_ns=500 * MS)
    fill, trig = out
    assert trig == 2000 * MS and fill.fill_rw == 2500 * MS and fill.kind == 'stop'
    assert fill.price == pytest.approx(95.0)  # sells into the book after the gap down, not at the stop price
    assert s.stop(0, 'long', 50.0, 1.0) is None


def test_funding_sign_and_window():
    mark = {'rw': [0, 10 * SECOND], 'mark': [100.0, 100.0], 'funding_rate': [0.001, 0.001],
            'next_funding_T': [5000, 5000]}  # funding timestamp at 5 s (ms)
    s = Simulator(FakeBooks(BOOK), trades([(0, 100.0, 0.0, True)]), mark)
    assert s.funding_usd(1 * SECOND, 9 * SECOND, 'long', 2.0) == pytest.approx(-0.2)
    assert s.funding_usd(1 * SECOND, 9 * SECOND, 'short', 2.0) == pytest.approx(0.2)
    assert s.funding_usd(6 * SECOND, 9 * SECOND, 'long', 2.0) == 0.0


def test_round_trip_pnl_includes_fees_and_slippage():
    states = [(0, [(100.0, 100)], [(101.0, 100)]), (5 * SECOND, [(103.0, 100)], [(104.0, 100)])]
    s = Simulator(FakeBooks(states), trades([(0, 100.0, 0.0, True)]), None)
    r = s.round_trip(0, 'buy', 1010.0, hold_ns=6 * SECOND, latency_ns=0, taker_fee=0.001)
    assert r.outcome == 'done' and r.entry.price == 101.0 and r.exit.price == 103.0
    assert r.pnl_usd == pytest.approx((103 - 101) * 10 - 0.001 * 10 * 101 - 0.001 * 10 * 103)
    assert r.net_bp == pytest.approx(r.pnl_usd / (101 * 10) * 1e4)


def test_round_trip_reports_exit_failure_instead_of_inventing_a_price():
    states = [(0, [(100.0, 100)], [(101.0, 100)])]
    gap = [(5 * SECOND, 60 * SECOND)]
    s = Simulator(FakeBooks(states, unavail=gap), trades([(0, 100.0, 0.0, True)]), None)
    r = s.round_trip(0, 'buy', 101.0, hold_ns=6 * SECOND, latency_ns=0, exit_retries=2)
    assert r.outcome == 'exit_failed' and r.pnl_usd is None


def test_no_result_uses_book_before_arrival_time():
    seen = []

    class Spy(FakeBooks):
        def book_at(self, t):
            seen.append(t)
            return super().book_at(t)

    s = Simulator(Spy(BOOK), trades([(0, 100.0, 0.0, True)]), None)
    s.market(1000, 'buy', qty=1, latency_ns=300)
    assert 1300 in seen


def test_grid_runner_shape_and_sensitivity():
    states = [(0, [(100.0, 100)], [(101.0, 100)]), (5 * SECOND, [(103.0, 100)], [(104.0, 100)])]
    s = Simulator(FakeBooks(states), trades([(0, 100.0, 0.0, True)]), None)
    rows, tr = run_grid(s, [(0, 'buy')], [0, SECOND // 2], [0.0005, 0.001], [101.0], hold_ns=6 * SECOND)
    assert len(rows) == 4 and len(tr) == 4
    by = {(r['latency_s'], r['taker_fee']): r['mean_net_bp'] for r in rows}
    assert by[(0.0, 0.0005)] > by[(0.0, 0.001)]
