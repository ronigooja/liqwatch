"""book_at(symbol, t): order book as known at receive time t, from the Parquet depth and snapshot datasets.

Sync rules follow docs/p0-core-design.md Component 5 and are shared with liqwatch_research.book.
A book is unavailable when no synchronized state exists at t: before the first usable snapshot, after a
sequence break until the next snapshot, or inside a quality interval of kind seq_break/unavailable/gap.
"""

import bisect
from dataclasses import dataclass

import pyarrow.dataset as ds

from liqwatch_research.book import OrderBook

UNAVAILABLE_KINDS = ('seq_break', 'unavailable', 'gap')


@dataclass
class BookView:
    rw: int
    symbol: str
    bids: list  # [(price, qty)] best first, full known depth
    asks: list
    update_id: int

    def best_bid(self):
        return self.bids[0] if self.bids else None

    def best_ask(self):
        return self.asks[0] if self.asks else None

    def mid(self):
        return (self.bids[0][0] + self.asks[0][0]) / 2 if self.bids and self.asks else None

    def walk(self, side, notional_usd=None, qty=None):
        """Walk the book to buy (side='buy', takes asks) or sell (side='sell', takes bids).
        Returns (filled_qty, avg_price, reached_end). reached_end is True when the known depth ran out
        before the requested size was filled; the caller must treat that as a rejection."""
        levels = self.asks if side == 'buy' else self.bids
        want_qty, want_usd = qty, notional_usd
        got_qty = cost = 0.0
        for price, q in levels:
            if want_qty is not None:
                take = min(q, want_qty - got_qty)
            else:
                take = min(q, (want_usd - cost) / price)
            if take <= 0:
                break
            got_qty += take
            cost += take * price
            if (want_qty is not None and got_qty >= want_qty) or (want_usd is not None and cost >= want_usd - 1e-9):
                return got_qty, cost / got_qty, False
        return got_qty, (cost / got_qty if got_qty else None), True


class SymbolBooks:
    """Sequential and random access to one symbol's book. Moving forward is incremental; moving backward
    restarts from the nearest earlier snapshot."""

    def __init__(self, root, symbol, date_from=None, date_to=None):
        self.symbol = symbol
        flt = ds.field('sym') == symbol
        if date_from:
            flt &= ds.field('date') >= int(date_from)
        if date_to:
            flt &= ds.field('date') <= int(date_to)
        d = ds.dataset(f'{root}/depth', partitioning='hive').to_table(filter=flt).sort_by('rw').to_pydict()
        s = ds.dataset(f'{root}/book_snapshots', partitioning='hive').to_table(filter=flt).sort_by('rw').to_pydict()
        self.diffs = d
        self.snaps = s
        self.diff_rw = d['rw']
        self.snap_rw = s['rw']
        self.unavail = self._intervals(root, flt)
        self._reset()

    def _intervals(self, root, flt):
        try:
            q = ds.dataset(f'{root}/quality', partitioning='hive').to_table().to_pydict()
        except Exception:
            return []
        out = []
        for i, kind in enumerate(q['kind']):
            if kind in UNAVAILABLE_KINDS and (q['sym'][i] in ('', self.symbol)):
                out.append((q['start_rw'][i], q['end_rw'][i]))
        return sorted(out)

    def _reset(self):
        self.book = None
        self.snap_k = None  # index of the snapshot the current state started from
        self.pos = 0       # index of next diff to apply
        self.last = None   # last applied u
        self.first = False
        self.at = None     # rw of the last applied record
        self.broken_since = None

    def _inside_gap(self, t):
        i = bisect.bisect_right(self.unavail, (t, float('inf'))) - 1
        return i >= 0 and self.unavail[i][0] <= t < self.unavail[i][1]

    def _start_from(self, snap_i):
        self.snap_k = snap_i
        body = self.snaps
        self.book = OrderBook({'bids': body['bids'][snap_i], 'asks': body['asks'][snap_i]})
        self.last = body['lastUpdateId'][snap_i]
        self.first = True
        self.at = body['rw'][snap_i]
        self.broken_since = None
        # The diff that contains the snapshot's update id may have been received before the snapshot itself,
        # so start at the first diff with u >= lastUpdateId, not at the first diff received afterwards.
        back = bisect.bisect_left(self.diff_rw, self.at)
        self.pos = back
        while self.pos > 0 and self.diffs['u'][self.pos - 1] >= self.last:
            self.pos -= 1

    def _apply_until(self, t):
        d = self.diffs
        while self.pos < len(self.diff_rw) and d['rw'][self.pos] <= t:
            i = self.pos
            U, u, pu = d['U'][i], d['u'][i], d['pu'][i]
            self.pos += 1
            if self.first and u < self.last:
                continue
            joins = (U <= self.last <= u) or pu == self.last
            if (self.first and not joins) or (not self.first and pu != self.last):
                self.book = None
                self.broken_since = d['rw'][i]
                return
            self.book.apply({'b': d['bids'][i], 'a': d['asks'][i]})
            self.last, self.first, self.at = u, False, d['rw'][i]

    def book_at_update(self, symbol_update_id):
        """Book exactly at an update id, for verification: the state after the last diff with u <= id.
        Returns None if the id falls inside a diff (cannot be reached exactly)."""
        k = bisect.bisect_right(self.snaps['lastUpdateId'], symbol_update_id) - 1
        ids = self.snaps['lastUpdateId']
        order = sorted(range(len(ids)), key=lambda i: self.snaps['rw'][i])
        starts = [i for i in order if ids[i] <= symbol_update_id]
        if not starts:
            return None
        self._reset()
        self._start_from(starts[-1])
        d = self.diffs
        while self.pos < len(self.diff_rw) and d['u'][self.pos] <= symbol_update_id:
            i = self.pos
            self.pos += 1
            if self.first and d['u'][i] < self.last:
                continue
            if (self.first and not (d['U'][i] <= self.last <= d['u'][i] or d['pu'][i] == self.last)) or (not self.first and d['pu'][i] != self.last):
                return None
            self.book.apply({'b': d['bids'][i], 'a': d['asks'][i]})
            self.last, self.first, self.at = d['u'][i], False, d['rw'][i]
        if self.last != symbol_update_id and self.pos < len(self.diff_rw) and d['U'][self.pos] <= symbol_update_id < d['u'][self.pos]:
            return None
        bids, asks = self.book.top_n(10 ** 9)
        return BookView(self.at or 0, self.symbol, bids, asks, self.last)

    def book_at(self, t):
        """BookView at receive time t, or None when unavailable.

        State is (book, pos, at). Going backward in time restarts from the newest snapshot at or before t.
        Going forward continues from the current state; if the book is broken, the next snapshot taken
        after the break restarts it. A snapshot is never used for a time before its own receive time."""
        if self._inside_gap(t):
            return None
        k = bisect.bisect_right(self.snap_rw, t) - 1
        if k < 0:
            return None
        if self.at is not None and t < self.at:
            self._reset()
        if self.at is None or self.snap_k != k:
            self._start_from(k)
        elif self.book is None and self.snap_rw[k] > self.broken_since:
            self._start_from(k)
        self._apply_until(t)
        if self.book is None and self.snap_rw[k] > (self.broken_since or 0):
            self._start_from(k)
            self._apply_until(t)
        if self.book is None:
            return None
        bids, asks = self.book.top_n(10 ** 9)
        return BookView(t, self.symbol, bids, asks, self.last)
