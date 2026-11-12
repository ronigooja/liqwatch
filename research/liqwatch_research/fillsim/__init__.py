"""Fill simulator. Design and parameter grid: docs/fill-simulator.md.

Time is receive time `rw` in nanoseconds. Fees are fractions (0.0005 = 5 bp). Slippage is in bp versus the
mid at the signal time, positive = cost. Our orders never change the replayed book.
"""

import bisect
import itertools
from dataclasses import dataclass, asdict

import numpy as np
import pyarrow.dataset as ds

from liqwatch_research.bookat import SymbolBooks

IMPACT_FRACTION = 0.10
IMPACT_BAND = 1e-3  # 10 bp
SECOND = 1_000_000_000


@dataclass
class Fill:
    signal_rw: int
    side: str  # 'buy' | 'sell'
    kind: str  # 'market' | 'limit' | 'stop'
    outcome: str  # filled | partial | missed | rejected
    reason: str = ''
    fill_rw: int | None = None
    qty: float = 0.0
    price: float | None = None
    fee_usd: float = 0.0
    slip_bp: float | None = None
    quality: bool = False  # window overlaps an unavailable interval
    impact: bool = False  # size above IMPACT_FRACTION of depth within 10 bp of the touch


@dataclass
class RoundTrip:
    entry: Fill
    exit: Fill | None
    outcome: str  # done | no_entry | exit_failed
    pnl_usd: float | None = None
    funding_usd: float = 0.0
    net_bp: float | None = None
    hold_s: float | None = None


class Simulator:
    def __init__(self, books, trades, mark=None):
        """books: SymbolBooks-like (book_at, diffs, diff_rw, unavail). trades: dict of arrays rw, price, qty,
        buyer_is_maker sorted by rw. mark: dict of arrays rw, mark, funding_rate, next_funding_T or None."""
        self.books = books
        self.t_rw = np.asarray(trades['rw'], dtype=np.int64)
        self.t_price = np.asarray(trades['price'], dtype=float)
        self.t_qty = np.asarray(trades['qty'], dtype=float)
        self.t_m = np.asarray(trades['buyer_is_maker'], dtype=bool)
        self.mark = None
        if mark is not None and len(mark['rw']):
            self.mark = {k: np.asarray(v) for k, v in mark.items()}

    @classmethod
    def from_parquet(cls, root, symbol):
        def table(name, cols):
            t = ds.dataset(f'{root}/{name}', partitioning='hive').to_table(filter=ds.field('sym') == symbol, columns=cols)
            return t.sort_by('rw').to_pydict()
        trades = table('trades', ['rw', 'price', 'qty', 'buyer_is_maker'])
        try:
            mark = table('mark', ['rw', 'mark', 'funding_rate', 'next_funding_T'])
        except Exception:
            mark = None
        return cls(SymbolBooks(root, symbol), trades, mark)

    def _in_gap(self, a, b):
        return any(s < b and a < e for s, e in self.books.unavail)

    def _band_usd(self, levels, side):
        best = levels[0][0]
        if side == 'buy':
            return sum(p * q for p, q in levels if p <= best * (1 + IMPACT_BAND))
        return sum(p * q for p, q in levels if p >= best * (1 - IMPACT_BAND))

    def market(self, signal_rw, side, size_usd=None, qty=None, latency_ns=SECOND // 5, taker_fee=0.0005):
        """Market order sent at signal_rw, reaching the book at signal_rw + latency. Walks the opposite side.
        The order is rejected, never clipped, when the known book is too shallow."""
        f = Fill(signal_rw, side, 'market', 'rejected')
        mid_s = None
        v0 = self.books.book_at(signal_rw)
        if v0 is not None:
            mid_s = v0.mid()
        t_exec = signal_rw + latency_ns
        v = self.books.book_at(t_exec)
        f.quality = self._in_gap(signal_rw, t_exec)
        if v is None or not v.bids or not v.asks:
            f.reason = 'book_unavailable'
            return f
        got, avg, short = v.walk(side, notional_usd=size_usd, qty=qty)
        levels = v.asks if side == 'buy' else v.bids
        want_usd = size_usd if size_usd is not None else qty * levels[0][0]
        f.impact = want_usd > IMPACT_FRACTION * self._band_usd(levels, side)
        if short or got <= 0:
            f.reason = 'book_too_shallow'
            return f
        f.outcome, f.fill_rw, f.qty, f.price = 'filled', t_exec, got, avg
        f.fee_usd = taker_fee * got * avg
        if mid_s:
            f.slip_bp = ((avg - mid_s) if side == 'buy' else (mid_s - avg)) / mid_s * 1e4
        return f

    def _level_updates(self, side, price, t0, t1):
        d, rw = self.books.diffs, self.books.diff_rw
        key = 'bids' if side == 'buy' else 'asks'
        out = []
        for i in range(bisect.bisect_right(rw, t0), bisect.bisect_right(rw, t1)):
            for p, q in d[key][i]:
                if p == price:
                    out.append((rw[i], q))
        return out

    def limit(self, signal_rw, side, size_usd, price, latency_ns=SECOND // 5, maker_fee=0.0002,
              ttl_ns=30 * SECOND, bound='pessimistic'):
        """Passive order resting at `price`, placed at signal_rw + latency, cancelled at placement + ttl.
        Fills come only from executed trades; see docs/fill-simulator.md (Model, item 3). A buy order is hit by
        sell-aggressor trades (buyer_is_maker) at or below its price; a sell order by buy-aggressor trades at or
        above. A trade through the price fills us up to that trade's quantity; a trade at the price first consumes the queue ahead.
        bound='optimistic' also shrinks the queue in proportion to displayed-size decreases at the price."""
        f = Fill(signal_rw, side, 'limit', 'rejected')
        v0 = self.books.book_at(signal_rw)
        mid_s = v0.mid() if v0 is not None else None
        t0 = signal_rw + latency_ns
        t1 = t0 + ttl_ns
        v = self.books.book_at(t0)
        f.quality = self._in_gap(signal_rw, t1)
        if v is None or not v.bids or not v.asks:
            f.reason = 'book_unavailable'
            return f
        if (side == 'buy' and price >= v.asks[0][0]) or (side == 'sell' and price <= v.bids[0][0]):
            f.reason = 'marketable'
            return f
        own = v.bids if side == 'buy' else v.asks
        queue = next((q for p, q in own if p == price), 0.0)
        cur = queue
        want = size_usd / price
        remaining = want
        filled = 0.0
        last_rw = None
        lo, hi = np.searchsorted(self.t_rw, t0, side='right'), np.searchsorted(self.t_rw, t1, side='right')
        updates = self._level_updates(side, price, t0, t1) if bound == 'optimistic' else []
        ui = 0
        for i in range(lo, hi):
            tr = int(self.t_rw[i])
            while ui < len(updates) and updates[ui][0] < tr:
                queue, cur = self._apply_update(queue, cur, updates[ui][1])
                ui += 1
            if (side == 'buy') != bool(self.t_m[i]):
                continue
            p, q = self.t_price[i], self.t_qty[i]
            through = p < price if side == 'buy' else p > price
            if through:
                take = min(q, remaining)
            elif p == price:
                eaten = min(q, queue)
                queue -= eaten
                cur = max(0.0, cur - q)
                take = min(q - eaten, remaining)
            else:
                continue
            if take > 0:
                filled += take
                remaining -= take
                last_rw = tr
            if remaining <= 1e-12:
                break
        f.impact = False
        if filled <= 0:
            f.outcome, f.reason = 'missed', 'no_fill_in_ttl'
            return f
        f.outcome = 'filled' if remaining <= 1e-12 else 'partial'
        f.fill_rw, f.qty, f.price = last_rw, filled, price
        f.fee_usd = maker_fee * filled * price
        if mid_s:
            f.slip_bp = ((price - mid_s) if side == 'buy' else (mid_s - price)) / mid_s * 1e4
        return f

    @staticmethod
    def _apply_update(queue, cur, new_size):
        if new_size < cur and cur > 0:
            queue *= new_size / cur
        return queue, new_size

    def stop(self, from_rw, position_side, stop_price, qty, latency_ns=SECOND // 2, taker_fee=0.0005, until_rw=None):
        """Exchange-side stop on mark price. A long position stops when mark <= stop_price, a short when
        mark >= stop_price. On trigger a market order closes `qty` after latency_ns. Returns (Fill, trigger_rw)
        or None if the stop never triggers."""
        if self.mark is None:
            raise ValueError('mark data required for stops')
        rw, mk = self.mark['rw'], self.mark['mark']
        lo = int(np.searchsorted(rw, from_rw, side='right'))
        hi = len(rw) if until_rw is None else int(np.searchsorted(rw, until_rw, side='right'))
        seg = mk[lo:hi]
        hit = np.nonzero(seg <= stop_price)[0] if position_side == 'long' else np.nonzero(seg >= stop_price)[0]
        if not len(hit):
            return None
        trig = int(rw[lo + hit[0]])
        side = 'sell' if position_side == 'long' else 'buy'
        fill = self.market(trig, side, qty=qty, latency_ns=latency_ns, taker_fee=taker_fee)
        fill.kind = 'stop'
        return fill, trig

    def funding_usd(self, entry_rw, exit_rw, position_side, qty):
        """Funding paid (negative) or received (positive) for a position held across funding timestamps.
        Positive rate: longs pay shorts."""
        if self.mark is None or exit_rw <= entry_rw:
            return 0.0
        m = self.mark
        times = np.unique(m['next_funding_T'])
        e_ms, x_ms = entry_rw // 1_000_000, exit_rw // 1_000_000
        total = 0.0
        for T in times:
            if not (e_ms < T <= x_ms):
                continue
            j = int(np.searchsorted(m['rw'], int(T) * 1_000_000, side='right')) - 1
            if j < 0:
                continue
            sign = 1.0 if position_side == 'long' else -1.0
            total -= sign * float(m['funding_rate'][j]) * qty * float(m['mark'][j])
        return total

    def round_trip(self, signal_rw, side, size_usd, entry='market', hold_ns=10 * SECOND, latency_ns=SECOND // 5,
                   taker_fee=0.0005, maker_fee=0.0002, bound='pessimistic', ttl_ns=30 * SECOND,
                   limit_price=None, exit_retries=5, retry_gap_ns=SECOND):
        """Enter at the signal, hold, exit with market orders. Exit failures are retried every retry_gap_ns up
        to exit_retries times, then reported as 'exit_failed' (not dropped, not assigned a price)."""
        if entry == 'market':
            e = self.market(signal_rw, side, size_usd=size_usd, latency_ns=latency_ns, taker_fee=taker_fee)
        else:
            if limit_price is None:
                v = self.books.book_at(signal_rw)
                if v is None:
                    return RoundTrip(Fill(signal_rw, side, 'limit', 'rejected', 'book_unavailable'), None, 'no_entry')
                limit_price = v.bids[0][0] if side == 'buy' else v.asks[0][0]
            e = self.limit(signal_rw, side, size_usd, limit_price, latency_ns, maker_fee, ttl_ns, bound)
        if e.outcome not in ('filled', 'partial'):
            return RoundTrip(e, None, 'no_entry')
        exit_side = 'sell' if side == 'buy' else 'buy'
        x = None
        t = e.fill_rw + hold_ns
        for k in range(exit_retries + 1):
            x = self.market(t + k * retry_gap_ns, exit_side, qty=e.qty, latency_ns=latency_ns, taker_fee=taker_fee)
            if x.outcome == 'filled':
                break
        if x.outcome != 'filled':
            return RoundTrip(e, x, 'exit_failed')
        sign = 1.0 if side == 'buy' else -1.0
        pos = 'long' if side == 'buy' else 'short'
        fund = self.funding_usd(e.fill_rw, x.fill_rw, pos, e.qty)
        pnl = sign * (x.price - e.price) * e.qty - e.fee_usd - x.fee_usd + fund
        return RoundTrip(e, x, 'done', pnl, fund, pnl / (e.price * e.qty) * 1e4, (x.fill_rw - e.fill_rw) / SECOND)


def run_grid(sim, signals, latencies_ns, taker_fees, sizes_usd, entry='market', maker_fees=(0.0002,),
             bounds=('pessimistic',), hold_ns=10 * SECOND):
    """signals: iterable of (rw, side). Returns one summary row per grid cell, plus per-trade rows."""
    signals = list(signals)
    rows, trades = [], []
    for L, tf, mf, size, bound in itertools.product(latencies_ns, taker_fees, maker_fees, sizes_usd, bounds):
        if entry == 'market' and (bound != bounds[0] or mf != maker_fees[0]):
            continue
        res = [sim.round_trip(rw, s, size, entry, hold_ns, L, tf, mf, bound) for rw, s in signals]
        done = [r for r in res if r.outcome == 'done']
        nb = np.array([r.net_bp for r in done]) if done else np.array([])
        n = len(res)
        rows.append(dict(latency_s=L / SECOND, taker_fee=tf, maker_fee=mf, size_usd=size, bound=bound, entry=entry,
                         signals=n, done=len(done), no_entry=sum(r.outcome == 'no_entry' for r in res),
                         exit_failed=sum(r.outcome == 'exit_failed' for r in res),
                         impact_flagged=sum(bool(r.entry.impact) for r in res),
                         mean_net_bp=float(nb.mean()) if len(nb) else None,
                         median_net_bp=float(np.median(nb)) if len(nb) else None,
                         mean_slip_bp=float(np.mean([r.entry.slip_bp for r in done if r.entry.slip_bp is not None])) if done else None))
        trades.extend({**asdict(r.entry), 'grid_latency_s': L / SECOND, 'grid_size': size, 'rt_outcome': r.outcome,
                       'net_bp': r.net_bp} for r in res)
    return rows, trades
