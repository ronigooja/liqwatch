"""Depth-book reconstruction and snapshot validation."""

from collections import deque
from itertools import zip_longest
import json

import pyarrow as pa
import pyarrow.parquet as pq


class OrderBook:
    def __init__(self, snapshot=None):
        self.bids = {}
        self.asks = {}
        self.known_depth = {'bids': None, 'asks': None}
        if snapshot is not None:
            for side in ('bids', 'asks'):
                levels = snapshot[side]
                book = getattr(self, side)
                book.update((float(p), float(q)) for p, q in levels if float(q))
                if levels:
                    prices = [float(p) for p, _ in levels]
                    self.known_depth[side] = min(prices) if side == 'bids' else max(prices)

    def apply(self, diff):
        undo = []
        for key, side in (('b', self.bids), ('a', self.asks)):
            for price, qty in diff[key]:
                price, qty = float(price), float(qty)
                undo.append((key, price, side.get(price)))
                if qty:
                    side[price] = qty
                else:
                    side.pop(price, None)
        return undo

    def best_bid(self):
        return max(self.bids.items(), default=None)

    def best_ask(self):
        return min(self.asks.items(), default=None)

    def top_n(self, n):
        return (sorted(self.bids.items(), reverse=True)[:n], sorted(self.asks.items())[:n])

    def imbalance(self, n):
        bids, asks = self.top_n(n)
        b, a = sum(q for _, q in bids), sum(q for _, q in asks)
        return (b - a) / (b + a) if b + a else None


class Validator:
    def __init__(self):
        self.states = {}
        self.events = []

    def _state(self, sym, rw):
        if sym not in self.states:
            self.states[sym] = dict(book=None, last=None, first=False, buffer=[], history=deque(),
                                    pending=[], unavailable=rw, diffs=0, breaks=0,
                                    verifies=0, passed=0, max_mismatch=0, unavailable_ns=0,
                                    last_rw=rw, unverifiable=0)
        return self.states[sym]

    def _emit(self, sym, kind, start, end=None, **details):
        self.events.append(dict(stream='depth', sym=sym, start_rw=start, end_rw=start if end is None else end,
                                kind=kind, details=json.dumps(details, sort_keys=True, separators=(',', ':'))))

    def feed(self, record):
        ch, sym, rw = record['ch'], record['sym'], record['rw']
        if ch not in ('depth', 'snapshot') or not sym:
            return
        s = self._state(sym, rw)
        s['last_rw'] = rw
        if ch == 'snapshot':
            p = record['p']
            if p.get('status') != 200 or 'lastUpdateId' not in p.get('body', {}):
                return
            body = p['body']
            if s['book'] is None:
                s['book'] = OrderBook(body)
                s['last'] = body['lastUpdateId']
                s['first'] = True
                buffered, s['buffer'] = s['buffer'], []
                for index, (d, t) in enumerate(buffered):
                    self._diff(sym, s, d, t, rw)
                    if s['book'] is None:
                        s['buffer'].extend(buffered[index + 1:])
                        break
            else:
                s['pending'].append((body, rw))
                self._verify_pending(sym, s)
        else:
            d = record['p']['data']
            if s['book'] is None:
                s['buffer'].append((d, rw))
            else:
                self._diff(sym, s, d, rw, rw)

    def _diff(self, sym, s, d, rw, available_rw):
        last = s['last']
        if s['first'] and d['u'] < last:
            return
        if (s['first'] and not d['U'] <= last <= d['u']) or (not s['first'] and d['pu'] != last):
            self._emit(sym, 'seq_break', rw, last_good_u=last, event_u=d['u'])
            s['breaks'] += 1
            s['book'], s['last'], s['first'] = None, None, False
            s['buffer'] = [(d, rw)]
            s['history'].clear()
            s['pending'].clear()
            if s['unavailable'] is None:
                s['unavailable'] = rw
            return
        undo = s['book'].apply(d)
        s['history'].append((d['U'], d['u'], undo, rw))
        s['last'], s['first'] = d['u'], False
        s['diffs'] += 1
        if s['unavailable'] is not None:
            start = s['unavailable']
            self._emit(sym, 'unavailable', start, available_rw)
            self._emit(sym, 'resync_done', available_rw, last_good_u=d['u'])
            s['unavailable_ns'] += max(0, available_rw - start)
            s['unavailable'] = None
        self._verify_pending(sym, s)
        while s['history'] and s['history'][0][3] < rw - 120_000_000_000:
            s['history'].popleft()

    def _verify_pending(self, sym, s):
        remaining = []
        for body, rw in s['pending']:
            target = body['lastUpdateId']
            if s['first'] or target > s['last']:
                remaining.append((body, rw))
                continue
            book = s['book']
            bids, asks = book.bids.copy(), book.asks.copy()
            exact = target == s['last']
            for U, u, undo, _ in reversed(s['history']):
                if u <= target:
                    exact = u == target
                    break
                for key, price, old in reversed(undo):
                    side = bids if key == 'b' else asks
                    if old is None:
                        side.pop(price, None)
                    else:
                        side[price] = old
                if U <= target < u:
                    break
            if not exact:
                s['unverifiable'] += 1
                for n in (20, 100):
                    self._emit(sym, 'verify', rw, n=n, mismatch_levels=None, **{'pass': None}, reason='update_id_not_observed')
                continue
            expected = OrderBook(body)
            actual = (sorted(bids.items(), reverse=True), sorted(asks.items()))
            target_book = expected.top_n(100)
            for n in (20, 100):
                mismatch = sum(sum(x != y for x, y in zip_longest(a[:n], b[:n]))
                               for a, b in zip(actual, target_book))
                self._emit(sym, 'verify', rw, n=n, mismatch_levels=mismatch, **{'pass': mismatch == 0})
                s['verifies'] += 1
                s['passed'] += mismatch == 0
                s['max_mismatch'] = max(s['max_mismatch'], mismatch)
        s['pending'] = remaining

    def finish(self):
        for sym, s in self.states.items():
            for body, rw in s['pending']:
                s['unverifiable'] += 1
                for n in (20, 100):
                    self._emit(sym, 'verify', rw, n=n, mismatch_levels=None, **{'pass': None}, reason='diff_not_observed')
            s['pending'] = []
            if s['unavailable'] is not None:
                self._emit(sym, 'unavailable', s['unavailable'], s['last_rw'])
                s['unavailable_ns'] += max(0, s['last_rw'] - s['unavailable'])
                s['unavailable'] = None
        return self.summary()

    def summary(self):
        return {sym: {k: s[k] for k in ('diffs', 'breaks', 'verifies', 'passed', 'max_mismatch', 'unavailable_ns', 'unverifiable')}
                for sym, s in self.states.items()}

    def write_parquet(self, path):
        schema = pa.schema([('stream', pa.string()), ('sym', pa.string()), ('start_rw', pa.int64()),
                            ('end_rw', pa.int64()), ('kind', pa.string()), ('details', pa.string())])
        pq.write_table(pa.Table.from_pylist(self.events, schema=schema), path, compression='zstd')
