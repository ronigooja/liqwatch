import heapq
import io
import json
from collections import Counter, deque
from dataclasses import dataclass, field

import zstandard


@dataclass(slots=True)
class Event:
    rw: int
    kind: str
    sym: str
    fields: dict = field(default_factory=dict)

    def __getattr__(self, name):
        try:
            return self.fields[name]
        except KeyError:
            raise AttributeError(name) from None


@dataclass
class Stats:
    channels: Counter = field(default_factory=Counter)
    duplicates: int = 0
    torn_lines: int = 0
    order_violations: int = 0
    min_rw: int | None = None
    max_rw: int | None = None


def read_records(paths, stats=None):
    stats = stats if stats is not None else Stats()
    for path in paths:
        with open(path, 'rb') as source:
            with zstandard.ZstdDecompressor().stream_reader(source, read_across_frames=True) as reader:
                buffered = io.BufferedReader(reader)
                try:
                    for line in buffered:
                        if not line.endswith(b'\n'):
                            stats.torn_lines += 1
                            break
                        yield json.loads(line)
                except zstandard.ZstdError as exc:
                    # Corruption is not necessarily a torn tail; do not hide it.
                    if 'did not decompress full frame' not in str(exc):
                        raise
                    stats.torn_lines += 1


def _levels(levels):
    return [[float(p), float(q)] for p, q in levels]


def _reorder(records, horizon_ns):
    queue = []
    maximum = None
    for seq, record in enumerate(records):
        rw = record['rw']
        maximum = rw if maximum is None else max(maximum, rw)
        heapq.heappush(queue, (rw, seq, record))
        while queue and queue[0][0] <= maximum - horizon_ns:
            yield heapq.heappop(queue)[2]
    while queue:
        yield heapq.heappop(queue)[2]


def _event(record, data, rw, backfilled=False):
    ch, sym = record['ch'], record['sym']
    if ch in ('aggTrade', 'aggTrade_backfill'):
        return Event(rw, 'aggTrade', sym, dict(T=data['T'], a=data['a'], price=float(data['p']), qty=float(data['q']), buyer_is_maker=data['m'], backfilled=backfilled)), ('aggTrade', sym, data['a'])
    if ch == 'markPrice_all':
        sym = data['s']
        return Event(rw, 'markPrice', sym, dict(E=data['E'], mark=float(data['p']), index=float(data['i']), funding_rate=float(data['r']), next_funding_T=data['T'])), ('markPrice', sym, data['E'])
    if ch == 'bookTicker':
        return Event(rw, ch, sym, dict(T=data['T'], u=data['u'], bid=float(data['b']), bid_qty=float(data['B']), ask=float(data['a']), ask_qty=float(data['A']))), (ch, sym, data['u'])
    if ch == 'depth':
        return Event(rw, ch, sym, dict(E=data['E'], U=data['U'], u=data['u'], pu=data['pu'], bids=_levels(data['b']), asks=_levels(data['a']))), (ch, sym, data['U'], data['u'])
    if ch == 'snapshot':
        return Event(rw, ch, sym, dict(lastUpdateId=data['lastUpdateId'], bids=_levels(data['bids']), asks=_levels(data['asks']))), None
    if ch == 'forceOrder_all':
        o = data['o']; sym = o['s']
        return Event(rw, 'forceOrder', sym, dict(E=data['E'], side=o['S'], price=float(o['p']), avg_price=float(o['ap']), qty=float(o['q']), filled_qty=float(o['z']), status=o['X'])), ('forceOrder', sym, data['E'])
    raise ValueError(f'unknown channel: {ch}')


class Replay:
    def __init__(self, paths, window_ns=300_000_000_000, reorder_ns=1_000_000_000):
        if window_ns < 0 or reorder_ns < 0:
            raise ValueError('windows must be nonnegative')
        self.paths = list(paths)
        self.window_ns = window_ns
        self.reorder_ns = reorder_ns
        self.stats = Stats()

    def __iter__(self):
        stats = self.stats = Stats()
        sources = [_reorder(read_records([path], stats), self.reorder_ns) for path in self.paths]
        merged = heapq.merge(*sources, key=lambda r: r['rw'])
        seen, expiry = set(), deque()
        last_stream, pending = {}, {}
        previous = None
        for r in merged:
            rw, ch, payload = r['rw'], r['ch'], r['p']
            stats.channels[ch] += 1
            if previous is not None and rw < previous:
                stats.order_violations += 1
            previous = rw
            stats.min_rw = rw if stats.min_rw is None else min(stats.min_rw, rw)
            stats.max_rw = rw if stats.max_rw is None else max(stats.max_rw, rw)
            while expiry and expiry[0][0] < rw - self.window_ns:
                _, key = expiry.popleft()
                seen.discard(key)
            if ch == 'gap':
                if payload['event'] == 'drop':
                    yield Event(rw, 'drop', '', dict(stream=payload['channel'], start_rw=payload['since_ns'], end_rw=rw, count=payload['count']))
                elif payload['event'] == 'conn_closed':
                    for stream in payload['streams']:
                        start = last_stream.get(stream)
                        if start is not None:
                            pending.setdefault(stream, []).append((start, r['conn']))
                continue
            stream = payload.get('stream')
            if stream:
                last_stream[stream] = rw
                waiting = pending.get(stream, [])
                remaining = []
                for start, conn in waiting:
                    if conn != r['conn']:
                        sym = stream.split('@', 1)[0].upper() if not stream.startswith('!') else ''
                        yield Event(rw, 'gap', sym, dict(stream=stream, start_rw=start, end_rw=rw))
                    else:
                        remaining.append((start, conn))
                if remaining:
                    pending[stream] = remaining
                else:
                    pending.pop(stream, None)
            if ch == 'aggTrade_backfill':
                if not isinstance(payload['body'], list):
                    continue
                rows, event_rw = payload['body'], payload['req_end_ns']
                if event_rw != rw:
                    raise ValueError('backfill req_end_ns differs from envelope rw; cannot stream in order')
            elif ch == 'snapshot':
                if not isinstance(payload['body'], dict) or 'lastUpdateId' not in payload['body']:
                    continue
                rows, event_rw = [payload['body']], rw
            else:
                data = payload['data']
                rows, event_rw = (data if ch == 'markPrice_all' else [data]), rw
            for data in rows:
                event, key = _event(r, data, event_rw, ch == 'aggTrade_backfill')
                if key is not None:
                    if key in seen:
                        stats.duplicates += 1
                        continue
                    seen.add(key)
                    expiry.append((event_rw, key))
                yield event
