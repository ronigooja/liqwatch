"""Independent REST recorder; design and operational limits: docs/poller.md."""

import fcntl
import hashlib
import json
import logging
import math
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests
import zstandard

BASE = 'https://fapi.binance.com'
BRACKETS = 'https://www.binance.com/bapi/futures/v1/friendly/future/common/brackets'
CHANNELS = ('brackets', 'exchangeInfo', 'constituents', 'openInterest', 'brackets_error')
logger = logging.getLogger(__name__)


class Clock:
    monotonic = staticmethod(time.monotonic)
    sleep = staticmethod(time.sleep)
    time = staticmethod(time.time)


def _save(path, value):
    temporary = path.with_name(path.name + '.tmp')
    with temporary.open('w') as f:
        json.dump(value, f)
        f.flush()
        os.fsync(f.fileno())
    temporary.replace(path)


class RawWriter:
    def __init__(self, root, clock=None):
        self.root, self.clock = Path(root), clock or Clock()
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = (self.root / 'poll.lock').open('a')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close()
            raise
        self.conn, self.seq = 'poll-' + uuid.uuid4().hex, 0
        self.start = self.clock.monotonic()
        self.path, self.file, self.hour = None, None, None
        for path in self.root.glob('*/poll-*.p*.jsonl.zst.part'):
            self._finish(path)

    def _finish(self, path):
        final = Path(str(path)[:-5])
        if final.exists():
            raise FileExistsError(final)
        path.replace(final)
        content = final.read_bytes()
        _save(Path(str(final) + '.meta.json'), dict(size=len(content),
              sha256=hashlib.sha256(content).hexdigest(), chunks=[final.name]))

    def rotate(self):
        hour = datetime.fromtimestamp(self.clock.time(), timezone.utc).strftime('%Y%m%d%H')
        if self.file is not None and self.hour != hour:
            self.flush()

    def write(self, ch, sym, payload, meta=None, rw=None):
        rw = int(self.clock.time() * 1e9) if rw is None else rw
        hour = datetime.fromtimestamp(rw / 1e9, timezone.utc).strftime('%Y%m%d%H')
        if self.file is not None and self.hour != hour:
            self.flush()
        if self.file is None:
            directory = self.root / hour[:8]
            directory.mkdir(exist_ok=True)
            n = 0
            while True:
                suffix = '' if n == 0 else f'.r{n}'
                final = directory / f'poll-{hour}.p{suffix}.jsonl.zst'
                self.path = Path(str(final) + '.part')
                if not final.exists() and not self.path.exists():
                    break
                n += 1
            self.file, self.hour = self.path.open('xb'), hour
        self.seq += 1
        record = dict(v=1, wseq=self.seq, conn=self.conn, cseq=self.seq, rw=rw,
                      rm=int((self.clock.monotonic() - self.start) * 1e9),
                      ch=ch, sym=sym, p=payload, meta=meta or {})
        # Independent complete frames make every acknowledged record crash-readable.
        self.file.write(zstandard.ZstdCompressor().compress((json.dumps(record) + '\n').encode()))
        self.file.flush()
        os.fsync(self.file.fileno())

    def flush(self):
        if self.file is not None:
            self.file.close()
            self.file = None
            self._finish(self.path)

    def close(self):
        try:
            self.flush()
        finally:
            self.lock.close()


@dataclass
class Config:
    weight_cap: int = 240
    oi_weight: int = 1
    constituents_weight: int = 1
    exchange_info_weight: int = 1
    brackets_weight: int = 1
    oi_rps: float = 2
    constituents_rps: float = 1

    def __post_init__(self):
        weights = (self.oi_weight, self.constituents_weight,
                   self.exchange_info_weight, self.brackets_weight)
        if self.weight_cap <= 0 or any(w <= 0 or w > self.weight_cap for w in weights):
            raise ValueError('weights must be positive and fit within the cap')
        if not 0 < self.oi_rps <= 2 or not 0 < self.constituents_rps <= 1:
            raise ValueError('rate exceeds safety ceiling or is nonpositive')


class RetryPoll(Exception):
    def __init__(self, delay, message):
        super().__init__(message)
        self.delay = delay


class BracketsStructureError(ValueError):
    pass


def validate_brackets(body):
    try:
        if not isinstance(body, dict) or body.get('success') is not True:
            raise ValueError('unsuccessful envelope')
        for key in ('code', 'message', 'messageDetail', 'data', 'success'):
            if key not in body:
                raise ValueError(f'missing envelope field {key}')
        if not isinstance(body['data'], dict):
            raise ValueError('data must be an object')
        entries = body['data']['brackets']
        if not isinstance(entries, list) or not entries:
            raise ValueError('data.brackets must be a nonempty list')
        seen = set()
        for entry in entries:
            if 'updateTime' not in entry:
                raise ValueError('missing updateTime')
            for key in ('symbol', 'updateTime', 'notionalLimit', 'riskBrackets'):
                if entry[key] is None and key != 'updateTime':
                    raise ValueError(f'null {key}')
            if not isinstance(entry['symbol'], str) or not entry['symbol'] or entry['symbol'] in seen:
                raise ValueError('invalid or duplicate symbol')
            seen.add(entry['symbol'])
            if not isinstance(entry['riskBrackets'], list) or not entry['riskBrackets']:
                raise ValueError('empty tiers')
            for tier in entry['riskBrackets']:
                for key in ('bracketSeq', 'bracketNotionalFloor', 'bracketNotionalCap',
                            'bracketMaintenanceMarginRate', 'cumFastMaintenanceAmount',
                            'minOpenPosLeverage', 'maxOpenPosLeverage'):
                    if tier[key] is None:
                        raise ValueError(f'null {key}')
        return entries
    except (KeyError, TypeError, ValueError) as exc:
        raise BracketsStructureError(str(exc)) from exc


class Poller:
    def __init__(self, session, writer, state_path, config=None, clock=None):
        self.session, self.writer = session, writer
        self.clock, self.config = clock or Clock(), config or Config()
        self.state_path = Path(state_path)
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}
        self.jobs, self.failures, self.blocked, self.last = {}, {}, {}, {}
        self.minute, self.weight, self.observed = None, 0, 0
        self._schedule()

    def _schedule(self):
        now = self.clock.time()
        day = math.floor(now / 86400) * 86400
        for ch in ('exchangeInfo', 'brackets'):
            key = (ch, '')
            if key not in self.jobs:
                fetched = self.state.get(ch, -1)
                self.jobs[key] = day + 86400 + 300 if fetched >= day else now
        for sym in self.state.get('symbols', []):
            self.jobs.setdefault(('openInterest', sym), now)
            fetched = self.state.get('constituents', {}).get(sym, -1)
            self.jobs.setdefault(('constituents', sym),
                                 day + 86400 + 300 if fetched >= day else now)

    def _request(self, ch, sym):
        now, cfg = self.clock.time(), self.config
        minute = int(now // 60)
        if minute != self.minute:
            self.minute, self.weight, self.observed = minute, 0, 0
        weight = dict(openInterest=cfg.oi_weight, constituents=cfg.constituents_weight,
                      exchangeInfo=cfg.exchange_info_weight, brackets=cfg.brackets_weight)[ch]
        delay = max(0, self.blocked.get(ch, 0) - now)
        if self.weight + weight > cfg.weight_cap or self.observed + weight > cfg.weight_cap:
            delay = max(delay, (minute + 1) * 60 - now)
        rps = cfg.oi_rps if ch == 'openInterest' else cfg.constituents_rps if ch == 'constituents' else 1
        delay = max(delay, self.last.get(ch, -math.inf) + 1 / rps - self.clock.monotonic())
        if delay > 0:
            raise RetryPoll(delay, 'rate/weight budget')
        self.weight += weight
        self.observed += weight
        self.last[ch] = self.clock.monotonic()
        response = None
        try:
            response = self.session.request('POST' if ch == 'brackets' else 'GET',
                BRACKETS if ch == 'brackets' else BASE + '/fapi/v1/' + ch,
                **({'json': {}} if ch == 'brackets' else {'params': {'symbol': sym} if sym else {}}),
                timeout=(10, 30))
            headers = {k.lower(): v for k, v in response.headers.items()}
            used = headers.get('x-mbx-used-weight-1m')
            if used is not None:
                self.observed = max(self.observed, int(used))
            if response.status_code in (418, 429):
                retry = headers.get('retry-after', '60')
                try:
                    delay = float(retry)
                except ValueError:
                    try:
                        delay = parsedate_to_datetime(retry).timestamp() - self.clock.time()
                    except (ValueError, TypeError, OverflowError):
                        delay = 60
                self.blocked[ch] = self.clock.time() + max(1, delay)
                raise RetryPoll(max(1, delay), f'HTTP {response.status_code}')
            response.raise_for_status()
            raw = response.content
            meta = dict(status=response.status_code, used_weight_1m=int(used) if used is not None else None)
            if ch == 'brackets':
                meta['response_sha256'] = hashlib.sha256(raw).hexdigest()
            rw = int(self.clock.time() * 1e9)
            try:
                body = response.json()
            except ValueError:
                if ch == 'brackets':
                    self.writer.write('brackets_error', '', response.text, meta, rw)
                    raise BracketsStructureError('non-JSON brackets response')
                raise
            return body, meta, rw
        finally:
            if response is not None:
                response.close()

    def step(self):
        self.writer.rotate()
        self._schedule()
        now = self.clock.time()
        priority = {'exchangeInfo': 0, 'brackets': 1, 'openInterest': 2, 'constituents': 3}
        key = min(self.jobs, key=lambda k: (self.jobs[k], priority[k[0]], k[1]))
        if self.jobs[key] > now:
            return min(1, self.jobs[key] - now)
        ch, sym = key
        try:
            body, meta, rw = self._request(ch, sym)
            if ch == 'brackets':
                try:
                    entries = validate_brackets(body)
                except BracketsStructureError:
                    self.writer.write('brackets_error', '', body, meta, rw)
                    raise
                for entry in entries:
                    self.writer.write(ch, entry['symbol'], entry, meta, rw)
            else:
                if ch == 'exchangeInfo':
                    symbols = sorted(r['symbol'] for r in body['symbols']
                                     if r['status'] == 'TRADING' and r['contractType'] == 'PERPETUAL'
                                     and r['quoteAsset'] == 'USDT')
                self.writer.write(ch, sym, body, meta, rw)
        except BracketsStructureError:
            logger.exception('brackets schema changed; stopping task')
            raise
        except (requests.RequestException, ValueError, KeyError, TypeError, RetryPoll) as exc:
            attempt = self.failures.get(key, 0)
            delay = exc.delay if isinstance(exc, RetryPoll) else min(300, 2 ** min(attempt, 9))
            if not isinstance(exc, RetryPoll):
                self.failures[key] = attempt + 1
            logger.warning('poll %s %s deferred %.1fs: %s', ch, sym, delay, exc)
            self.jobs[key] = self.clock.time() + delay
            return 0
        self.failures.pop(key, None)
        now = rw / 1e9
        if ch == 'exchangeInfo':
            self.state['symbols'] = symbols
            self.jobs = {k: v for k, v in self.jobs.items() if not k[1] or k[1] in symbols}
        if ch == 'constituents':
            self.state.setdefault(ch, {})[sym] = now
        elif ch != 'openInterest':
            self.state[ch] = now
        if ch != 'openInterest':
            _save(self.state_path, self.state)
        self.jobs[key] = ((math.floor(now / 300) + 1) * 300 if ch == 'openInterest'
                          else (math.floor(now / 86400) + 1) * 86400 + 300)
        return 0

    def run(self):
        while True:
            self.clock.sleep(self.step())
