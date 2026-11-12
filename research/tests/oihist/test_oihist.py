from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import polars as pl
import pytest
import requests

from liqwatch_research import oihist

STEP = 300_000


class Response:
    def __init__(self, body, status=200, headers=None):
        self.body, self.status_code = body, status
        self.headers = headers or {}
        self.closed = False

    def json(self):
        return self.body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def close(self):
        self.closed = True


class Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return next(self.responses)


def row(timestamp, supply=None):
    result = {"symbol": "BTCUSDT", "timestamp": timestamp,
              "sumOpenInterest": "1.25", "sumOpenInterestValue": "123.5"}
    if supply is not None:
        result["CMCCirculatingSupply"] = supply
    return result


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    now = [0.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(oihist.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(oihist.time, "sleep", sleep)
    monkeypatch.setattr(oihist, "_limiter", oihist.RateLimiter())
    return now, sleeps


def test_windows_boundaries_and_dedupe():
    session = Session([
        Response([row(i * STEP) for i in range(500)]),
        Response([row(499 * STEP)] + [row(i * STEP) for i in range(500, 1000)]),
        Response([row(999 * STEP), row(1000 * STEP), row(1001 * STEP)]),
    ])
    frame = oihist.fetch_symbol(session, "BTCUSDT", 0, 1001 * STEP)
    assert frame["timestamp"].to_list() == [i * STEP for i in range(1001)]
    assert [(call[1]["params"]["startTime"], call[1]["params"]["endTime"])
            for call in session.calls] == [(0, 500 * STEP - 1),
                                          (500 * STEP, 1000 * STEP - 1),
                                          (1000 * STEP, 1001 * STEP - 1)]
    assert all(call[1]["params"]["limit"] == 500 for call in session.calls)


@pytest.mark.parametrize("status", [429, 418, 503])
def test_retry(status, clock):
    failed = Response({}, status, {"Retry-After": "2", "x-mbx-used-weight-1m": "5"})
    session = Session([failed, Response([row(0)])])
    assert len(oihist.fetch_symbol(session, "BTCUSDT", 0, STEP)) == 1
    assert clock[1] == [2.0]
    assert failed.closed
    assert len(session.calls) == 2


def test_retry_after_date():
    date = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=60))
    assert 58 <= oihist._retry_after(date) <= 60
    assert oihist._retry_after("invalid") == 0


def test_retry_exhausted():
    session = Session([Response({}, 503) for _ in range(6)])
    with pytest.raises(requests.HTTPError, match="503"):
        oihist.fetch_symbol(session, "BTCUSDT", 0, STEP)
    assert len(session.calls) == 6


def test_gap_counting():
    frame = pl.DataFrame({"timestamp": [STEP, 2 * STEP, 5 * STEP]})
    assert oihist.missing_steps(frame) == 2
    assert oihist.missing_steps(frame, 0, 7 * STEP) == 4
    assert oihist.missing_steps(pl.DataFrame(schema=oihist.SCHEMA), 0, 3 * STEP) == 3


def test_history_too_old():
    session = Session([Response({"code": -1130, "msg": "invalid startTime"}, 400)])
    with pytest.raises(oihist.HistoryRangeError, match="oldest allowed start.*not supplied"):
        oihist.fetch_symbol(session, "BTCUSDT", 0, STEP)
    assert len(session.calls) == 1


@pytest.mark.parametrize("rows", [[], [row(0), row(STEP, "100.5")]])
def test_parquet_schema(tmp_path, rows):
    frame = oihist.fetch_symbol(Session([Response(rows)]), "BTCUSDT", 0, 2 * STEP)
    dest = tmp_path / "oi_BTCUSDT.parquet"
    frame.write_parquet(dest)
    restored = pl.read_parquet(dest)
    assert restored.schema == oihist.SCHEMA
    assert restored.equals(frame)
    if rows:
        assert restored["supply"].to_list() == [None, 100.5]


def test_limiter_shared_across_symbols(clock):
    session = Session([Response([]), Response([])])
    oihist.fetch_symbol(session, "BTCUSDT", 0, STEP)
    oihist.fetch_symbol(session, "ETHUSDT", 0, STEP)
    assert clock[1] == [0.51]


def test_limiter_long_run(clock):
    stamps = []
    for _ in range(1200):
        oihist._limiter.wait()
        stamps.append(clock[0][0])
    assert all(b - a >= 0.5 for a, b in zip(stamps, stamps[1:]))
    assert all(stamps[i + 599] - stamps[i] > 300 for i in range(len(stamps) - 599))
