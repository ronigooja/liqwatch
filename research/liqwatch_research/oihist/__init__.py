import logging
import math
import threading
import time
from collections import deque
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import polars as pl
import requests

BASE = "https://fapi.binance.com"
PERIOD_MS = {"5m": 300_000, "15m": 900_000, "30m": 1_800_000,
             "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
             "6h": 21_600_000, "12h": 43_200_000, "1d": 86_400_000}
SCHEMA = {"symbol": pl.String, "timestamp": pl.Int64, "sum_oi": pl.Float64,
          "sum_oi_value": pl.Float64, "supply": pl.Float64}
logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self):
        self.lock = threading.Lock()
        self.times = deque()

    def wait(self):
        with self.lock:
            while True:
                now = time.monotonic()
                while self.times and now - self.times[0] >= 300:
                    self.times.popleft()
                delay = max(0, self.times[-1] + 0.51 - now) if self.times else 0
                if len(self.times) >= 599:
                    delay = max(delay, self.times[0] + 300 - now)
                if delay <= 0:
                    self.times.append(now)
                    return
                time.sleep(delay)


_limiter = RateLimiter()


class HistoryRangeError(ValueError):
    pass


def _retry_after(value):
    if value is None:
        return 0
    try:
        return max(0, float(value))
    except ValueError:
        try:
            return max(0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return 0


def _get(session, path, params=None):
    for attempt in range(6):
        _limiter.wait()
        response = session.get(BASE + path, params=params, timeout=(10, 60))
        try:
            for key, value in response.headers.items():
                if key.lower().startswith("x-mbx-used-weight"):
                    logger.debug("%s=%s", key, value)
            if response.status_code == 400:
                body = response.json()
                if body.get("code") == -1130:
                    raise HistoryRangeError(
                        f"openInterestHist rejected startTime={params.get('startTime') if params else None} "
                        f"(-1130); oldest allowed start is not supplied by this endpoint; "
                        f"request a more recent range. Server: {body.get('msg', '')}"
                    )
            if response.status_code in (418, 429) or 500 <= response.status_code < 600:
                if attempt < 5:
                    delay = max(0.5 * 2 ** attempt,
                                _retry_after(response.headers.get("Retry-After")))
                    response.close()
                    time.sleep(delay)
                    continue
            response.raise_for_status()
            return response.json()
        finally:
            response.close()


def fetch_symbol(session, symbol, start_ms, end_ms, period="5m"):
    if period not in PERIOD_MS:
        raise ValueError(f"unsupported period: {period}")
    if start_ms >= end_ms:
        raise ValueError("start_ms must be before end_ms (exclusive)")
    rows = {}
    cursor = start_ms
    while cursor < end_ms:
        stop = min(end_ms, cursor + 500 * PERIOD_MS[period])
        body = _get(session, "/futures/data/openInterestHist", {
            "symbol": symbol, "period": period, "limit": 500,
            "startTime": cursor, "endTime": stop - 1,
        })
        for row in body:
            timestamp = int(row["timestamp"])
            if start_ms <= timestamp < end_ms:
                supply = row.get("CMCCirculatingSupply")
                rows[timestamp] = {
                    "symbol": row["symbol"], "timestamp": timestamp,
                    "sum_oi": float(row["sumOpenInterest"]),
                    "sum_oi_value": float(row["sumOpenInterestValue"]),
                    "supply": None if supply is None else float(supply),
                }
        cursor = stop
    return pl.DataFrame(list(rows.values()), schema=SCHEMA).sort("timestamp")


def missing_steps(frame, start_ms=None, end_ms=None, period="5m"):
    step = PERIOD_MS[period]
    stamps = sorted(set(frame["timestamp"].to_list()))
    if start_ms is None or end_ms is None:
        return sum(max(0, (b - a) // step - 1) for a, b in zip(stamps, stamps[1:]))
    expected = max(0, math.ceil(end_ms / step) - math.ceil(start_ms / step))
    present = sum(start_ms <= stamp < end_ms and stamp % step == 0 for stamp in stamps)
    return expected - present


def list_symbols(session):
    body = _get(session, "/fapi/v1/exchangeInfo")
    return sorted(row["symbol"] for row in body["symbols"]
                  if row["status"] == "TRADING" and row["contractType"] == "PERPETUAL"
                  and row["quoteAsset"] == "USDT")
