"""Causal one-second labels; last trade is a temporary proxy for book mid.

Grid timestamps are inclusive bucket ends. Supply another positive reference column
in the input to replace trade price with a book-derived mid. In-spike means the
current reference differs from the reference `spike_lookback` seconds ago; its
sign selects the direction, and its high/low is the observed reference extreme
in that trailing window. Flat or unavailable windows have no refresh/touch label.
First-touch extension is refresh by delta; retrace is delta from the current
reference in the opposite direction. Same-second ties conservatively retrace.
Default k=3, half-life=60 seconds, delta=0.001, lookback=30 seconds are tunable,
not calibrated. Zero volatility is invalid to avoid zero-threshold artifacts.
"""
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq
from numpy.lib.stride_tricks import sliding_window_view

VERSION = '1'
NS = 1_000_000_000
HORIZONS = (5, 10, 30)


def resample(trades: pl.DataFrame, reference: str = 'price') -> pl.DataFrame:
    """Bucket by receive time, carrying the last reference across quiet seconds."""
    if trades.is_empty():
        raise ValueError('trades is empty')
    df = trades.sort('rw', maintain_order=True).with_columns(
        ((pl.col('rw') // NS + 1) * NS - 1).alias('second'),
        pl.when(pl.col('buyer_is_maker')).then(-pl.col('qty')).otherwise(pl.col('qty')).alias('_signed'),
    )
    grouped = df.group_by('second', maintain_order=True).agg(
        pl.col(reference).last().alias('reference'),
        pl.col(reference).max().alias('high'), pl.col(reference).min().alias('low'),
        pl.col('_signed').sum().alias('signed_volume'), pl.len().alias('trade_count'),
    )
    grid = pl.DataFrame({'second': np.arange(grouped['second'][0], grouped['second'][-1] + NS, NS, dtype=np.int64)})
    return grid.join(grouped, on='second', how='left').with_columns(
        pl.col('reference').forward_fill(), pl.col('signed_volume').fill_null(0),
        pl.col('trade_count').fill_null(0),
    ).with_columns(pl.col('high').fill_null(pl.col('reference')), pl.col('low').fill_null(pl.col('reference')))


def labels(trades: pl.DataFrame, quality: pl.DataFrame | None = None, *, reference: str = 'price',
           k: float = 3.0, half_life: float = 60.0, delta: float = 0.001,
           spike_lookback: int = 30) -> pl.DataFrame:
    if k <= 0 or half_life <= 0 or delta <= 0 or spike_lookback < 1:
        raise ValueError('parameters must be positive')
    grid = resample(trades, reference)
    p = grid['reference'].to_numpy()
    if not np.all(np.isfinite(p) & (p > 0)):
        raise ValueError('reference prices must be finite and positive')
    n = len(p)
    logp = np.log(p)
    high, low = np.log(grid['high'].to_numpy()), np.log(grid['low'].to_numpy())
    variance = np.full(n, np.nan)
    alpha = -np.expm1(np.log(0.5) / half_life)
    if n > 1:
        variance[1] = (logp[1] - logp[0]) ** 2
        for i in range(2, n):
            variance[i] = alpha * (logp[i] - logp[i-1]) ** 2 + (1-alpha) * variance[i-1]
    direction = np.zeros(n, dtype=np.int8)
    trail_high = np.full(n, np.nan)
    trail_low = np.full(n, np.nan)
    if n > spike_lookback:
        windows = sliding_window_view(logp, spike_lookback + 1)
        direction[spike_lookback:] = np.sign(windows[:, -1] - windows[:, 0]).astype(np.int8)
        trail_high[spike_lookback:] = windows.max(axis=1)
        trail_low[spike_lookback:] = windows.min(axis=1)
    sec = grid['second'].to_numpy()
    out = grid.with_columns(pl.Series('spike_direction', direction))
    for h in HORIZONS:
        sigma = np.sqrt(variance * h)
        valid = np.isfinite(sigma) & (sigma > 0) & (np.arange(n) + h < n)
        if quality is not None and not quality.is_empty():
            for start, end in quality.select('start_rw', 'end_rw').iter_rows():
                valid &= ~((sec - NS + 1 <= end) & (sec + h * NS >= start))
        risk_up = np.zeros(n, dtype=bool); risk_down = risk_up.copy()
        refresh = risk_up.copy(); ambiguous = risk_up.copy()
        touch = np.full(n, 'unavailable', dtype='<U11')
        maxret = np.full(n, np.nan); minret = maxret.copy()
        m = max(n-h, 0)
        if m:
            wh = sliding_window_view(high[1:], h)[:m]
            wl = sliding_window_view(low[1:], h)[:m]
            base = logp[:m, None]
            maxret[:m] = wh.max(axis=1) - logp[:m]
            minret[:m] = wl.min(axis=1) - logp[:m]
            risk_up[:m] = maxret[:m] >= k * sigma[:m]
            risk_down[:m] = minret[:m] <= -k * sigma[:m]
            d = direction[:m, None]
            ext = np.where(d > 0, wh >= trail_high[:m, None] + delta, wl <= trail_low[:m, None] - delta) & (d != 0)
            ret = np.where(d > 0, wl <= base - delta, wh >= base + delta) & (d != 0)
            ext_i = np.where(ext.any(axis=1), ext.argmax(axis=1), h)
            ret_i = np.where(ret.any(axis=1), ret.argmax(axis=1), h)
            refresh[:m] = ext_i < h
            ambiguous[:m] = (ext_i == ret_i) & (ext_i < h)
            touch[:m] = np.where(direction[:m] == 0, 'unavailable', np.where(ret_i <= ext_i, np.where(ret_i < h, 'retrace', 'neither'), 'extension'))
        # Null outcomes prevent invalid or non-spike observations becoming negatives.
        spike_valid = valid & (direction != 0)
        def nullable(values, mask):
            return pl.Series(values).set(pl.Series(~mask), None)
        out = out.with_columns(
            pl.Series(f'sigma_{h}', sigma), pl.Series(f'valid_{h}', valid),
            nullable(risk_up, valid).alias(f'risk_up_{h}'), nullable(risk_down, valid).alias(f'risk_down_{h}'),
            nullable(maxret, valid).alias(f'range_max_{h}'), nullable(minret, valid).alias(f'range_min_{h}'),
            nullable(refresh, spike_valid).alias(f'refresh_{h}'), nullable(touch, spike_valid).alias(f'first_touch_{h}'),
            nullable(ambiguous, spike_valid).alias(f'ambiguous_{h}'),
        )
    return out


def write_parquet(frame: pl.DataFrame, path: str | Path) -> None:
    table = frame.to_arrow()
    metadata = dict(table.schema.metadata or {})
    metadata[b'label_code_version'] = VERSION.encode()
    pq.write_table(table.replace_schema_metadata(metadata), path, compression='zstd')
