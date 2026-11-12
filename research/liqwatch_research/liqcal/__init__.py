"""Receive-time calibration of single-symbol liquidation maps.

Evaluation moments lie on an epoch-aligned step_s-second grid (default 60),
using only that symbol's data; fewer than min_trades window trades skips a moment.
The map and trade slice use params['window_ns'] (CLI --window-hours, default 8 h).
At t, m0 is the last mark at or before t and Q = liquidation_map(trades, t,
**params). The mark path in (t, t + H], H = horizon_s (default 300), supplies
lo/min and hi/max. Only complete horizons with at least one future mark qualify.
Predicted down is Q.q_down(lo) - Q.q_down(m0) if lo < m0, otherwise zero;
predicted up is Q.q_up(hi) - Q.q_up(m0) if hi > m0, otherwise zero.
Down counts levels in [lo, m0); up counts levels in (m0, hi]. A level exactly
at the starting mark is already triggered or not newly crossed, so is excluded.
Observed down/up sum avg_price*filled_qty
for SELL/BUY liquidation records in (t, t + H]: SELL closes longs, BUY shorts.
Records with null, nonfinite or nonpositive avg_price or filled_qty are absent;
their dropped count is logged as a warning for the selected symbol.
Moment columns are t, sym, m0, lo, hi, pred_down, pred_up, obs_down, obs_up,
move_down, move_up. Moves are nonnegative fractions of m0.

Crossed means pred > 0 on that side. Scores sum predictions and observations
only on crossed moments, with ratio sum_obs/sum_pred and tie-averaged Spearman
correlation. Total sums the crossed sides within each moment, counting a moment
once if either side crosses. Undefined ratios/correlations are NaN.
The baseline jointly permutes observed down/up pairs within each symbol across
all moments before applying the unchanged crossing masks. It reports mean and
5th/95th percentiles of ratio and correlation over n seeded permutations;
model correlation should exceed the permutation range, not just its mean.

Maps are rebuilt only on grid moments, never in a Python trade loop. Sorting
costs O(N log N); vectorized searchsorted bounds cost O(G log N). For W trades
per window and L leverage weights, map rebuilding costs O(G L W log W);
mark extrema scan O(G M_H) future mark samples and liquidation sums use prefix
sums in float64. Subtracting large nearby cumulative totals can lose small
window sums through cancellation. No incremental map or parameter search is performed.
"""
import logging
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.dataset as ds

from liqwatch_research.liqmap import liquidation_map, load_brackets

NS = 1_000_000_000
MOMENT_SCHEMA = {'t': pl.Int64, 'sym': pl.String, **{
    c: pl.Float64 for c in ('m0', 'lo', 'hi', 'pred_down', 'pred_up', 'obs_down', 'obs_up',
                           'move_down', 'move_up')}}
INPUT_SCHEMA = {
    'trades': {'rw': pl.Int64, 'price': pl.Float64, 'qty': pl.Float64, 'buyer_is_maker': pl.Boolean},
    'mark': {'rw': pl.Int64, 'mark': pl.Float64},
    'liquidations': {'rw': pl.Int64, 'side': pl.String, 'avg_price': pl.Float64, 'filled_qty': pl.Float64},
}


def load_day(root, sym: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Read selected columns from one or multiple Parquet roots, filtering sym.

    Returns trades, mark, liquidations in stable receive-time order. Missing
    dataset directories yield typed empty frames; malformed datasets raise.
    """
    roots = [root] if isinstance(root, (str, Path)) else root
    frames = []
    for name, schema in INPUT_SCHEMA.items():
        parts = []
        for base in roots:
            path = Path(base) / name
            if not path.exists():
                continue
            table = ds.dataset(path, format='parquet', partitioning='hive').to_table(
                columns=list(schema), filter=ds.field('sym') == sym)
            parts.append(pl.from_arrow(table).cast(schema))
        frames.append(pl.concat(parts).sort('rw', maintain_order=True)
                      if parts else pl.DataFrame(schema=schema))
    return tuple(frames)


def moments(trades: pl.DataFrame, mark: pl.DataFrame, liquidations: pl.DataFrame,
            sym: str, *, step_s: int = 60, horizon_s: int = 300,
            min_trades: int = 1, params: dict | None = None) -> pl.DataFrame:
    """Evaluate one symbol; optional sym columns are filtered before evaluation.

    params forwards liquidation_map options, including symbol-specific brackets,
    open_interest=(timestamps_ms, base-unit sum_oi), and oi_mode='bucket'.
    """
    for value in (step_s, horizon_s, min_trades):
        if not isinstance(value, (int, np.integer)) or value <= 0:
            raise ValueError('step_s, horizon_s and min_trades must be positive integers')
    params = dict(params or {})
    window = params.get('window_ns', 8 * 60 * 60 * NS)
    if not isinstance(window, (int, np.integer)) or window <= 0:
        raise ValueError('window_ns must be a positive integer')
    trades, mark, liquidations = [
        (df.filter(pl.col('sym') == sym) if 'sym' in df.columns else df)
        .sort('rw', maintain_order=True) for df in (trades, mark, liquidations)]
    valid = pl.all_horizontal([
        pl.col(name).is_finite() & (pl.col(name) > 0)
        for name in ('avg_price', 'filled_qty')]).fill_null(False)
    filtered = liquidations.filter(valid)
    dropped = liquidations.height - filtered.height
    if dropped:
        logging.getLogger(__name__).warning('Dropped %d liquidation records for %s with invalid price or quantity',
                                            dropped, sym)
    liquidations = filtered
    if trades.is_empty() or mark.is_empty():
        return pl.DataFrame(schema=MOMENT_SCHEMA)
    prices = mark['mark'].to_numpy().astype(float)
    if mark['mark'].null_count() or not np.all(np.isfinite(prices) & (prices > 0)):
        raise ValueError('mark prices must be finite and positive')
    trw, mrw = trades['rw'].to_numpy(), mark['rw'].to_numpy()
    step, horizon = step_s * NS, horizon_s * NS
    start = ((max(int(trw[0]), int(mrw[0])) + step - 1) // step) * step
    grid = np.arange(start, int(mrw[-1]) - horizon + 1, step, dtype=np.int64)
    left = np.searchsorted(trw, grid - window, side='right')
    right = np.searchsorted(trw, grid, side='right')
    ml = np.searchsorted(mrw, grid, side='right')
    mh = np.searchsorted(mrw, grid + horizon, side='right')
    lrw = liquidations['rw'].to_numpy()
    ll = np.searchsorted(lrw, grid, side='right')
    lh = np.searchsorted(lrw, grid + horizon, side='right')
    usd = (liquidations['avg_price'].to_numpy().astype(np.float64)
           * liquidations['filled_qty'].to_numpy().astype(np.float64))
    observed = {}
    for side, order in [('down', 'SELL'), ('up', 'BUY')]:
        cumulative = np.r_[0., np.cumsum(np.where(liquidations['side'].to_numpy() == order, usd, 0.),
                                       dtype=np.float64)]
        observed[side] = cumulative[lh] - cumulative[ll]
    rows = []
    for i in np.flatnonzero((right - left >= min_trades) & (mh > ml)):
        t = int(grid[i])
        m0 = prices[ml[i] - 1]
        path = prices[ml[i]:mh[i]]
        lo, hi = float(path.min()), float(path.max())
        q = liquidation_map(trades.slice(int(left[i]), int(right[i] - left[i])), t, **params)
        rows.append((t, sym, m0, lo, hi,
                     float(q.q_down(lo) - q.q_down(m0)) if lo < m0 else 0.,
                     float(q.q_up(hi) - q.q_up(m0)) if hi > m0 else 0.,
                     float(observed['down'][i]), float(observed['up'][i]),
                     max((m0 - lo) / m0, 0.), max((hi - m0) / m0, 0.)))
    return pl.DataFrame(rows, schema=MOMENT_SCHEMA, orient='row')


def _spearman(x, y):
    if len(x) < 2 or not (np.isfinite(x).all() and np.isfinite(y).all()):
        return float('nan')
    a, b = [pl.Series(v).rank(method='average').to_numpy() for v in (x, y)]
    a, b = a - a.mean(), b - b.mean()
    denominator = np.linalg.norm(a) * np.linalg.norm(b)
    return float(np.dot(a, b) / denominator) if denominator else float('nan')


def _arrays(df):
    pred = df.select('pred_down', 'pred_up').to_numpy().astype(float)
    obs = df.select('obs_down', 'obs_up').to_numpy().astype(float)
    return pred, obs


def _selected(pred, obs, side):
    if side == 'total':
        mask = (pred > 0).any(axis=1)
        return pred.sum(axis=1)[mask], np.where(pred > 0, obs, 0.).sum(axis=1)[mask]
    j = 0 if side == 'down' else 1
    mask = pred[:, j] > 0
    return pred[mask, j], obs[mask, j]


def _metrics(pred, obs):
    total = float(pred.sum())
    return float(obs.sum() / total) if total else float('nan'), _spearman(pred, obs)


def _with_move(df, side):
    if side not in ('down', 'up', 'total'):
        raise ValueError("side must be 'down', 'up' or 'total'")
    pred, obs = _arrays(df)
    p, o = _selected(pred, obs, side)
    moves = np.column_stack([
        df[f'move_{s}'].to_numpy().astype(float) if f'move_{s}' in df.columns
        else np.full(df.height, np.nan) for s in ('down', 'up')])
    if side == 'total':
        move = moves.sum(axis=1)[(pred > 0).any(axis=1)]
    else:
        j = 0 if side == 'down' else 1
        move = moves[pred[:, j] > 0, j]
    return p, o, move


def partial_spearman(df: pl.DataFrame, side: str) -> float:
    """Correlate rank residuals after linear adjustment for move ranks.

    Use the model's crossed moments. Fewer than three moments, nonfinite
    inputs, or zero rank/residual variance yield NaN.
    """
    values = _with_move(df, side)
    if len(values[0]) < 3 or not all(np.isfinite(v).all() for v in values):
        return float('nan')
    p, o, m = [pl.Series(v).rank(method='average').to_numpy() for v in values]
    p, o, m = [v - v.mean() for v in (p, o, m)]
    if not all(np.linalg.norm(v) > 0 for v in (p, o, m)):
        return float('nan')
    residuals = [v - m * (np.dot(m, v) / np.dot(m, m)) for v in (p, o)]
    if any(np.linalg.norm(r) <= 1e-12 * np.linalg.norm(v)
           for r, v in zip(residuals, (p, o))):
        return float('nan')
    a, b = residuals
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


def move_baseline(df: pl.DataFrame) -> pl.DataFrame:
    """Return crossed-moment move correlations and power-law fit coefficients.

    For each side, pred_move = k * move**b; *_move_k and *_move_b fit
    log(obs) = log(k) + b*log(move) by least squares using finite positive
    obs/move pairs. Fewer than ten pairs or constant log(move) yields NaN.
    Fits are in-sample diagnostics, not held-out predictions. Total move is
    move_down + move_up; total observed counts only crossed sides. Legacy
    frames without move columns yield NaN move diagnostics.
    """
    result = {}
    for side in ('down', 'up', 'total'):
        _, obs, move = _with_move(df, side)
        result[f'{side}_move_spearman'] = _spearman(move, obs)
        result[f'{side}_partial_spearman'] = partial_spearman(df, side)
        valid = np.isfinite(move) & np.isfinite(obs) & (move > 0) & (obs > 0)
        k = b = float('nan')
        if valid.sum() >= 10:
            x, y = np.log(move[valid]), np.log(obs[valid])
            if np.ptp(x) > 0:
                slope, intercept = np.linalg.lstsq(
                    np.column_stack((x, np.ones(len(x)))), y, rcond=None)[0]
                with np.errstate(over='ignore', invalid='ignore'):
                    k = float(np.exp(intercept))
                b = float(slope)
        result.update({f'{side}_move_k': k, f'{side}_move_b': b})
    return pl.DataFrame([result])


def conditional_by_move(df: pl.DataFrame, side: str, bins: int = 5) -> pl.DataFrame:
    """Compare crossed moments within move quantiles (zero-based bin labels).

    Duplicate boundaries collapse; ties stay together. Nonfinite moves occupy
    bin -1 so counts still cover all crossings. Correlations require five
    moments and finite, nonconstant inputs; otherwise they are NaN.
    """
    if not isinstance(bins, (int, np.integer)) or bins <= 0:
        raise ValueError('bins must be a positive integer')
    pred, obs, move = _with_move(df, side)
    schema = {'bin': pl.Int64, 'n': pl.Int64, 'mean_pred': pl.Float64,
              'mean_obs': pl.Float64, 'spearman': pl.Float64}
    finite = np.isfinite(move)
    labels = np.full(len(move), -1, dtype=np.int64)
    if finite.any():
        edges = np.unique(np.quantile(move[finite], np.linspace(0, 1, bins + 1)))
        labels[finite] = np.searchsorted(edges[1:-1], move[finite], side='right')
    rows = []
    for label in np.unique(labels):
        p, o = pred[labels == label], obs[labels == label]
        rows.append((int(label), len(p), float(p.mean()), float(o.mean()),
                     _spearman(p, o) if len(p) >= 5 else float('nan')))
    return pl.DataFrame(rows, schema=schema, orient='row')


def baseline(df: pl.DataFrame, seed: int, n: int = 200) -> pl.DataFrame:
    """Return one wide row of within-symbol permutation statistics."""
    if not isinstance(n, (int, np.integer)) or n <= 0:
        raise ValueError('n must be a positive integer')
    pred, obs = _arrays(df)
    rng = np.random.default_rng(seed)
    groups = [np.flatnonzero(df['sym'].to_numpy() == sym) for sym in df['sym'].unique().sort()]
    samples = {side: [] for side in ('down', 'up', 'total')}
    for _ in range(n):
        shuffled = obs.copy()
        for indices in groups:
            shuffled[indices] = obs[rng.permutation(indices)]
        for side in samples:
            samples[side].append(_metrics(*_selected(pred, shuffled, side)))
    result = {}
    for side, values in samples.items():
        for j, metric in enumerate(('ratio', 'spearman')):
            finite = np.asarray(values)[:, j]
            finite = finite[np.isfinite(finite)]
            stats = (finite.mean(), *np.quantile(finite, [.05, .95])) if len(finite) else [float('nan')] * 3
            for stat, value in zip(('mean', 'p05', 'p95'), stats):
                result[f'{side}_baseline_{metric}_{stat}'] = float(value)
    return pl.DataFrame([result])


def score(df: pl.DataFrame, *, seed: int = 0, n: int = 200) -> pl.DataFrame:
    """Return one wide aggregate row, including deterministic baseline statistics."""
    pred, obs = _arrays(df)
    result = {}
    for side in ('down', 'up', 'total'):
        p, o = _selected(pred, obs, side)
        ratio, correlation = _metrics(p, o)
        result.update({f'{side}_n_moments': df.height, f'{side}_n_crossed': len(p),
                       f'{side}_sum_pred': float(p.sum()), f'{side}_sum_obs': float(o.sum()),
                       f'{side}_ratio': ratio, f'{side}_spearman': correlation})
    return pl.DataFrame([result]).hstack(baseline(df, seed, n)).hstack(move_baseline(df))


def fit_scale(df: pl.DataFrame) -> float:
    """Return sum_obs/sum_pred across crossed sides, NaN without crossings.

    Liquidation records are a lower bound (one latest order per symbol per
    1000 ms), so this is a lower-bound scale estimate, not full-market scale.
    It is a multiplier of the supplied predictions; fit on training data only.
    """
    pred, obs = _arrays(df)
    p, o = _selected(pred, obs, 'total')
    return float(o.sum() / p.sum()) if p.sum() else float('nan')


def main(argv=None):
    """Write per-symbol scores and optionally evaluated moments as CSV."""
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('roots', nargs='+')
    parser.add_argument('--syms', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--moments')
    parser.add_argument('--bins')
    parser.add_argument('--step', type=int, default=60)
    parser.add_argument('--horizon', type=int, default=300)
    parser.add_argument('--window-hours', type=float, default=8, help='map trade window in hours (default: 8)')
    parser.add_argument('--brackets', help='CSV entry-notional bracket rules')
    parser.add_argument('--oi', help='Directory containing oi_<SYMBOL>.parquet')
    parser.add_argument('--oi-mode', choices=['bucket', 'bucket_both'])
    args = parser.parse_args(argv)
    window_ns = args.window_hours * 3600 * NS
    if not np.isfinite(window_ns) or window_ns <= 0 or round(window_ns) <= 0:
        parser.error('--window-hours must be finite and positive, rounding to at least 1 nanosecond')
    window_ns = round(window_ns)
    if args.oi_mode and not args.oi:
        parser.error('--oi-mode requires --oi')
    tables = load_brackets(args.brackets) if args.brackets else None
    scores, evaluated, conditional = [], [], []
    for sym in args.syms.split(','):
        params = {'oi_mode': args.oi_mode, 'window_ns': window_ns}
        if tables is not None:
            if sym not in tables:
                parser.error(f'bracket table has no symbol {sym}')
            params['brackets'] = tables[sym]
        if args.oi:
            path = Path(args.oi) / f'oi_{sym}.parquet'
            if not path.is_file():
                parser.error(f'OI file not found: {path}')
            oi = pl.read_parquet(path)
            if not {'symbol', 'timestamp', 'sum_oi'} <= set(oi.columns):
                parser.error(f'OI file requires symbol, timestamp, sum_oi: {path}')
            if oi['symbol'].null_count() or oi.filter(pl.col('symbol') != sym).height:
                parser.error(f'OI file contains another symbol: {path}')
            params['open_interest'] = (oi['timestamp'].to_numpy(), oi['sum_oi'].to_numpy())
        frame = moments(*load_day(args.roots, sym), sym, step_s=args.step,
                        horizon_s=args.horizon, params=params)
        scores.append(score(frame).with_columns(pl.lit(sym).alias('sym')))
        if args.moments:
            evaluated.append(frame)
        if args.bins:
            for side in ('down', 'up', 'total'):
                conditional.append(conditional_by_move(frame, side).with_columns(
                    pl.lit(sym).alias('sym'), pl.lit(side).alias('side'))
                    .select('sym', 'side', 'bin', 'n', 'mean_pred', 'mean_obs', 'spearman'))
    pl.concat(scores).write_csv(args.out)
    if args.moments:
        pl.concat(evaluated).write_csv(args.moments)
    if args.bins:
        pl.concat(conditional).write_csv(args.bins)
