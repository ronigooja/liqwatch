"""Decompose realised-path crossed mass into 14 unweighted leverage candidates.

Inputs: --roots Parquet roots accepted by liqcal.load_day: trades (sym, rw ns,
price, qty, buyer_is_maker), mark (sym, rw ns, mark), liquidations (sym, rw ns,
side BUY/SELL, avg_price, filled_qty). --brackets uses liqmap.load_brackets CSV
(symbol, tier, lower, upper, max_leverage, mmr, maint_amount). --oi is Parquet
(timestamp ms, sum_oi base units; optional symbol must match --sym).
Optional --ls CSV has timestamp ms and top_pos_ratio; publication is timestamp
+ 300000 ms, and only the last published ratio at t is used (fallback r=1).
Outputs: CSV t ns, m0, lo, hi, obs_up, obs_down, up_<L>, dn_<L> in USD.
Example (from research): python -m scripts.liq_decompose --roots /data/b0
  --sym ZECUSDT --brackets /data/brackets.csv --oi /data/oi.parquet --out /tmp/dec.csv

After-the-fact scoring uses the realised future mark path, not a forecast.
Observed USD counts only liquidations shown by the stream, a lower bound.
Obs/pred must be computed only where predicted mass is positive.
Candidates are unweighted and ignore aggressor direction (bucket_both). --ls
multiplies down/up by r/(1+r), 1/(1+r); absent --ls each side gets full mass.
Default map weights renormalize per trade under bracket caps, so globally
weighting candidate columns need not reproduce liquidation_map. With a single
allowed leverage and bucket_both, the corresponding masses do agree.
Uses private liqmap._oi_quantities to preserve its complete-bucket allocation.
Trade survival excludes the opening trade; mark survival includes marks at its
receive timestamp, as in the source. Neither observes actual position survival.
"""
import argparse

import numpy as np
import polars as pl

from liqwatch_research.liqcal import NS, load_day
from liqwatch_research.liqmap import _oi_quantities, load_brackets

LEVERAGES = (2, 3, 4, 5, 7, 10, 15, 20, 25, 30, 40, 50, 75, 100)
WINDOW_NS = 8 * 3600 * NS
SCHEMA = {'t': pl.Int64, **{name: pl.Float64 for name in (
    'm0', 'lo', 'hi', 'obs_up', 'obs_down',
    *[f'up_{lev}' for lev in LEVERAGES], *[f'dn_{lev}' for lev in LEVERAGES])}}


def load_ratios(path):
    """Return publication nanoseconds and ratios, sorted stably by publication."""
    frame = pl.read_csv(path).sort('timestamp', maintain_order=True)
    timestamps = frame['timestamp'].to_numpy()
    ratios = frame['top_pos_ratio'].to_numpy().astype(float)
    if (frame.select('timestamp', 'top_pos_ratio').null_count().to_numpy().any()
            or not np.issubdtype(timestamps.dtype, np.integer)
            or not np.all(np.isfinite(ratios) & (ratios >= 0))):
        raise ValueError('ratio timestamps must be integer ms and ratios finite, nonnegative')
    return (timestamps.astype(np.int64) + 300_000) * 1_000_000, ratios


def position_shares(t, ratios=None):
    """Return long/short shares using only values published by t (inclusive)."""
    if ratios is None:
        return 1., 1.
    times, values = ratios
    index = np.searchsorted(times, t, side='right') - 1
    ratio = float(values[index]) if index >= 0 else 1.
    short = 1. / (1. + ratio)
    return ratio * short, short


def crossed_levels(trades, mark, t, m0, lo, hi, brackets, oi, *,
                   window_ns=WINDOW_NS, ratios=None, mark_survival=False,
                   leverage_weights=None):
    """Yield (side, leverage, prices, USD masses, trade indices) for each level.

    Optional weights renormalize over allowed leverages per trade, for event
    inspection. None emits unweighted candidates. oi=None uses raw quantities
    for synthetic checks; the CLI always requires OI anchoring.
    """
    frame = trades.filter((pl.col('rw') > t - window_ns) & (pl.col('rw') <= t)).sort('rw', maintain_order=True)
    p = frame['price'].to_numpy().astype(float)
    qty = frame['qty'].to_numpy().astype(float)
    if not np.all(np.isfinite(p) & (p > 0) & np.isfinite(qty) & (qty >= 0)):
        raise ValueError('trade prices must be positive and quantities nonnegative, all finite')
    notion = p * (_oi_quantities(frame, qty, t, window_ns, oi) if oi is not None else qty)
    tier = np.searchsorted(brackets.lower, p * qty, side='right') - 1
    rate, cap = brackets.mmr[tier], brackets.max_leverage[tier]
    deduction = np.divide(brackets.amount[tier], qty, out=np.zeros_like(qty), where=qty > 0)
    high = np.r_[np.maximum.accumulate(p[:0:-1])[::-1], -np.inf][:len(p)]
    low = np.r_[np.minimum.accumulate(p[:0:-1])[::-1], np.inf][:len(p)]
    if mark_survival:
        past = mark.filter((pl.col('rw') > t - window_ns) & (pl.col('rw') <= t)).sort('rw', maintain_order=True)
        prices = past['mark'].to_numpy().astype(float)
        indices = np.searchsorted(past['rw'].to_numpy(), frame['rw'].to_numpy(), side='left')
        low = np.r_[np.minimum.accumulate(prices[::-1])[::-1], np.inf][indices]
        high = np.r_[np.maximum.accumulate(prices[::-1])[::-1], -np.inf][indices]
    leverages = LEVERAGES if leverage_weights is None else tuple(leverage_weights)
    weights = np.ones((len(leverages), len(p)))
    allowed = np.asarray(leverages)[:, None] <= cap
    if leverage_weights is not None:
        weights = allowed * np.asarray(list(leverage_weights.values()))[:, None]
        totals = weights.sum(axis=0)
        weights = np.divide(weights, totals, out=np.zeros_like(weights), where=totals > 0)
    long_share, short_share = position_shares(t, ratios)
    for j, leverage in enumerate(leverages):
        up = (p * (1 + 1 / leverage) + deduction) / (1 + rate)
        down = (p * (1 - 1 / leverage) - deduction) / (1 - rate)
        valid = allowed[j] & (notion > 0) & (weights[j] > 0)
        masks = {'up': valid & (high <= up) & (up > m0) & (up <= hi),
                 'down': valid & (low >= down) & (down >= lo) & (down < m0) & (down > 0)}
        for side, levels, share in (('up', up, short_share), ('down', down, long_share)):
            mask = masks[side]
            yield side, leverage, levels[mask], (notion * weights[j] * share)[mask], np.flatnonzero(mask)


def decompose(trades, mark, liquidations, brackets, oi, *, step_s=60,
              horizon_s=300, window_ns=WINDOW_NS, ratios=None, mark_survival=False,
              limit=0):
    """Return per-grid-moment candidate masses and observed horizon notionals."""
    if step_s <= 0 or horizon_s <= 0 or window_ns <= 0 or limit < 0:
        raise ValueError('step, horizon, window must be positive; limit nonnegative')
    trades, mark, liquidations = [f.sort('rw', maintain_order=True) for f in (trades, mark, liquidations)]
    if trades.is_empty() or mark.is_empty():
        return pl.DataFrame(schema=SCHEMA)
    mp = mark['mark'].to_numpy().astype(float)
    if not np.all(np.isfinite(mp) & (mp > 0)):
        raise ValueError('marks must be finite and positive')
    valid = pl.all_horizontal([pl.col(c).is_finite() & (pl.col(c) > 0)
                               for c in ('avg_price', 'filled_qty')]).fill_null(False)
    liquidations = liquidations.filter(valid)
    ts, mr, lr = [f['rw'].to_numpy() for f in (trades, mark, liquidations)]
    usd = liquidations['avg_price'].to_numpy() * liquidations['filled_qty'].to_numpy()
    cumulative = {s: np.r_[0., np.cumsum(np.where(liquidations['side'].to_numpy() == order, usd, 0.))]
                  for s, order in (('up', 'BUY'), ('down', 'SELL'))}
    step, horizon = step_s * NS, horizon_s * NS
    start = ((max(int(ts[0]), int(mr[0])) + step - 1) // step) * step
    rows = []
    for t in range(start, int(mr[-1]) - horizon + 1, step):
        left, right = np.searchsorted(ts, [t - window_ns, t], side='right')
        a, z = np.searchsorted(mr, [t, t + horizon], side='right')
        if right <= left or z <= a:
            continue
        m0, lo, hi = float(mp[a - 1]), float(mp[a:z].min()), float(mp[a:z].max())
        ll, lh = np.searchsorted(lr, [t, t + horizon], side='right')
        row = dict(t=t, m0=m0, lo=lo, hi=hi,
                   **{f'obs_{s}': float(v[lh] - v[ll]) for s, v in cumulative.items()})
        for side, leverage, _, mass, _ in crossed_levels(
                trades.slice(int(left), int(right - left)), mark, t, m0, lo, hi,
                brackets, oi, window_ns=window_ns, ratios=ratios, mark_survival=mark_survival):
            row[f'{"up" if side == "up" else "dn"}_{leverage}'] = float(mass.sum())
        rows.append(row)
        if limit and len(rows) >= limit:
            break
    return pl.DataFrame(rows, schema=SCHEMA)


def add_data_arguments(parser):
    parser.add_argument('--roots', nargs='+', required=True)
    parser.add_argument('--sym', required=True)
    parser.add_argument('--brackets', required=True)
    parser.add_argument('--oi', required=True, help='Parquet: timestamp ms, sum_oi base units, optional symbol')
    parser.add_argument('--out', required=True)
    parser.add_argument('--window-hours', type=float, default=8)


def read_inputs(args):
    """Load shared event/decomposition inputs without silently mixing OI symbols."""
    samples = pl.read_parquet(args.oi)
    if 'symbol' in samples.columns and (samples['symbol'].null_count()
                                        or samples.filter(pl.col('symbol') != args.sym).height):
        raise ValueError('OI file contains another or null symbol')
    if not np.isfinite(args.window_hours) or args.window_hours <= 0:
        raise ValueError('--window-hours must be finite and positive')
    return (*load_day(args.roots, args.sym), load_brackets(args.brackets)[args.sym],
            (samples['timestamp'].to_numpy(), samples['sum_oi'].to_numpy()))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    add_data_arguments(parser)
    parser.add_argument('--ls', help='CSV: timestamp ms, top_pos_ratio; publication shifted +300000 ms')
    parser.add_argument('--mark-survival', action='store_true')
    parser.add_argument('--step', type=int, default=60)
    parser.add_argument('--horizon', type=int, default=300)
    parser.add_argument('--limit', type=int, default=0, help='maximum moments; 0 means all')
    args = parser.parse_args(argv)
    result = decompose(*read_inputs(args), step_s=args.step, horizon_s=args.horizon,
                       window_ns=round(args.window_hours * 3600 * NS),
                       ratios=load_ratios(args.ls) if args.ls else None,
                       mark_survival=args.mark_survival, limit=args.limit)
    result.write_csv(args.out)
    print(f'wrote {result.height} moments to {args.out}')


if __name__ == '__main__':
    main()
