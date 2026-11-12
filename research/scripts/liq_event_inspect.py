"""Inspect largest-prediction moments by leverage, distance and observed events.

Inputs: --roots, --sym, --brackets, --oi use liq_decompose's documented formats.
--moments is a liqcal CSV (t ns, m0, lo, hi, pred_up/down, obs_up/down USD).
Output: --out JSON, both-side all-moment leverage/distance totals and largest
moments for --side (default up), at most --per-hour per UTC hour. Details include
crossed masses, source age/entry-size/maker buckets, mass-weighted price
quantiles and every valid same-side liquidation in (t,t+--horizon], including
USD, last mark at/before its receive time and liquidation average price.
Distance buckets in bp: [0,25),[25,50),[50,100),[100,200),[200,infinity).
Example (from research): python -m scripts.liq_event_inspect --roots /data/b0
  --sym HOTUSDT --brackets /data/brackets.csv --oi /data/oi.parquet
  --moments /tmp/moments.csv --out /tmp/events.json

After-the-fact scoring uses the realised future path, not a forecast. Observed
USD counts only liquidations shown by the stream, a lower bound. Obs/pred is
computed only where predicted mass is positive. Default weights use liqmap's
per-trade bracket-cap renormalization and trade-price survival, bucket_both.
Inputs must have been generated with the same window, weights and horizon;
max_prediction_error exposes disagreement, not a refit. Optional --weights
L=W,... changes leverage weights (nonnegative); --unweighted emits all 14 raw
candidates instead. Neither option applies top-trader shares or mark survival.
The source script's HOT-only details are generalized to any symbol and side.
Private liqmap._oi_quantities is reused indirectly via liq_decompose.crossed_levels.
"""
import argparse
from datetime import datetime, timezone
import json

import numpy as np
import polars as pl

from liqwatch_research.liqcal import NS
from liqwatch_research.liqmap import DEFAULT_LEVERAGE_WEIGHTS

if __package__:
    from .liq_decompose import LEVERAGES, add_data_arguments, crossed_levels, read_inputs
    from .liq_fit_weights import json_ready
else:
    from liq_decompose import LEVERAGES, add_data_arguments, crossed_levels, read_inputs
    from liq_fit_weights import json_ready


def inspect(trades, mark, liquidations, brackets, oi, moments, *, side='up',
            top=10, per_hour=2, horizon_s=300, window_ns=8 * 3600 * NS,
            weights=DEFAULT_LEVERAGE_WEIGHTS):
    if side not in ('up', 'down') or top <= 0 or per_hour <= 0 or horizon_s <= 0 or window_ns <= 0:
        raise ValueError('invalid side or nonpositive inspection parameter')
    if weights is not None and (not weights or not all(np.isfinite(l) and l > 0 and np.isfinite(w) and w >= 0
                                                       for l, w in weights.items()) or sum(weights.values()) <= 0):
        raise ValueError('weights require positive finite leverages and finite nonnegative weights with positive sum')
    trades, mark, liquidations = [f.sort('rw', maintain_order=True) for f in (trades, mark, liquidations)]
    rows = moments.to_dicts()
    picked, hours = set(), {}
    for row in sorted(rows, key=lambda r: -r[f'pred_{side}']):
        hour = row['t'] // (3600 * NS)
        if hours.get(hour, 0) < per_hour:
            picked.add(row['t'])
            hours[hour] = hours.get(hour, 0) + 1
        if len(picked) >= top:
            break
    leverages = list(LEVERAGES if weights is None else weights)
    totals = {s: np.zeros(len(leverages)) for s in ('up', 'down')}
    distances = {s: np.zeros(5) for s in ('up', 'down')}
    error, details = 0., []
    for row in rows:
        t, m0 = row['t'], row['m0']
        frame = trades.filter((pl.col('rw') > t - window_ns) & (pl.col('rw') <= t))
        det = {s: np.zeros(len(leverages)) for s in totals}
        dist = {s: np.zeros(5) for s in totals}
        source = []
        for s, leverage, levels, mass, indices in crossed_levels(
                frame, mark, t, m0, row['lo'], row['hi'], brackets, oi,
                window_ns=window_ns, leverage_weights=weights):
            det[s][leverages.index(leverage)] = mass.sum()
            bp = np.abs(levels / m0 - 1) * 10_000
            dist[s] += np.bincount(np.searchsorted([25, 50, 100, 200], bp, side='right'), weights=mass, minlength=5)
            if s == side and t in picked and len(mass):
                source.append((levels, mass, (t - frame['rw'].to_numpy()[indices]) / NS / 3600,
                               (frame['price'].to_numpy() * frame['qty'].to_numpy())[indices],
                               frame['buyer_is_maker'].to_numpy()[indices].astype(int)))
        for s in totals:
            totals[s] += det[s]
            distances[s] += dist[s]
            error = max(error, abs(float(det[s].sum()) - row[f'pred_{s}']))
        if t not in picked:
            continue
        events = liquidations.filter((pl.col('rw') > t) & (pl.col('rw') <= t + horizon_s * NS)
                                     & (pl.col('side') == ('BUY' if side == 'up' else 'SELL'))
                                     & pl.col('avg_price').is_finite() & (pl.col('avg_price') > 0)
                                     & pl.col('filled_qty').is_finite() & (pl.col('filled_qty') > 0))
        observed = []
        for event in events.iter_rows(named=True):
            index = np.searchsorted(mark['rw'].to_numpy(), event['rw'], side='right') - 1
            observed.append(dict(t=event['rw'], usd=event['avg_price'] * event['filled_qty'],
                                 mark=float(mark['mark'][int(index)]) if index >= 0 else None,
                                 avg_price=event['avg_price']))
        buckets, quantiles = {}, []
        if source:
            level, mass, age, size, maker = [np.concatenate(parts) for parts in zip(*source)]
            for name, values, edges in (('age_hours', age, [1, 2, 4, 6, 8]),
                                        ('entry_usd', size, [1, 10, 100, 1000]), ('buyer_is_maker', maker, [.5])):
                buckets[name] = dict(edges=edges, mass=np.bincount(
                    np.searchsorted(edges, values, side='right'), weights=mass, minlength=len(edges) + 1).tolist())
            order = np.argsort(level)
            cumulative = np.cumsum(mass[order])
            quantiles = [float(level[order[min(np.searchsorted(cumulative, cumulative[-1] * q), len(level) - 1)]])
                         for q in (.1, .5, .9)]
        prediction = float(det[side].sum())
        obs = float(sum(event['usd'] for event in observed))
        details.append(dict(t=t, utc=datetime.fromtimestamp(t / NS, timezone.utc).isoformat(),
                            m0=m0, lo=row['lo'], hi=row['hi'], pred_input=row[f'pred_{side}'],
                            obs_input=row[f'obs_{side}'], pred=prediction, obs=obs,
                            ratio=obs / prediction if prediction > 0 else float('nan'),
                            leverage_mass=det[side].tolist(), distance_mass=dist[side].tolist(),
                            source=buckets, price_quantiles_p10_p50_p90=quantiles, events=observed))
    pred = moments[f'pred_{side}'].to_numpy()
    concentration = {str(fraction): float(np.sort(pred)[-int(np.ceil(len(pred) * fraction)):].sum() / pred.sum())
                     if len(pred) and pred.sum() > 0 else float('nan') for fraction in (.01, .05)}
    return dict(side=side, n=len(rows), leverage_order=leverages, distance_edges_bp=[25, 50, 100, 200],
                horizon_s=horizon_s, window_ns=window_ns, weights=weights,
                sum_pred=float(pred.sum()), concentration=concentration, max_prediction_error=error,
                leverage_mass={s: v.tolist() for s, v in totals.items()},
                distance_mass={s: v.tolist() for s, v in distances.items()},
                details=sorted(details, key=lambda r: r['t']))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    add_data_arguments(parser)
    parser.add_argument('--moments', required=True)
    parser.add_argument('--side', choices=['up', 'down'], default='up')
    parser.add_argument('--top', type=int, default=10)
    parser.add_argument('--per-hour', type=int, default=2)
    parser.add_argument('--horizon', type=int, default=300)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--weights', help='comma-separated L=W pairs, renormalized per trade')
    group.add_argument('--unweighted', action='store_true', help='all 14 raw leverage candidates')
    args = parser.parse_args(argv)
    weights = DEFAULT_LEVERAGE_WEIGHTS
    if args.unweighted:
        weights = None
    elif args.weights:
        weights = {float(l): float(w) for l, w in (item.split('=') for item in args.weights.split(','))}
    result = inspect(*read_inputs(args), pl.read_csv(args.moments), side=args.side,
                     top=args.top, per_hour=args.per_hour, horizon_s=args.horizon,
                     window_ns=round(args.window_hours * 3600 * NS), weights=weights)
    with open(args.out, 'w') as output:
        json.dump(json_ready(result), output, indent=2, allow_nan=False)
        output.write('\n')


if __name__ == '__main__':
    main()
