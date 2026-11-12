"""Split crossed liquidation-map mass with/against strictly past-only signals.

Inputs: repeated --input DAY,SYM,MOMENTS,ROOT[,ROOT...] entries. MOMENTS is a
liqcal CSV (t receive ns, pred_up, pred_down, obs_up, obs_down in USD); roots
contain trades (sym, rw ns, price, qty, buyer_is_maker) and mark (sym, rw ns,
mark) Parquet read by liqcal.load_day. Optional --check DAY,SYM,SCORE entries
verify side_sum_pred/side_sum_obs against score/reference CSV rows (sym if present).
Outputs: --out feature CSV day,sym,method,side,grp,block,pred,obs,cross; stdout
JSON eligibility counts, per-day/ALL grouped sums and ratios, and hourly-block
bootstrap against/with intervals pooled over sides and days per symbol/method.
Example (from research): python -m scripts.liq_trend_split
  --input 20261104,ZECUSDT,/tmp/moments.csv,/data/b0 --out /tmp/features.csv

Features use marks strictly before t, a mark at/before t-1h, and trade flow in
[t-1h,t). Both methods require |past-hour return| >= 5 bp by default and trade
coverage back to t-1h. Zero/undefined signals are omitted. Flow is signed USD
(taker buys minus sells)/total USD. Bootstrap resamples UTC epoch-hour blocks,
not individual overlapping moments; it does not include model-fit uncertainty.
After-the-fact scoring uses the realised future path. Observed USD counts only
liquidations shown by the stream, a lower bound. Obs/pred uses only positive
predicted mass; group n includes eligible zero predictions, cross counts do not.
"""
import argparse
import json

import numpy as np
import polars as pl

from liqwatch_research.liqcal import NS, load_day

if __package__:
    from .liq_fit_weights import json_ready
else:
    from liq_fit_weights import json_ready

HOUR = 3600 * NS
SCHEMA = {'day': pl.String, 'sym': pl.String, 'method': pl.String,
          'side': pl.String, 'grp': pl.String, 'block': pl.Int64,
          'pred': pl.Float64, 'obs': pl.Float64, 'cross': pl.Int64}


def trend_features(trades, mark, times):
    """Return ret_bp, flow and coverage validity without reading at/after t."""
    trades, mark = [f.sort('rw', maintain_order=True) for f in (trades, mark)]
    times = np.asarray(times, dtype=np.int64)
    ret, flow = np.full(len(times), np.nan), np.full(len(times), np.nan)
    valid = np.zeros(len(times), dtype=bool)
    if trades.is_empty() or mark.is_empty():
        return ret, flow, valid
    mt, mp = mark['rw'].to_numpy(), mark['mark'].to_numpy()
    tt = trades['rw'].to_numpy()
    a = np.searchsorted(mt, times, side='left') - 1
    b = np.searchsorted(mt, times - HOUR, side='right') - 1
    left = np.searchsorted(tt, times - HOUR, side='left')
    right = np.searchsorted(tt, times, side='left')
    # Sum each past-only slice directly: appending future data cannot alter it.
    usd = trades['price'].to_numpy() * trades['qty'].to_numpy()
    signed = np.where(trades['buyer_is_maker'].to_numpy(), -usd, usd)
    for i, (l, r) in enumerate(zip(left, right)):
        volume = usd[l:r].sum()
        if volume > 0:
            flow[i] = signed[l:r].sum() / volume
    valid = (a >= 0) & (b >= 0) & (tt[0] <= times - HOUR)
    indices = np.flatnonzero(valid)
    ret[indices] = 10_000 * (mp[a[indices]] / mp[b[indices]] - 1)
    valid &= np.isfinite(ret)
    return ret, flow, valid


def split_rows(moments, trades, mark, day, sym, min_bp=5):
    times = moments['t'].to_numpy()
    ret, flow, valid = trend_features(trades, mark, times)
    eligible = valid & (np.abs(ret) >= min_bp)
    rows = []
    for i in np.flatnonzero(eligible):
        for method, signal in (('ret', ret[i]), ('flow', flow[i])):
            if not np.isfinite(signal) or signal == 0:
                continue
            for side, sign in (('down', -1), ('up', 1)):
                pred, obs = float(moments[f'pred_{side}'][int(i)]), float(moments[f'obs_{side}'][int(i)])
                rows.append((day, sym, method, side, 'with' if signal * sign > 0 else 'against',
                             int(times[i] // HOUR), pred, obs if pred > 0 else 0., int(pred > 0)))
    return pl.DataFrame(rows, schema=SCHEMA, orient='row'), int(eligible.sum())


def group_metrics(frame):
    pred, obs = float(frame['pred'].sum()), float(frame['obs'].sum())
    return dict(n=frame.height, cross=int(frame['cross'].sum()), pred=pred, obs=obs,
                ratio=obs / pred if pred > 0 else float('nan'))


def contrast(values):
    p, o, q, r = values
    return float((r / q) / (o / p)) if p > 0 and q > 0 and o > 0 else float('nan')


def bootstrap(frame, count=2000, seed=20261112):
    """Resample whole hours, pooling both sides and all days in this frame."""
    if count <= 0:
        raise ValueError('bootstrap count must be positive')
    blocks = frame['block'].unique().sort().to_list()
    array = np.zeros((len(blocks), 4))
    index = {b: i for i, b in enumerate(blocks)}
    for row in frame.group_by('block', 'grp').agg(pl.col('pred').sum(), pl.col('obs').sum()).iter_rows(named=True):
        j = 0 if row['grp'] == 'with' else 2
        array[index[row['block']], j:j + 2] = row['pred'], row['obs']
    rng = np.random.default_rng(seed)
    samples = np.asarray([contrast(array[rng.integers(len(array), size=len(array))].sum(axis=0))
                          for _ in range(count)]) if len(array) else np.array([])
    good = samples[np.isfinite(samples)]
    low, high = np.quantile(good, [.025, .975]) if len(good) else (float('nan'), float('nan'))
    return dict(ratio=contrast(array.sum(axis=0)), low=float(low), high=float(high),
                valid=len(good), samples=count, blocks=len(blocks))


def summarize(frame, count=2000, seed=20261112):
    groups, intervals = [], []
    for method in ('ret', 'flow'):
        for sym in frame['sym'].unique().sort():
            pooled = frame.filter((pl.col('method') == method) & (pl.col('sym') == sym))
            for day in [*pooled['day'].unique().sort().to_list(), 'ALL']:
                for side in ('down', 'up'):
                    selected = pooled.filter(pl.col('side') == side)
                    if day != 'ALL':
                        selected = selected.filter(pl.col('day') == day)
                    metrics = {g: group_metrics(selected.filter(pl.col('grp') == g)) for g in ('with', 'against')}
                    a, b = metrics['with']['ratio'], metrics['against']['ratio']
                    ratio = b / a if a > 0 else float('nan')
                    groups.append(dict(method=method, sym=sym, day=day, side=side, **metrics,
                                       log_against_with=float(np.log(ratio)) if ratio > 0 else float('nan')))
            intervals.append(dict(method=method, sym=sym, **bootstrap(pooled, count, seed)))
    return dict(groups=groups, bootstrap=intervals)


def check_totals(moments, reference, sym):
    if 'sym' in reference.columns:
        reference = reference.filter(pl.col('sym') == sym)
    if reference.height != 1:
        raise ValueError(f'expected one reference row for {sym}')
    row = reference.row(0, named=True)
    for side in ('down', 'up'):
        pred = moments[f'pred_{side}'].to_numpy()
        obs = moments[f'obs_{side}'].to_numpy()
        for kind, value in (('pred', pred[pred > 0].sum()), ('obs', obs[pred > 0].sum())):
            key = f'{side}_sum_{kind}'
            if not np.isclose(value, row[key], rtol=1e-8, atol=1e-5):
                raise ValueError(f'{sym} {key} does not match reference: {value} vs {row[key]}')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', action='append', required=True, help='DAY,SYM,MOMENTS,ROOT[,ROOT...]')
    parser.add_argument('--check', action='append', default=[], help='DAY,SYM,SCORE or reference CSV')
    parser.add_argument('--out', required=True)
    parser.add_argument('--min-bp', type=float, default=5)
    parser.add_argument('--bootstrap', type=int, default=2000)
    parser.add_argument('--seed', type=int, default=20261112)
    args = parser.parse_args(argv)
    if not np.isfinite(args.min_bp) or args.min_bp < 0 or args.bootstrap <= 0:
        parser.error('--min-bp must be finite and nonnegative; --bootstrap must be positive')
    checks = {}
    for entry in args.check:
        day, sym, path = entry.split(',', 2)
        checks.setdefault((day, sym), []).append(path)
    frames, eligibility, seen = [], [], set()
    for entry in args.input:
        day, sym, path, *roots = entry.split(',')
        if not roots or (day, sym) in seen:
            parser.error('each --input needs roots and a unique day/symbol pair')
        seen.add((day, sym))
        moments = pl.read_csv(path)
        for reference in checks.pop((day, sym), []):
            check_totals(moments, pl.read_csv(reference), sym)
        trades, mark, _ = load_day(roots, sym)
        frame, eligible = split_rows(moments, trades, mark, day, sym, args.min_bp)
        frames.append(frame)
        eligibility.append(dict(day=day, sym=sym, moments=moments.height, eligible=eligible))
    if checks:
        parser.error('--check entries must match an --input day/symbol')
    features = pl.concat(frames)
    features.write_csv(args.out)
    report = dict(eligibility=eligibility, **summarize(features, args.bootstrap, args.seed))
    print(json.dumps(json_ready(report), indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
