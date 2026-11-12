"""Fit nonnegative 14-leverage weights on training days; score held-out days.

Inputs: --inputs DAY=CSV pairs; CSV columns t ns, obs_up, obs_down and
up_<L>, dn_<L> USD candidates from liq_decompose (see that module for formats).
--train and --test are disjoint comma-separated day labels. Inputs for multiple
model variants are evaluated by separate invocations, refitting each variant.
Output: stdout JSON with per-side weights, per-day obs/pred and Spearman, and
test-cell gate counts, min, max, max/min and mean rho. Undefined values are null.
Example (from research): python -m scripts.liq_fit_weights --train 20261104
  --test 20261105 --inputs 20261104=/tmp/day4.csv 20261105=/tmp/day5.csv

After-the-fact scoring uses the realised future path, not a causal forecast.
Observed USD counts only liquidations shown by the stream, a lower bound.
Obs/pred is sum(obs)/sum(pred) only on moments with positive predicted mass;
Spearman uses ALL moments, including zero predictions, matching the fit sources.
Overlapping horizons are not independent; the fit has no uncertainty estimate.
SciPy is imported only inside fit_weights, so other diagnostics can run without it.
Uses private liqcal._spearman for the existing tie-averaged rank convention.
"""
import argparse
import json

import numpy as np
import polars as pl

from wicker_research.liqcal import _spearman

if __package__:
    from .liq_decompose import LEVERAGES
else:
    from liq_decompose import LEVERAGES


def fit_weights(matrix, observed):
    """Solve column-scaled NNLS; return weights in original USD units."""
    from scipy.optimize import nnls

    matrix, observed = np.asarray(matrix, dtype=float), np.asarray(observed, dtype=float)
    if (matrix.ndim != 2 or observed.shape != (len(matrix),) or not len(matrix)
            or not np.isfinite(matrix).all() or not np.isfinite(observed).all()
            or np.any(matrix < 0) or np.any(observed < 0)):
        raise ValueError('NNLS requires a nonempty finite nonnegative matrix and observation vector')
    scale = matrix.std(axis=0)
    scale[scale == 0] = 1.
    weights, _ = nnls(matrix / scale, observed)
    return weights / scale


def matrices(frame, side):
    prefix = 'up' if side == 'up' else 'dn'
    return (frame.select([f'{prefix}_{lev}' for lev in LEVERAGES]).to_numpy(),
            frame[f'obs_{side}'].to_numpy())


def evaluate(data, train, test):
    if not train or not test or set(train) & set(test):
        raise ValueError('training and test days must be nonempty and disjoint')
    if len(set(train)) != len(train) or len(set(test)) != len(test):
        raise ValueError('day lists must not contain duplicates')
    days = [*train, *test]
    result = {'ratio_condition': 'pred > 0', 'spearman_condition': 'all moments', 'weights': {}, 'days': []}
    for side in ('down', 'up'):
        pairs = [matrices(data[day], side) for day in train]
        weights = fit_weights(np.vstack([x for x, _ in pairs]), np.concatenate([y for _, y in pairs]))
        result['weights'][side] = {str(lev): float(w) for lev, w in zip(LEVERAGES, weights)}
        for day in days:
            matrix, obs = matrices(data[day], side)
            if not (np.isfinite(matrix).all() and np.isfinite(obs).all()) or np.any(matrix < 0) or np.any(obs < 0):
                raise ValueError('evaluation masses and observations must be finite and nonnegative')
            pred = matrix @ weights
            positive = pred > 0
            result['days'].append(dict(day=day, split='train' if day in train else 'test', side=side,
                                       n=len(pred), n_positive=int(positive.sum()),
                                       obs_positive=float(obs[positive].sum()), pred=float(pred.sum()),
                                       obs_all=float(obs.sum()),
                                       ratio=float(obs[positive].sum() / pred[positive].sum()) if positive.any() else float('nan'),
                                       spearman=_spearman(pred, obs)))
    cells = [r for r in result['days'] if r['split'] == 'test']
    ratios = np.asarray([r['ratio'] for r in cells])
    finite = ratios[np.isfinite(ratios)]
    low, high = (float(finite.min()), float(finite.max())) if len(finite) else (float('nan'), float('nan'))
    rhos = np.asarray([r['spearman'] for r in cells])
    result['test'] = dict(cells=len(cells), finite_cells=len(finite),
                          within_gate=int(((ratios >= .5) & (ratios <= 2)).sum()),
                          min=low, max=high, max_min=high / low if low > 0 else float('nan'),
                          mean_spearman=float(np.mean(rhos)))
    return result


def json_ready(value):
    """Replace undefined floats with JSON null rather than nonstandard NaN."""
    if isinstance(value, dict):
        return {k: json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_ready(v) for v in value]
    return None if isinstance(value, float) and not np.isfinite(value) else value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--train', required=True, help='comma-separated day labels')
    parser.add_argument('--test', required=True, help='comma-separated day labels')
    parser.add_argument('--inputs', required=True, nargs='+', help='DAY=CSV, one per day')
    args = parser.parse_args(argv)
    data = {}
    for item in args.inputs:
        day, path = item.split('=', 1)
        if day in data:
            parser.error(f'duplicate input day: {day}')
        data[day] = pl.read_csv(path)
    result = evaluate(data, args.train.split(','), args.test.split(','))
    print(json.dumps(json_ready(result), indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
