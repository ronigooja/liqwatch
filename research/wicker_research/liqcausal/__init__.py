"""Past-only liquidation-map ablation, not an economic or intervention model.

For one symbol, epoch-aligned t (step_s=60) freezes liquidation_map(trades, t).
m0 is the last mark at or before t. mass_down_d sums long levels in
[m0*(1-d/10000), m0); mass_up_d sums short levels in (m0, m0*(1+d/10000)]
for distances_bp=(10,25,50). total_down/up include every level on that side.
nearest_down/up_bp is the nearest strictly below/above-m0 level whose individual
notional is >= min_cluster USD, or 1% of total side notional when None; no
qualifying level yields NaN. Levels are raw map levels, not display bins.
sigma is the population std of one-second log mark returns in (t-1800s,t],
times sqrt(300). mass_down/up_{k}sigma use fractional distances k*sigma,
k=(1,2); undefined/zero sigma produces NaN (omitted by train-only imputation).
One-second marks are last-observation-carried-forward on the epoch grid, never
backfilled; a return needs both endpoints inside the observed past mark span.

Baseline windows are (t-W,t]: volume_5m/60m sum trade price*qty;
signed_flow_5m is aggressor buy minus sell USD; realized_vol_5m/60m are
population stds of one-second log mark returns; liq_5m_down/up sum recorded
SELL/BUY USD; hour_of_day is fractional UTC hour. Optional next_funding_T is
an epoch millisecond timestamp known in the last mark record, converted to
seconds until funding; oi_change_60m is the absolute base-unit difference of
last OI samples at or before t and t-3600s, NaN if either is missing. OI uses
the same timestamp-ms/base-quantity tuple and bracket options as liqcal.

Outcomes use (t,t+H], H=horizon_s=300: liq_down/up_usd sum SELL/BUY
avg_price*filled_qty, dropping null/nonfinite/nonpositive price or quantity
with a warning, as in liqcal. ext_down/up_bp are max(0,(m0-min)/m0)*10000
and max(0,(max-m0)/m0)*10000. Complete horizons and a future mark are required.
event_down/up is outcome >= its symbol/side training quantile q=0.9, including
ties (a zero threshold therefore labels zero outcomes positive). Events and
thresholds are computed in evaluate, never in the feature builder.

Models a/b/c predict log1p(outcome USD): a uses baselines;
b adds total_down/up; c adds both totals AND localised mass/nearest features.
USD/volume/mass features use log1p, signed flow/OI use signed log1p(abs(x));
other features are unchanged. For ridge (default), training medians impute missing values (zero
for all-missing columns); training mean/std standardize, constant columns use
unit std. An unpenalized intercept and fixed alpha=10 solve squared error
plus alpha*sum(beta**2); no tuning or test fit. Scores need no inverse transform.
For model='gbm', HistGradientBoostingRegressor uses the same transformed
features with native NaN handling (no imputation or standardization). GBM drops
columns with fewer than two distinct finite training values from both matrices,
logs their names once per fit, and predicts the training log-target mean if none
remain. Output records the dropped names per a/b/c and their union in
gbm_dropped_columns (comma-separated; empty for ridge or no drops). GBM uses fixed
max_depth=3, learning_rate=0.05, max_iter=200, min_samples_leaf=50,
l2_regularization=1.0, early_stopping=False, random_state=0; no tuning.
scikit-learn is imported only when GBM is selected.

Held-out metrics: tie-averaged Spearman, rank ROC-AUC, grouped-tie average
precision (PR-AUC), recall at false-positive budgets 1 and 3 per observed hour.
The latter is a descriptive held-out score sweep, NOT a deployable training-
calibrated threshold: take the largest score-tie-complete prefix within the FP
budget, reporting actual total alerts/hour and false alarms/hour. Exposure is
n*cadence, excluding missing moments. Undefined class/correlation metrics are
NaN; no-positive AP/recall and one-class ROC-AUC are NaN.

Paired percentile 95% CIs for c-b and b-a resample held-out epoch-aligned
contiguous block_minutes=60 blocks, or whole UTC days when 2-3 days are
present, retaining within-block order. Models/thresholds remain fixed; intervals
are conditional on the training fit, not training/model-selection uncertainty.
One block cannot identify uncertainty and yields NaN bounds. Sensitivities
remove the held-out hour with largest summed two-side outcome (ties earliest),
or keep epoch-aligned every ceil(H/step)th moment (complete horizons cannot
overlap); models remain fitted on the full purged training set.

Splits select whole UTC dates. Training horizons must end inside selected
training-day coverage; test horizons must end inside selected test coverage.
Test horizons intersecting a training day are purged; at a contiguous train/
test boundary, test moments before boundary+embargo_minutes (30) are removed.
Training dates must precede test dates. Rolling reports consecutive day pairs
separately; three days do not constitute a sealed holdout. Past feature context
may include earlier loaded days; outcomes never cross an unselected split day.

load_day reuses liqcal.load_day and separately reads optional funding metadata.
Searchsorted bounds and prefix sums avoid Python trade loops; each grid moment
still rebuilds a map, O(G*L*W*log(W)), with O(G*3600) resampled mark work.
BTC-sized 1440-map days can take minutes; no incremental map or real-data
performance claim is made. Recorded liquidation coverage and map assumptions
remain those documented in docs/liquidation-model.md.
"""
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.dataset as ds

from wicker_research import liqcal
from wicker_research.liqmap import liquidation_map, load_brackets

NS = liqcal.NS
DAY = 86400 * NS
BASELINES = ('volume_5m', 'volume_60m', 'realized_vol_5m', 'realized_vol_60m',
             'signed_flow_5m', 'liq_5m_down', 'liq_5m_up', 'hour_of_day',
             'time_to_next_funding', 'oi_change_60m')
GBM_PARAMS = dict(max_depth=3, learning_rate=0.05, max_iter=200,
                  min_samples_leaf=50, l2_regularization=1.0,
                  early_stopping=False, random_state=0)


def load_day(roots, sym):
    trades, mark, liquidations = liqcal.load_day(roots, sym)
    roots = [roots] if isinstance(roots, (str, Path)) else roots
    parts = []
    for root in roots:
        path = Path(root) / 'mark'
        if not path.exists():
            continue
        dataset = ds.dataset(path, format='parquet', partitioning='hive')
        if 'next_funding_T' in dataset.schema.names:
            parts.append(pl.from_arrow(dataset.to_table(
                columns=['rw', 'next_funding_T'], filter=ds.field('sym') == sym)))
        else:
            parts.append(pl.from_arrow(dataset.to_table(
                columns=['rw'], filter=ds.field('sym') == sym)).with_columns(
                    pl.lit(None, dtype=pl.Int64).alias('next_funding_T')))
    if parts and any(p['next_funding_T'].null_count() < p.height for p in parts):
        # Stable sorting preserves duplicate receive-time records without a many-to-many join.
        funding = pl.concat(parts).sort('rw', maintain_order=True)
        if funding.height != mark.height:
            raise ValueError('funding and mark record counts differ')
        mark = mark.with_columns(funding['next_funding_T'])
    return trades, mark, liquidations


def map_features(q, m0, *, distances_bp=(10, 25, 50), sigma=np.nan, min_cluster=None):
    if (not np.isfinite(m0) or m0 <= 0 or
            any(not np.isfinite(d) or d <= 0 for d in distances_bp) or
            (min_cluster is not None and (not np.isfinite(min_cluster) or min_cluster < 0))):
        raise ValueError('positive mark/distances and nonnegative min_cluster required')
    result = {}
    for side in ('down', 'up'):
        prices, amounts = getattr(q, f'{side}_prices'), getattr(q, f'{side}_notional')
        total = float(amounts.sum())
        result[f'total_{side}'] = total
        bp = (m0 - prices) / m0 * 10000 if side == 'down' else (prices - m0) / m0 * 10000
        threshold = .01 * total if min_cluster is None else min_cluster
        eligible = bp[(bp > 0) & (amounts >= threshold)]
        result[f'nearest_{side}_bp'] = float(eligible.min()) if len(eligible) else np.nan
        for label, distance in [(f'{d:g}', d / 10000) for d in distances_bp] + [
                (f'{k}sigma', k * sigma) for k in (1, 2)]:
            if not np.isfinite(distance) or distance <= 0:
                mass = np.nan
            elif side == 'down':
                mass = q.q_down(m0 * (1 - distance)) - q.q_down(m0)
            else:
                mass = q.q_up(m0 * (1 + distance)) - q.q_up(m0)
            result[f'mass_{side}_{label}'] = float(mass)
    return result


def _clean_liquidations(frame, sym):
    valid = pl.all_horizontal([pl.col(c).is_finite() & (pl.col(c) > 0)
                               for c in ('avg_price', 'filled_qty')]).fill_null(False)
    cleaned = frame.filter(valid)
    if cleaned.height < frame.height:
        logging.getLogger(__name__).warning(
            'Dropped %d liquidation records for %s with invalid price or quantity',
            frame.height - cleaned.height, sym)
    return cleaned


def _window_sum(times, cumulative, left, right):
    bounds = np.searchsorted(times, [left, right], side='right')
    return float(cumulative[bounds[1]] - cumulative[bounds[0]])


def _returns(times, prices, t, seconds):
    grid = np.arange(t // NS - seconds, t // NS + 1, dtype=np.int64) * NS
    indices = np.searchsorted(times, grid, side='right') - 1
    valid = indices >= 0
    values = np.full(len(grid), np.nan)
    values[valid] = np.log(prices[indices[valid]])
    returns = np.diff(values)
    return returns[np.isfinite(returns)]


def moments(trades, mark, liquidations, sym, *, step_s=60, horizon_s=300,
            distances_bp=(10, 25, 50), min_cluster=None, min_trades=1, params=None):
    for value in (step_s, horizon_s, min_trades):
        if not isinstance(value, (int, np.integer)) or value <= 0:
            raise ValueError('step_s, horizon_s and min_trades must be positive integers')
    params = dict(params or {})
    window = params.get('window_ns', 8 * 3600 * NS)
    if not isinstance(window, (int, np.integer)) or window <= 0:
        raise ValueError('window_ns must be a positive integer')
    trades, mark, liquidations = [
        (df.filter(pl.col('sym') == sym) if 'sym' in df.columns else df)
        .sort('rw', maintain_order=True) for df in (trades, mark, liquidations)]
    liquidations = _clean_liquidations(liquidations, sym)
    if trades.is_empty() or mark.is_empty():
        return pl.DataFrame(schema={'t': pl.Int64, 'sym': pl.String})
    trw, mrw, lrw = [df['rw'].to_numpy() for df in (trades, mark, liquidations)]
    prices = mark['mark'].to_numpy().astype(float)
    if mark['mark'].null_count() or not np.all(np.isfinite(prices) & (prices > 0)):
        raise ValueError('mark prices must be finite and positive')
    traded = trades['price'].to_numpy() * trades['qty'].to_numpy()
    volume = np.r_[0., np.cumsum(traded, dtype=float)]
    flow = np.r_[0., np.cumsum(np.where(trades['buyer_is_maker'].to_numpy(),
                                      -traded, traded), dtype=float)]
    usd = liquidations['avg_price'].to_numpy() * liquidations['filled_qty'].to_numpy()
    cumulative = {s: np.r_[0., np.cumsum(np.where(liquidations['side'].to_numpy() == order,
                                                usd, 0.), dtype=float)]
                  for s, order in [('down', 'SELL'), ('up', 'BUY')]}
    step, horizon = step_s * NS, horizon_s * NS
    start = ((max(int(trw[0]), int(mrw[0])) + step - 1) // step) * step
    grid = np.arange(start, int(mrw[-1]) - horizon + 1, step, dtype=np.int64)
    left = np.searchsorted(trw, grid - window, side='right')
    right = np.searchsorted(trw, grid, side='right')
    ml = np.searchsorted(mrw, grid, side='right')
    mh = np.searchsorted(mrw, grid + horizon, side='right')
    oi = params.get('open_interest', params.get('oi'))
    if oi is not None:
        ot, ov = np.asarray(oi[0]), np.asarray(oi[1], dtype=float)
        if (ot.ndim != 1 or ov.ndim != 1 or len(ot) != len(ov) or
                not np.issubdtype(ot.dtype, np.integer) or np.any(np.diff(ot) <= 0) or
                not np.isfinite(ov).all() or np.any(ov < 0)):
            raise ValueError('OI requires increasing integer milliseconds and nonnegative finite values')
    rows = []
    for i in np.flatnonzero((right - left >= min_trades) & (mh > ml)):
        t, m0 = int(grid[i]), float(prices[ml[i] - 1])
        returns = _returns(mrw[:ml[i]], prices[:ml[i]], t, 3600)
        r30 = _returns(mrw[:ml[i]], prices[:ml[i]], t, 1800)
        r5 = _returns(mrw[:ml[i]], prices[:ml[i]], t, 300)
        std = lambda r: float(np.std(r)) if len(r) >= 2 else np.nan
        q = liquidation_map(trades.slice(int(left[i]), int(right[i] - left[i])), t, **params)
        row = {'t': t, 'sym': sym, 'm0': m0,
               **map_features(q, m0, distances_bp=distances_bp,
                              min_cluster=min_cluster, sigma=std(r30) * np.sqrt(300)),
               'volume_5m': _window_sum(trw, volume, t - 300 * NS, t),
               'volume_60m': _window_sum(trw, volume, t - 3600 * NS, t),
               'signed_flow_5m': _window_sum(trw, flow, t - 300 * NS, t),
               'realized_vol_5m': std(r5), 'realized_vol_60m': std(returns),
               'hour_of_day': (t % DAY) / (3600 * NS)}
        if 'next_funding_T' in mark.columns:
            funding = mark['next_funding_T'][int(ml[i] - 1)]
            row['time_to_next_funding'] = np.nan if funding is None else funding / 1000 - t / NS
        if oi is not None:
            indices = np.searchsorted(ot, [t // 1_000_000 - 3600000, t // 1_000_000], side='right') - 1
            row['oi_change_60m'] = float(ov[indices[1]] - ov[indices[0]]) if min(indices) >= 0 else np.nan
        path = prices[ml[i]:mh[i]]
        for side in ('down', 'up'):
            row[f'liq_5m_{side}'] = _window_sum(lrw, cumulative[side], t - 300 * NS, t)
            row[f'liq_{side}_usd'] = _window_sum(lrw, cumulative[side], t, t + horizon)
            move = m0 - path.min() if side == 'down' else path.max() - m0
            row[f'ext_{side}_bp'] = float(max(0., move / m0 * 10000))
        rows.append(row)
    return pl.DataFrame(rows) if rows else pl.DataFrame(schema={'t': pl.Int64, 'sym': pl.String})


def ranks(values):
    values = np.asarray(values)
    order = np.argsort(values, kind='stable')
    sorted_values = values[order]
    starts = np.r_[0, np.flatnonzero(sorted_values[1:] != sorted_values[:-1]) + 1]
    ends = np.r_[starts[1:], len(values)]
    result = np.empty(len(values), dtype=float)
    for lo, hi in zip(starts, ends):
        result[order[lo:hi]] = (lo + hi - 1) / 2 + 1
    return result


def spearman(pred, outcome):
    if len(pred) < 2:
        return np.nan
    a, b = ranks(pred), ranks(outcome)
    a, b = a - a.mean(), b - b.mean()
    denominator = np.linalg.norm(a) * np.linalg.norm(b)
    return float(a @ b / denominator) if denominator else np.nan


def roc_auc(labels, scores):
    labels = np.asarray(labels, dtype=bool)
    positives = labels.sum()
    negatives = len(labels) - positives
    if not positives or not negatives:
        return np.nan
    return float((ranks(scores)[labels].sum() - positives * (positives + 1) / 2)
                 / (positives * negatives))


def _curve(labels, scores):
    order = np.argsort(-np.asarray(scores), kind='stable')
    sorted_scores = np.asarray(scores)[order]
    ends = np.r_[np.flatnonzero(sorted_scores[1:] != sorted_scores[:-1]) + 1, len(order)]
    tp = np.cumsum(np.asarray(labels, dtype=bool)[order])[ends - 1]
    return ends, tp, ends - tp


def average_precision(labels, scores):
    labels = np.asarray(labels, dtype=bool)
    if not labels.sum():
        return np.nan
    ends, tp, _ = _curve(labels, scores)
    return float(np.sum(np.diff(np.r_[0, tp]) * tp / ends) / labels.sum())


def metrics(pred, outcome, threshold, *, step_s=60, budgets=(1., 3.)):
    pred, outcome = np.asarray(pred), np.asarray(outcome)
    if pred.shape != outcome.shape or not np.isfinite(pred).all() or not np.isfinite(outcome).all():
        raise ValueError('metrics require equal finite arrays')
    labels = outcome >= threshold
    result = {'spearman': spearman(pred, outcome), 'roc_auc': roc_auc(labels, pred),
              'pr_auc': average_precision(labels, pred)}
    hours = len(pred) * step_s / 3600
    ends, tp, fp = _curve(labels, pred) if len(pred) else (np.array([]),) * 3
    for budget in budgets:
        eligible = np.flatnonzero(fp <= budget * hours)
        j = eligible[-1] if len(eligible) else None
        alerts, hits, false = (ends[j], tp[j], fp[j]) if j is not None else (0, 0, 0)
        prefix = f'budget_{budget:g}'
        result[f'{prefix}_recall'] = float(hits / labels.sum()) if labels.sum() else np.nan
        result[f'{prefix}_alerts_per_hour'] = float(alerts / hours) if hours else np.nan
        result[f'{prefix}_false_alarms_per_hour'] = float(false / hours) if hours else np.nan
    return result


def _transform(frame, columns):
    x = frame.select(columns).to_numpy().astype(float)
    for j, name in enumerate(columns):
        if name.startswith(('volume_', 'liq_5m_', 'total_', 'mass_')):
            x[:, j] = np.log1p(np.maximum(x[:, j], 0))
        elif name in ('signed_flow_5m', 'oi_change_60m'):
            x[:, j] = np.sign(x[:, j]) * np.log1p(np.abs(x[:, j]))
    x[~np.isfinite(x)] = np.nan
    return x


def ridge_predict(train, test, columns, outcome, *, alpha=10.):
    if not np.isfinite(alpha) or alpha <= 0:
        raise ValueError('alpha must be finite and positive')
    x, z = _transform(train, columns), _transform(test, columns)
    medians = np.array([np.median(c[np.isfinite(c)]) if np.isfinite(c).any() else 0.
                        for c in x.T])
    x, z = np.where(np.isfinite(x), x, medians), np.where(np.isfinite(z), z, medians)
    mean, scale = x.mean(axis=0), x.std(axis=0)
    scale[scale == 0] = 1
    x, z = (x - mean) / scale, (z - mean) / scale
    y = np.log1p(train[outcome].to_numpy().astype(float))
    beta = np.linalg.solve(x.T @ x + alpha * np.eye(len(columns)), x.T @ (y - y.mean()))
    return z @ beta + y.mean()


def gbm_predict(train, test, columns, outcome, *, dropped_columns=None):
    from sklearn.ensemble import HistGradientBoostingRegressor

    x, z = _transform(train, columns), _transform(test, columns)
    y = np.log1p(train[outcome].to_numpy().astype(float))
    keep = np.array([len(np.unique(c[np.isfinite(c)])) >= 2 for c in x.T], dtype=bool)
    dropped = [name for name, usable in zip(columns, keep) if not usable]
    if dropped_columns is not None:
        dropped_columns.extend(dropped)
    if dropped:
        logging.getLogger(__name__).warning(
            'Dropped GBM columns with fewer than two distinct finite training values: %s',
            ','.join(dropped))
    if not keep.any():
        return np.full(test.height, y.mean())
    return HistGradientBoostingRegressor(**GBM_PARAMS).fit(x[:, keep], y).predict(z[:, keep])


def bootstrap_differences(times, predictions, outcome, threshold, *, step_s=60,
                          block_minutes=60, n_boot=200, seed=0, budgets=(1., 3.)):
    if block_minutes <= 0 or n_boot <= 0:
        raise ValueError('positive block_minutes and n_boot required')
    times = np.asarray(times, dtype=np.int64)
    days = np.unique(times // DAY)
    block_ns = DAY if 2 <= len(days) <= 3 else int(block_minutes * 60 * NS)
    labels = times // block_ns
    groups = [np.flatnonzero(labels == key) for key in np.unique(labels)]
    keys = list(metrics(predictions['a'], outcome, threshold, step_s=step_s, budgets=budgets))
    samples = {f'{hi}_minus_{lo}_{key}': [] for hi, lo in [('b', 'a'), ('c', 'b')] for key in keys}
    if len(groups) >= 2:
        rng = np.random.default_rng(seed)
        for _ in range(n_boot):
            indices = np.concatenate([groups[i] for i in rng.integers(len(groups), size=len(groups))])
            scores = {name: metrics(p[indices], outcome[indices], threshold,
                                    step_s=step_s, budgets=budgets) for name, p in predictions.items()}
            for hi, lo in [('b', 'a'), ('c', 'b')]:
                for key in keys:
                    samples[f'{hi}_minus_{lo}_{key}'].append(scores[hi][key] - scores[lo][key])
    result = {'bootstrap_blocks': len(groups), 'bootstrap_block_minutes': block_ns / NS / 60}
    for key, values in samples.items():
        finite = np.asarray(values)[np.isfinite(values)]
        lo, hi = np.quantile(finite, [.025, .975]) if len(finite) else (np.nan, np.nan)
        result[f'{key}_lo'], result[f'{key}_hi'] = float(lo), float(hi)
        result[f'{key}_bootstrap_valid'] = len(finite)
    return result


def evaluate(train, test, side, *, q=.9, alpha=10., step_s=60, horizon_s=300,
             block_minutes=60, n_boot=200, seed=0, budgets=(1., 3.), model='ridge'):
    if model not in ('ridge', 'gbm'):
        raise ValueError('model must be ridge/gbm')
    if side not in ('down', 'up') or not 0 <= q <= 1:
        raise ValueError('side must be down/up and q in [0,1]')
    if train.is_empty() or test.is_empty():
        raise ValueError('nonempty training and held-out moments required')
    if train['sym'].n_unique() != 1 or test['sym'].n_unique() != 1 or train['sym'][0] != test['sym'][0]:
        raise ValueError('evaluate requires exactly one matching symbol')
    train, test = train.sort('t'), test.sort('t')
    if train['t'].max() + horizon_s * NS > test['t'].min():
        raise ValueError('training horizons must end before test moments')
    target = f'liq_{side}_usd'
    threshold = float(np.quantile(train[target].to_numpy(), q))
    baseline = [c for c in BASELINES if c in train.columns and c in test.columns]
    totals = ['total_down', 'total_up']
    local = [c for c in train.columns if c.startswith(('mass_', 'nearest_')) and c in test.columns]
    predict = ridge_predict if model == 'ridge' else gbm_predict
    options = {'alpha': alpha} if model == 'ridge' else {}
    predictions, dropped = {}, {name: [] for name in 'abc'}
    for name, columns in [('a', baseline), ('b', baseline + totals),
                          ('c', baseline + totals + local)]:
        if model == 'gbm':
            options = {'dropped_columns': dropped[name]}
        predictions[name] = predict(train, test, columns, target, **options)
    times, outcomes = test['t'].to_numpy(), test[target].to_numpy()
    hourly = test.group_by((pl.col('t') // (3600 * NS)).alias('hour')).agg(
        (pl.col('liq_down_usd') + pl.col('liq_up_usd')).sum().alias('usd')).sort(
            ['usd', 'hour'], descending=[True, False])
    largest = int(hourly['hour'][0])
    stride = int(np.ceil(horizon_s / step_s))
    variants = [('all', np.ones(len(times), dtype=bool), step_s),
                ('without_largest_hour', times // (3600 * NS) != largest, step_s),
                ('nonoverlap', (times // (step_s * NS)) % stride == 0, stride * step_s)]
    rows = []
    for variant, mask, cadence in variants:
        p = {name: values[mask] for name, values in predictions.items()}
        y, t = outcomes[mask], times[mask]
        row = {'sym': test['sym'][0], 'side': side, 'variant': variant, 'model': model,
               'threshold': threshold, 'n_train': train.height, 'n_test': len(y),
               'events': int((y >= threshold).sum()), 'alpha': alpha,
               'largest_event_hour_ns': largest * 3600 * NS,
               'train_start_ns': int(train['t'].min()), 'train_end_ns': int(train['t'].max()),
               'test_start_ns': int(times.min()), 'test_end_ns': int(times.max())}
        row['gbm_dropped_columns'] = ','.join(dict.fromkeys(
            column for names in dropped.values() for column in names))
        for name, names in dropped.items():
            row[f'{name}_gbm_dropped_columns'] = ','.join(names)
        scores = {name: metrics(values, y, threshold, step_s=cadence, budgets=budgets)
                  for name, values in p.items()}
        for name, values in scores.items():
            row.update({f'{name}_{key}': value for key, value in values.items()})
        for hi, lo in [('b', 'a'), ('c', 'b')]:
            row.update({f'{hi}_minus_{lo}_{key}': scores[hi][key] - scores[lo][key]
                        for key in scores[hi]})
        row.update(bootstrap_differences(t, p, y, threshold, step_s=cadence,
                                         block_minutes=block_minutes, n_boot=n_boot,
                                         seed=seed, budgets=budgets))
        rows.append(row)
    return pl.DataFrame(rows)


def _day_ns(value):
    value = date.fromisoformat(value)
    return int(datetime(value.year, value.month, value.day, tzinfo=timezone.utc).timestamp()) * NS


def split_moments(frame, train_dates, test_dates, *, horizon_s=300, embargo_minutes=30):
    train_days = sorted({_day_ns(d) for d in train_dates})
    test_days = sorted({_day_ns(d) for d in test_dates})
    if not train_days or not test_days or max(train_days) >= min(test_days):
        raise ValueError('all training dates must precede all test dates')
    if horizon_s <= 0 or embargo_minutes < 0:
        raise ValueError('positive horizon and nonnegative embargo required')
    times = frame['t'].to_numpy()
    ends = times + horizon_s * NS

    def inside(days):
        starts = [d for d in days if d - DAY not in days]
        stops = [d + DAY for d in days if d + DAY not in days]
        mask = np.zeros(len(times), dtype=bool)
        for start, stop in zip(starts, stops):
            mask |= (times >= start) & (ends < stop)
        return mask

    train_mask, test_mask = inside(train_days), inside(test_days)
    for day in train_days:
        test_mask &= ~((ends >= day) & (times < day + DAY))
        if day + DAY in test_days:
            test_mask &= times >= day + DAY + embargo_minutes * 60 * NS
    return frame.filter(train_mask), frame.filter(test_mask)


def rolling_pairs(dates):
    dates = sorted(set(dates), key=_day_ns)
    return [([a], [b]) for a, b in zip(dates, dates[1:]) if _day_ns(b) - _day_ns(a) == DAY]


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', action='append', required=True, help='DATE=ROOT1,ROOT2,...')
    parser.add_argument('--train', help='comma-separated ISO UTC dates')
    parser.add_argument('--test', help='comma-separated ISO UTC dates')
    parser.add_argument('--rolling', action='store_true')
    parser.add_argument('--syms', required=True)
    parser.add_argument('--out', required=True, help='directory for SYMBOL_SIDE[_gbm].csv')
    parser.add_argument('--model', choices=['ridge', 'gbm'], default='ridge')
    parser.add_argument('--step', type=int, default=60)
    parser.add_argument('--horizon', type=int, default=300)
    parser.add_argument('--distances-bp', default='10,25,50')
    parser.add_argument('--min-cluster', type=float)
    parser.add_argument('--quantile', type=float, default=.9)
    parser.add_argument('--alpha', type=float, default=10.)
    parser.add_argument('--embargo-minutes', type=float, default=30)
    parser.add_argument('--block-minutes', type=float, default=60)
    parser.add_argument('--bootstrap', type=int, default=200)
    parser.add_argument('--seed', type=int, default=0)
    parser.add_argument('--budgets', default='1,3')
    parser.add_argument('--brackets')
    parser.add_argument('--oi', help='directory containing oi_<SYMBOL>.parquet')
    parser.add_argument('--oi-mode', choices=['bucket', 'bucket_both'])
    args = parser.parse_args(argv)
    if args.oi_mode and not args.oi:
        parser.error('--oi-mode requires --oi')
    roots = {}
    try:
        for spec in args.root:
            day, paths = spec.split('=', 1)
            _day_ns(day)
            roots.setdefault(day, []).extend(paths.split(','))
        pairs = rolling_pairs(roots) if args.rolling else [(args.train.split(','), args.test.split(','))]
        if not pairs:
            raise ValueError('no consecutive rolling day pairs')
        for train_dates, test_dates in pairs:
            if not set(train_dates + test_dates) <= roots.keys():
                raise ValueError('every split date requires --root')
        distances = tuple(float(d) for d in args.distances_bp.split(','))
        budgets = tuple(float(b) for b in args.budgets.split(','))
        if any(not np.isfinite(b) or b < 0 for b in budgets):
            raise ValueError('budgets must be finite and nonnegative')
    except (ValueError, AttributeError) as exc:
        parser.error(str(exc))
    tables = load_brackets(args.brackets) if args.brackets else None
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for sym in args.syms.split(','):
        params = {'oi_mode': args.oi_mode}
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
        loaded = [load_day(paths, sym) for day, paths in sorted(roots.items())]
        data = [pl.concat([day[i] for day in loaded], how='diagonal_relaxed') for i in range(3)]
        frame = moments(*data, sym, step_s=args.step, horizon_s=args.horizon,
                        distances_bp=distances, min_cluster=args.min_cluster, params=params)
        results = {'down': [], 'up': []}
        for train_dates, test_dates in pairs:
            train, test = split_moments(frame, train_dates, test_dates, horizon_s=args.horizon,
                                        embargo_minutes=args.embargo_minutes)
            for side in results:
                scored = evaluate(train, test, side, q=args.quantile, alpha=args.alpha, model=args.model,
                                  step_s=args.step, horizon_s=args.horizon, seed=args.seed,
                                  block_minutes=args.block_minutes, n_boot=args.bootstrap,
                                  budgets=budgets).with_columns(
                                      pl.lit(','.join(train_dates)).alias('train_dates'),
                                      pl.lit(','.join(test_dates)).alias('test_dates'))
                results[side].append(scored)
                row = scored.row(0, named=True)
                print(f"{sym} {side} train={','.join(train_dates)} test={','.join(test_dates)} "
                      f"n={row['n_test']} threshold={row['threshold']:.6g} "
                      f"Spearman a/b/c={row['a_spearman']:.4f}/{row['b_spearman']:.4f}/{row['c_spearman']:.4f} "
                      f"b-a={row['b_minus_a_spearman']:.4f} "
                      f"95%CI=[{row['b_minus_a_spearman_lo']:.4f},{row['b_minus_a_spearman_hi']:.4f}] "
                      f"c-b={row['c_minus_b_spearman']:.4f} "
                      f"95%CI=[{row['c_minus_b_spearman_lo']:.4f},{row['c_minus_b_spearman_hi']:.4f}]")
        for side, frames in results.items():
            suffix = '_gbm' if args.model == 'gbm' else ''
            pl.concat(frames).write_csv(out / f'{sym}_{side}{suffix}.csv')
