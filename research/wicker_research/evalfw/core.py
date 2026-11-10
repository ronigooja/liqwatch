"""Evaluation helpers. Inputs are held-out observations, not model-selection policy."""

import logging
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score


def _seconds(ts):
    x = np.asarray(ts)
    if np.issubdtype(x.dtype, np.datetime64):
        return x.astype('datetime64[ns]').astype('int64') / 1e9
    return x.astype(float)


def time_split(ts, fractions, purge=0, embargo=0):
    """Chronological indices; purge is label horizon, embargo is time after boundary.

    Fractions determine boundaries by sample count before gaps. A label ending at
    a boundary is considered overlapping. Duplicate boundary timestamps remain
    together on the later side and may be removed by the embargo.
    """
    t = _seconds(ts)
    f = np.asarray(fractions, dtype=float)
    if len(f) != 3 or np.any(f <= 0) or not np.isclose(f.sum(), 1) or purge < 0 or embargo < 0:
        raise ValueError('three positive fractions summing to one and nonnegative gaps required')
    order = np.argsort(t, kind='stable')
    a, b = int(len(t) * f[0]), int(len(t) * (f[0] + f[1]))
    if not 0 < a < b < len(t):
        raise ValueError('not enough samples for three splits')
    x, y = t[order[a]], t[order[b]]
    return (order[t[order] + purge < x],
            order[(t[order] >= x + embargo) & (t[order] + purge < y)],
            order[t[order] >= y + embargo])


def walk_forward(ts, min_train, test_size, purge=0, embargo=0):
    """Expanding training windows and consecutive test windows, sizes in samples."""
    t = _seconds(ts)
    if min_train < 1 or test_size < 1 or purge < 0 or embargo < 0:
        raise ValueError('invalid fold sizes or gaps')
    order = np.argsort(t, kind='stable')
    for start in range(min_train, len(t), test_size):
        boundary = t[order[start]]
        end = min(start + test_size, len(t))
        train = order[:start]
        test = order[start:end]
        yield train[t[train] + purge < boundary], test[t[test] >= boundary + embargo]


class SealedTest:
    """Explicit access gate, not a security boundary; each unseal is logged."""
    def __init__(self, data):
        self._data = data
        self._open = False

    def unseal(self):
        logging.getLogger(__name__).warning('Sealed test unsealed')
        self._open = True

    @property
    def data(self):
        if not self._open:
            raise RuntimeError('sealed test must be explicitly unsealed')
        return self._data


def brier(y, p):
    return float(np.mean((np.asarray(y) - np.asarray(p)) ** 2))


def log_loss(y, p):
    p = np.clip(np.asarray(p, dtype=float), 1e-15, 1 - 1e-15)
    y = np.asarray(y)
    return float(-np.mean(y * np.log(p) + (1-y) * np.log1p(-p)))


def pr_auc(y, p):
    return float(average_precision_score(y, p))


def roc_auc(y, p):
    return float(roc_auc_score(y, p))


def reliability_curve(y, p, bins=10):
    y, p = np.asarray(y), np.asarray(p)
    if bins < 1 or np.any((p < 0) | (p > 1)) or len(y) != len(p):
        raise ValueError('invalid probabilities or bins')
    index = np.minimum((p * bins).astype(int), bins - 1)
    return [{'count': int(np.sum(index == i)), 'mean_probability': float(np.mean(p[index == i])),
             'fraction_positive': float(np.mean(y[index == i]))}
            for i in range(bins) if np.any(index == i)]


def ece(y, p, bins=10):
    return sum(r['count'] * abs(r['mean_probability'] - r['fraction_positive'])
               for r in reliability_curve(y, p, bins)) / len(y)


def recall_at_fa_budget(y, p, ts, budget):
    """Max sample recall under a false-alarm/hour budget, with score ties intact.

    An alarm is one sample with score >= threshold; a false alarm is such a
    sample with y=0. Hours are (max timestamp - min timestamp)/3600, including
    gaps, not summed active time. Threshold selection uses the supplied data;
    choose it on tuning data, never on the sealed test. Returns recall,
    threshold and achieved false alarms/hour; no alarms is always eligible.
    """
    y, p, t = np.asarray(y), np.asarray(p), _seconds(ts)
    if len(y) != len(p) or len(y) != len(t) or len(y) < 2 or budget < 0 or np.ptp(t) <= 0 or not np.any(y == 1):
        raise ValueError('need aligned samples, positive duration, positives and nonnegative budget')
    hours = np.ptp(t) / 3600
    best = (0., float('inf'), 0.)
    for threshold in np.unique(p)[::-1]:
        alarm = p >= threshold
        rate = float(np.sum(alarm & (y == 0)) / hours)
        recall = float(np.sum(alarm & (y == 1)) / np.sum(y == 1))
        if rate <= budget and recall > best[0]:
            best = (recall, float(threshold), rate)
    return best


def lead_time(event_starts, alarm_times, lookback):
    """For each event use its earliest alarm within [start-lookback, start)."""
    events, alarms = _seconds(event_starts), _seconds(alarm_times)
    if lookback < 0:
        raise ValueError('lookback must be nonnegative')
    leads = [e - np.min(a) for e in events if len(a := alarms[(alarms >= e-lookback) & (alarms < e)])]
    return {'p50': float(np.percentile(leads, 50)), 'p90': float(np.percentile(leads, 90))} if leads else {'p50': float('nan'), 'p90': float('nan')}


def pinball_loss(y, prediction, quantile):
    if not 0 < quantile < 1:
        raise ValueError('quantile must be in (0, 1)')
    d = np.asarray(y) - np.asarray(prediction)
    return float(np.mean(np.maximum(quantile*d, (quantile-1)*d)))


def coverage(y, lower, upper):
    y, lower, upper = np.asarray(y), np.asarray(lower), np.asarray(upper)
    return float(np.mean((lower <= y) & (y <= upper)))


def mean_width(lower, upper):
    return float(np.mean(np.asarray(upper) - np.asarray(lower)))


def non_crossing(quantiles):
    return bool(np.all(np.diff(np.asarray(quantiles), axis=-1) >= 0))


def multiclass_brier(y, p):
    p = np.asarray(p)
    return float(np.mean(np.sum((p - np.eye(p.shape[1])[np.asarray(y, dtype=int)])**2, axis=1)))


def multiclass_log_loss(y, p):
    p = np.asarray(p)
    return float(-np.mean(np.log(np.clip(p[np.arange(len(p)), np.asarray(y, dtype=int)], 1e-15, 1))))


def unresolved_share(unresolved):
    return float(np.mean(unresolved))


def trading_metrics(profit, costs=0):
    """Profit and costs are per-trade values in a common additive unit."""
    x = np.asarray(profit, dtype=float) - np.asarray(costs, dtype=float)
    wins, losses = x[x > 0], x[x < 0]
    equity = np.r_[0., np.cumsum(x)]
    n = max(1, int(np.ceil(.05 * len(x))))
    return {'expectancy': float(np.mean(x)), 'win_rate': float(np.mean(x > 0)),
            'payoff_ratio': float(np.mean(wins) / -np.mean(losses)) if len(wins) and len(losses) else float('nan'),
            'max_drawdown': float(np.max(np.maximum.accumulate(equity) - equity)),
            'cvar_5': float(np.mean(np.sort(x)[:n]))}


def bootstrap_expectancy(profit, days, costs=0, n_boot=2000, confidence=.95, seed=None):
    """Resample entire days with replacement, retaining all trades within each day."""
    x, days = np.asarray(profit, dtype=float) - np.asarray(costs), np.asarray(days)
    if len(x) != len(days) or not len(x) or n_boot < 1 or not 0 < confidence < 1:
        raise ValueError('invalid bootstrap inputs')
    _, inverse = np.unique(days, return_inverse=True)
    blocks = [x[inverse == i] for i in range(inverse.max()+1)]
    rng = np.random.default_rng(seed)
    means = [np.mean(np.concatenate([blocks[i] for i in rng.integers(len(blocks), size=len(blocks))])) for _ in range(n_boot)]
    return tuple(float(v) for v in np.quantile(means, [(1-confidence)/2, (1+confidence)/2]))


def group_report(profit, groups, costs=0):
    """Report each provided grouping separately, e.g. tier, month and regime."""
    x = np.asarray(profit, dtype=float) - np.asarray(costs)
    result = {}
    for name, values in groups.items():
        values = np.asarray(values)
        if len(values) != len(x):
            raise ValueError('groups must align with trades')
        result[name] = {str(v): trading_metrics(x[values == v]) for v in np.unique(values)}
    return result


def extreme_profit_share(profit, k, threshold):
    """Return top-k net-profit share and flag when it meets the chosen threshold."""
    x = np.asarray(profit, dtype=float)
    if not len(x) or k < 1 or not 0 <= threshold <= 1:
        raise ValueError('invalid concentration parameters')
    total = x.sum()
    share = float(np.sort(x)[-k:].sum() / total) if total > 0 else float('nan')
    return {'share': share, 'flag': bool(total > 0 and share >= threshold)}


class BaselineS:
    """Nested categorical frequencies: (cell positives + m * parent rate)/(cell count + m).

    Columns run from coarsest to finest; the global empirical rate is the root.
    Unseen cells fall back to their nearest observed parent.
    """
    def __init__(self, m=10):
        if m < 0:
            raise ValueError('m must be nonnegative')
        self.m = m

    def fit(self, X, y):
        X, y = np.asarray(X), np.asarray(y, dtype=float)
        if X.ndim != 2 or len(X) != len(y) or not len(y):
            raise ValueError('expected nonempty aligned matrix and labels')
        self.root = float(np.mean(y))
        self.rates = {}
        self.n_features = X.shape[1]
        for depth in range(1, X.shape[1]+1):
            cells = {}
            for row, value in zip(X, y):
                cells.setdefault(tuple(row[:depth]), []).append(value)
            for key, vals in cells.items():
                parent = self.rates.get(key[:-1], self.root)
                self.rates[key] = (sum(vals) + self.m * parent) / (len(vals) + self.m)
        return self

    def predict_proba(self, X):
        X = np.asarray(X)
        if X.ndim != 2 or X.shape[1] != self.n_features:
            raise ValueError('feature dimensions do not match fit')
        return np.array([next((self.rates[tuple(row[:d])] for d in range(self.n_features, 0, -1) if tuple(row[:d]) in self.rates), self.root) for row in X])


class BaselineL:
    """Logistic regression probability baseline; calibration must be evaluated out of sample."""
    def __init__(self, **kwargs):
        self.model = LogisticRegression(**kwargs)

    def fit(self, X, y):
        self.model.fit(X, y)
        return self

    def predict_proba(self, X):
        return self.model.predict_proba(X)[:, 1]


def report(*, classification=None, range_data=None, first_touch=None, trading=None):
    """Produce a nested metrics dict for the supplied task inputs.

    Optional dictionaries contain metric inputs; grouped range coverage is
    provided by calling coverage on each regime. Trading capacity and real
    execution costs require external data and are not inferred here.
    """
    out = {}
    if classification is not None:
        y, p = classification['y'], classification['p']
        out['classification'] = {'brier': brier(y, p), 'log_loss': log_loss(y, p), 'pr_auc': pr_auc(y, p), 'roc_auc': roc_auc(y, p), 'reliability': reliability_curve(y, p), 'ece': ece(y, p)}
        if 'budget' in classification:
            out['classification']['recall_at_fa_budget'] = recall_at_fa_budget(y, p, classification['ts'], classification['budget'])
        if 'event_starts' in classification:
            out['classification']['lead_time'] = lead_time(classification['event_starts'], classification['alarm_times'], classification['lookback'])
    if range_data is not None:
        r = range_data
        out['range'] = {'pinball': {str(q): pinball_loss(r['y'], r['predictions'][:, i], q) for i, q in enumerate(r['quantiles'])}, 'coverage': coverage(r['y'], r['lower'], r['upper']), 'mean_width': mean_width(r['lower'], r['upper']), 'non_crossing': non_crossing(r['predictions'])}
        if 'groups' in r:
            out['range']['conditional_coverage'] = {name: {str(v): coverage(np.asarray(r['y'])[np.asarray(g) == v], np.asarray(r['lower'])[np.asarray(g) == v], np.asarray(r['upper'])[np.asarray(g) == v]) for v in np.unique(g)} for name, g in r['groups'].items()}
    if first_touch is not None:
        f = first_touch
        out['first_touch'] = {'brier': multiclass_brier(f['y'], f['p']), 'log_loss': multiclass_log_loss(f['y'], f['p']), 'unresolved_share': unresolved_share(f['unresolved'])}
    if trading is not None:
        tr = trading
        out['trading'] = trading_metrics(tr['profit'], tr.get('costs', 0))
        if 'groups' in tr:
            out['trading']['groups'] = group_report(tr['profit'], tr['groups'], tr.get('costs', 0))
        if 'days' in tr:
            out['trading']['expectancy_ci'] = bootstrap_expectancy(tr['profit'], tr['days'], tr.get('costs', 0), seed=tr.get('seed'))
    return out
