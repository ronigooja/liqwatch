import logging

import numpy as np
import pytest

from wicker_research.evalfw import (
    BaselineL, BaselineS, SealedTest, bootstrap_expectancy, brier, coverage,
    ece, extreme_profit_share, group_report, lead_time, log_loss, mean_width,
    multiclass_brier, multiclass_log_loss, non_crossing, pinball_loss, pr_auc,
    recall_at_fa_budget, reliability_curve, report, roc_auc, time_split,
    trading_metrics, unresolved_share, walk_forward,
)


def test_splits_and_folds():
    ts = np.arange(100.)
    train, tune, test = time_split(ts, (.6, .2, .2), 3, 2)
    assert ts[train].max() + 3 < 60
    assert ts[tune].min() >= 62 and ts[tune].max() + 3 < 80
    assert ts[test].min() >= 82
    assert not set(train) & set(tune) and not set(tune) & set(test)
    for train, test in walk_forward(ts, 40, 20, 3, 2):
        assert ts[train].max() + 3 < ts[test].min()
        assert not set(train) & set(test)


def test_sealed(caplog):
    s = SealedTest([1])
    with pytest.raises(RuntimeError):
        _ = s.data
    with caplog.at_level(logging.WARNING):
        s.unseal()
        s.unseal()
    assert s.data == [1] and len(caplog.records) == 2


def test_binary():
    y, p = [0, 1], [.25, .75]
    assert brier(y, p) == pytest.approx(.0625)
    assert log_loss(y, p) == pytest.approx(-np.log(.75))
    assert pr_auc(y, p) == 1 and roc_auc(y, p) == 1
    assert sum(x['count'] for x in reliability_curve(y, p)) == 2
    assert ece(y, p) == pytest.approx(.25)
    assert ece(np.tile([0, 1], 5000), np.full(10000, .5)) == 0
    recall, threshold, rate = recall_at_fa_budget([1, 0, 1, 0], [.9, .8, .7, .1], [0, 1200, 2400, 3600], 1)
    assert (recall, threshold, rate) == (1, .7, 1)
    assert lead_time([100, 200], [90, 195], 20) == {'p50': 7.5, 'p90': 9.5}


def test_ranges_multiclass():
    assert pinball_loss([1, 3], [2, 2], .5) == .5
    assert coverage([1, 3], [0, 0], [2, 2]) == .5
    assert mean_width([0, 1], [2, 4]) == 2.5
    assert non_crossing([[1, 2], [0, 0]]) and not non_crossing([[2, 1]])
    p = np.array([[.5, .5, 0], [.1, .2, .7]])
    assert multiclass_brier([0, 2], p) == pytest.approx(.32)
    assert multiclass_log_loss([0, 2], p) == pytest.approx(-np.log(.35)/2)
    assert unresolved_share([True, False]) == .5


def test_trading_and_groups():
    x = [3, -2, 1]
    r = trading_metrics(x, 1)
    assert r['expectancy'] == pytest.approx(-1/3)
    assert r['win_rate'] == pytest.approx(1/3)
    assert r['payoff_ratio'] == pytest.approx(2/3)
    assert r['max_drawdown'] == 3 and r['cvar_5'] == -3
    assert group_report(x, {'tier': ['a', 'b', 'a']})['tier']['a']['expectancy'] == 2
    assert extreme_profit_share([10, -3, 1], 1, .8)['flag']


def test_bootstrap():
    rng = np.random.default_rng(123)
    x = rng.normal(2, 1, 600)
    lo, hi = bootstrap_expectancy(x, np.repeat(np.arange(60), 10), seed=10)
    assert lo < 2 < hi


def test_baselines():
    X = np.array([['a', 'x'], ['a', 'x'], ['a', 'y'], ['b', 'z']])
    model = BaselineS(m=2).fit(X, [1, 1, 0, 0])
    root = .5
    parent = (2 + 2*root)/5
    assert model.predict_proba([['a', 'x'], ['a', 'unknown'], ['c', 'z']]) == pytest.approx([(2+2*parent)/4, parent, root])
    logistic = BaselineL().fit([[0], [1], [2], [3]], [0, 0, 1, 1])
    assert logistic.predict_proba([[0], [3]])[0] < logistic.predict_proba([[0], [3]])[1]


def test_report():
    r = report(classification={'y': [0, 1], 'p': [.2, .8]},
               range_data={'y': [1, 2], 'predictions': np.array([[0, 2], [1, 3]]), 'quantiles': [.1, .9], 'lower': [0, 1], 'upper': [2, 3], 'groups': {'regime': ['a', 'b']}},
               first_touch={'y': [0, 1], 'p': np.array([[.8, .2], [.1, .9]]), 'unresolved': [False, True]},
               trading={'profit': [1, -1], 'groups': {'tier': ['a', 'b']}})
    assert r['range']['conditional_coverage']['regime']['a'] == 1
    assert r['first_touch']['unresolved_share'] == .5
    assert r['trading']['groups']['tier']['a']['expectancy'] == 1
