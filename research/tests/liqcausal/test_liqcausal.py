"""Causal features, train-only fits, metrics and temporal sensitivity checks."""
import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest
from polars.testing import assert_frame_equal

from wicker_research.liqcal import INPUT_SCHEMA
from wicker_research.liqcausal import (
    DAY, NS, average_precision, bootstrap_differences, evaluate, gbm_predict, load_day, main,
    map_features, metrics, moments, ridge_predict, roc_auc, rolling_pairs,
    split_moments,
)
from wicker_research.liqmap import LiqMap


def data():
    trades = pl.DataFrame({'rw': [0, 60 * NS], 'price': [100., 101.],
                           'qty': [2., 3.], 'buyer_is_maker': [False, True]})
    mark = pl.DataFrame({'rw': np.arange(181, dtype=np.int64) * NS,
                         'mark': 100. + np.sin(np.arange(181) / 10)})
    liq = pl.DataFrame({'rw': [0, 30 * NS, 60 * NS, 90 * NS, 120 * NS],
                        'side': ['SELL', 'SELL', 'BUY', 'SELL', 'BUY'],
                        'avg_price': [100., 90., 110., 80., 120.],
                        'filled_qty': [1., 2., 3., 4., 5.]})
    return trades, mark, liq


def features(frame):
    return frame.drop([c for c in frame.columns if c.startswith(('ext_',)) or c.endswith('_usd')])


def test_features_causal_append_and_future_marks():
    original = data()
    before = moments(*original, 'A', horizon_s=60)
    extended = [pl.concat([df, df.tail(1).with_columns(pl.lit(300 * NS).alias('rw'))])
                for df in original]
    after = moments(*extended, 'A', horizon_s=60).filter(pl.col('t') <= before['t'].max())
    assert_frame_equal(features(before), features(after))
    tr, mark, liq = original
    changed = mark.with_columns(pl.when(pl.col('rw') > 60 * NS).then(200.).otherwise(
        pl.col('mark')).alias('mark'))
    new = moments(tr, changed, liq, 'A', horizon_s=60)
    assert_frame_equal(features(before.filter(pl.col('t') <= 60 * NS)),
                       features(new.filter(pl.col('t') <= 60 * NS)))
    assert new['ext_up_bp'][1] != before['ext_up_bp'][1]


def test_map_features_hand_computed_boundaries():
    q = LiqMap(np.array([99.8, 99.9, 99.95, 100., 101.]), np.array([11., 20., 30., 40., 50.]),
               np.array([99., 100., 100.05, 100.1, 100.2]), np.array([7., 9., 13., 17., 19.]))
    row = map_features(q, 100., sigma=.001)
    assert row['mass_down_10'] == 50
    assert row['mass_up_10'] == 30
    assert row['total_down'] == 151
    assert row['total_up'] == 65
    assert row['nearest_down_bp'] == pytest.approx(5.)
    assert row['nearest_up_bp'] == pytest.approx(5.)
    assert row['mass_down_1sigma'] == 50
    assert row['mass_up_2sigma'] == 49
    strict = map_features(q, 100., min_cluster=31.)
    assert np.isnan(strict['nearest_down_bp'])
    assert np.isnan(strict['nearest_up_bp'])
    assert np.isnan(strict['mass_down_1sigma'])
    with pytest.raises(ValueError):
        map_features(q, 100., distances_bp=[0])


def test_baselines_outcomes_and_invalid(caplog):
    tr, mark, liq = data()
    invalid = liq.head(1).with_columns(pl.lit(10 * NS).alias('rw'), pl.lit(float('nan')).alias('avg_price'))
    liq = pl.concat([liq, invalid])
    result = moments(tr, mark, liq, 'A', horizon_s=60)
    zero, sixty = result.row(0, named=True), result.row(1, named=True)
    assert zero['liq_down_usd'] == 180.
    assert zero['liq_up_usd'] == 330.
    assert zero['liq_5m_down'] == 100.
    assert sixty['liq_down_usd'] == 320.
    assert sixty['liq_up_usd'] == 600.
    assert sixty['liq_5m_down'] == 280.
    assert sixty['liq_5m_up'] == 330.
    assert sixty['volume_5m'] == sixty['volume_60m'] == 503.
    assert sixty['signed_flow_5m'] == -103.
    assert sixty['hour_of_day'] == pytest.approx(1 / 60)
    path = mark.filter((pl.col('rw') > 60 * NS) & (pl.col('rw') <= 120 * NS))['mark'].to_numpy()
    m0 = mark['mark'][60]
    assert sixty['ext_down_bp'] == pytest.approx(max(0, (m0 - path.min()) / m0 * 10000))
    assert sixty['ext_up_bp'] == pytest.approx(max(0, (path.max() - m0) / m0 * 10000))
    assert sixty['realized_vol_5m'] == pytest.approx(np.std(np.diff(np.log(mark['mark'].head(61)))))
    assert 'Dropped 1 liquidation records' in caplog.text
    assert 'time_to_next_funding' not in result.columns
    assert 'oi_change_60m' not in result.columns


def test_optional_past_funding_oi_and_bucket_causality():
    tr, mark, liq = data()
    tr = tr.with_columns(pl.col('rw') + 3600 * NS)
    mark = pl.concat([mark.head(1), mark.with_columns(pl.col('rw') + 3600 * NS)]).with_columns(
        pl.lit(7200000).alias('next_funding_T'))
    oi = (np.array([0, 3600000, 3660000]), np.array([10., 14., 16.]))
    result = moments(tr, mark, liq, 'A', horizon_s=60, params={'open_interest': oi})
    row = result.row(0, named=True)
    assert row['oi_change_60m'] == 4.
    assert row['time_to_next_funding'] == 3600.
    modified = (np.r_[oi[0], 4000000], np.r_[oi[1], 1e8])
    other = moments(tr, mark, liq, 'A', horizon_s=60, params={'open_interest': modified})
    assert_frame_equal(features(result), features(other))


def test_bucket_oi_and_brackets_forwarded():
    from wicker_research.liqmap import Brackets

    tr, _, liq = data()
    tr = tr.head(1).with_columns(pl.lit(NS).alias('rw'))
    mark = pl.DataFrame({'rw': [0, 300 * NS, 360 * NS], 'mark': [100., 100., 100.]})
    brackets = Brackets(np.array([0.]), np.array([.01]), np.array([0.]), np.array([10.]))
    oi = (np.array([0, 300000, 600000]), np.array([100., 103., 1000.]))
    params = {'brackets': brackets, 'leverage_weights': {10: 1},
              'oi_mode': 'bucket', 'open_interest': oi}
    result = moments(tr, mark, liq, 'A', horizon_s=60, params=params)
    assert result.filter(pl.col('t') == 300 * NS)['total_down'].item() == 300.
    assert result.filter(pl.col('t') < 300 * NS)['total_down'].sum() == 0.
    params['open_interest'] = (oi[0][:2], oi[1][:2])
    assert_frame_equal(features(result), features(moments(tr, mark, liq, 'A', horizon_s=60, params=params)))


def world(seed=0, signal=True, n=1200):
    rng = np.random.default_rng(seed)
    baseline = rng.uniform(0, 2, n)
    local = rng.uniform(0, 2, n)
    total = local + rng.uniform(2, 6, n)
    log_outcome = .3 * baseline + (2 * local if signal else 0) + rng.normal(0, .4, n)
    frame = pl.DataFrame({'t': np.arange(n, dtype=np.int64) * 60 * NS, 'sym': ['A'] * n,
                          'volume_5m': np.expm1(baseline), 'total_down': np.expm1(total),
                          'total_up': rng.uniform(0, 100, n), 'mass_down_10': np.expm1(local),
                          'liq_down_usd': np.exp(log_outcome), 'liq_up_usd': rng.uniform(0, 10, n)})
    train, test = frame.head(n // 2), frame.tail(n // 2).with_columns(pl.col('t') + DAY)
    return train, test


def test_threshold_training_only_and_preprocessing():
    train, test = world(n=200)
    first = evaluate(train, test, 'down', n_boot=3)
    changed = test.with_columns(pl.col('liq_down_usd') * 1000)
    second = evaluate(train, changed, 'down', n_boot=3)
    assert first['threshold'].to_list() == second['threshold'].to_list()
    assert first['threshold'][0] == pytest.approx(np.quantile(train['liq_down_usd'], .9))
    columns = ['volume_5m', 'total_down', 'mass_down_10']
    p = ridge_predict(train, test, columns, 'liq_down_usd')
    extreme = pl.concat([test, test.tail(1).with_columns(pl.lit(1e20).alias('volume_5m'))])
    np.testing.assert_allclose(p, ridge_predict(train, extreme, columns, 'liq_down_usd')[:-1])


def test_purge_embargo_and_split_horizons():
    boundary = DAY
    times = np.array([0, DAY - 301 * NS, DAY - 300 * NS, DAY - NS, DAY,
                      DAY + 1799 * NS, DAY + 1800 * NS, 2 * DAY - 300 * NS])
    frame = pl.DataFrame({'t': times})
    train, test = split_moments(frame, ['1970-01-01'], ['1970-01-02'])
    assert train['t'].to_list() == [0, DAY - 301 * NS]
    assert test['t'].to_list() == [boundary + 1800 * NS]
    _, no_embargo = split_moments(frame, ['1970-01-01'], ['1970-01-02'], embargo_minutes=0)
    assert no_embargo['t'].to_list() == [DAY, DAY + 1799 * NS, DAY + 1800 * NS]
    with pytest.raises(ValueError):
        split_moments(frame, ['1970-01-02'], ['1970-01-01'])
    frame = pl.DataFrame({'t': [DAY - 60 * NS, DAY + 60 * NS, 3 * DAY]})
    train, _ = split_moments(frame, ['1970-01-01', '1970-01-02'], ['1970-01-04'])
    assert train.height == 2


@pytest.mark.parametrize('seed', range(10))
def test_auc_ap_bruteforce_ties(seed):
    rng = np.random.default_rng(seed)
    labels = rng.random(90) < .3
    scores = rng.integers(0, 12, len(labels)).astype(float)
    positives, negatives = scores[labels], scores[~labels]
    expected_auc = np.mean([float(a > b) + .5 * float(a == b) for a in positives for b in negatives])
    expected_ap = 0.
    for score in np.unique(scores):
        selected = scores >= score
        expected_ap += ((labels & (scores == score)).sum() / labels.sum()
                        * labels[selected].mean())
    assert roc_auc(labels, scores) == pytest.approx(expected_auc)
    assert average_precision(labels, scores) == pytest.approx(expected_ap)


def test_metrics_empty_classes_and_budget_ties():
    assert np.isnan(roc_auc([False, False], [1., 2.]))
    assert np.isnan(average_precision([False, False], [1., 2.]))
    row = metrics(np.ones(60), np.r_[np.ones(10), np.zeros(50)], .5)
    assert row['budget_1_recall'] == 0
    assert row['budget_1_alerts_per_hour'] == 0
    row = metrics(np.arange(60), np.r_[np.zeros(50), np.ones(10)], .5)
    assert row['budget_1_recall'] == 1
    assert row['budget_1_alerts_per_hour'] == 11
    assert row['budget_1_false_alarms_per_hour'] == 1


def test_bootstrap_seed_days_and_single_block():
    train, test = world(n=300)
    y = test['liq_down_usd'].to_numpy()
    rng = np.random.default_rng(9)
    predictions = {name: rng.normal(size=len(y)) for name in 'abc'}
    kwargs = {'n_boot': 12, 'seed': 41}
    args = (test['t'].to_numpy(), predictions, y, np.quantile(y, .9))
    a, b = bootstrap_differences(*args, **kwargs), bootstrap_differences(*args, **kwargs)
    assert a == b
    c = bootstrap_differences(*args, n_boot=12, seed=42)
    assert a != c
    daily = np.arange(len(y), dtype=np.int64) % 2 * DAY
    days = bootstrap_differences(daily, predictions, y, .5, n_boot=2)
    assert days['bootstrap_block_minutes'] == 1440
    assert days['bootstrap_blocks'] == 2
    short = bootstrap_differences(np.zeros(len(y), dtype=np.int64), predictions, y, .5, n_boot=2)
    assert np.isnan(short['c_minus_b_spearman_lo'])


def test_localised_signal_beats_total_and_sensitivities():
    train, test = world()
    result = evaluate(train, test, 'down', n_boot=25)
    row = result.row(0, named=True)
    assert row['c_minus_b_spearman'] > .3
    assert row['c_minus_b_spearman_lo'] > 0
    assert row['c_pr_auc'] > row['b_pr_auc']
    assert result['variant'].to_list() == ['all', 'without_largest_hour', 'nonoverlap']
    assert result['n_test'][2] == test.filter((pl.col('t') // (60 * NS)) % 5 == 0).height
    without = result.row(1, named=True)
    excluded = test.filter(pl.col('t') // (3600 * NS) == row['largest_event_hour_ns'] // (3600 * NS))
    assert without['n_test'] == test.height - excluded.height


def test_no_localised_signal_majority_of_seeds():
    no_claim = []
    for seed in range(7):
        train, test = world(seed=seed, signal=False)
        row = evaluate(train, test, 'down', n_boot=30, seed=seed).row(0, named=True)
        no_claim.append(row['c_minus_b_spearman_lo'] <= 0)
    assert sum(no_claim) > len(no_claim) // 2


def test_load_day_funding_multi_root_and_symbol(tmp_path):
    tr, mark, liq = data()
    for i in range(2):
        root = tmp_path / str(i)
        for name, frame in zip(INPUT_SCHEMA, [tr, mark, liq]):
            path = root / name / 'sym=A'
            path.mkdir(parents=True)
            if i == 0 and name == 'mark':
                frame = frame.with_columns(pl.lit(1000000, dtype=pl.Int64).alias('next_funding_T'))
            pq.write_table(frame.to_arrow(), path / 'part.parquet')
    loaded = load_day([tmp_path / '0', tmp_path / '1'], 'A')
    assert loaded[1].height == 362
    assert loaded[1]['next_funding_T'].null_count() == 181
    assert loaded[1]['next_funding_T'].head(4).to_list() == [1000000, None, 1000000, None]
    assert load_day(tmp_path / '0', 'B')[1].is_empty()


def test_rolling_pairs_and_cli(tmp_path, monkeypatch, capsys):
    assert rolling_pairs(['1970-01-03', '1970-01-01', '1970-01-02', '1970-01-05']) == [
        (['1970-01-01'], ['1970-01-02']), (['1970-01-02'], ['1970-01-03'])]
    tr, mark, liq = data()
    roots = []
    for i in range(3):
        roots.append(f'--root=1970-01-0{i + 1}={tmp_path / str(i)}')

    def fake_load(roots, sym):
        i = int(roots[0].split('/')[-1])
        return tuple(df.with_columns(pl.col('rw') + i * DAY) for df in (tr, mark, liq))

    monkeypatch.setattr('wicker_research.liqcausal.load_day', fake_load)
    out = tmp_path / 'scores'
    main([*roots, '--rolling', '--syms', 'A', '--out', str(out), '--step', '60',
          '--horizon', '60', '--embargo-minutes', '0', '--bootstrap', '2'])
    scored = pl.read_csv(out / 'A_down.csv')
    assert scored.height == 6
    assert scored['train_dates'].unique().sort().to_list() == ['1970-01-01', '1970-01-02']
    assert scored['test_dates'].unique().sort().to_list() == ['1970-01-02', '1970-01-03']
    assert pl.read_csv(out / 'A_up.csv').height == 6
    assert len(capsys.readouterr().out.splitlines()) == 4
    main([*roots, '--train', '1970-01-01', '--test', '1970-01-03', '--syms', 'A',
          '--out', str(out), '--horizon', '60', '--bootstrap', '2'])
    assert pl.read_csv(out / 'A_down.csv').height == 3


def test_empty_symbol_filter_validation():
    frames = tuple(pl.DataFrame(schema=schema) for schema in INPUT_SCHEMA.values())
    assert moments(*frames, 'A').is_empty()
    with pytest.raises(ValueError):
        moments(*data(), 'A', step_s=0)
    tr, mark, liq = data()
    originals = moments(tr, mark, liq, 'A', horizon_s=60)
    mixed = [pl.concat([df.with_columns(pl.lit('A').alias('sym')),
                        df.with_columns(pl.lit('B').alias('sym'))]) for df in data()]
    assert_frame_equal(originals, moments(*mixed, 'A', horizon_s=60))
    train, test = world(n=200)
    with pytest.raises(ValueError):
        evaluate(test, train, 'down', n_boot=2)


def test_gbm_localised_signal_and_determinism():
    train, test = world()
    first = evaluate(train, test, 'down', model='gbm', n_boot=25)
    second = evaluate(train, test, 'down', model='gbm', n_boot=25)
    assert_frame_equal(first, second, check_exact=True)
    assert first['model'].to_list() == ['gbm'] * 3
    assert first['gbm_dropped_columns'].to_list() == [''] * 3
    row = first.row(0, named=True)
    assert row['c_minus_b_spearman'] > .3
    assert row['c_minus_b_spearman_lo'] > 0
    assert row['c_pr_auc'] > row['b_pr_auc']


def test_gbm_drops_missing_and_constant_training_columns(caplog):
    train, test = world()
    train = train.with_columns(pl.lit(float('nan')).alias('missing'),
                               pl.lit(7.).alias('constant'),
                               pl.when(pl.int_range(pl.len()) % 2 == 0).then(None)
                               .otherwise(3.).alias('one_finite_value'))
    # Test-only variation must not rescue an unusable training column.
    test = test.with_columns(pl.int_range(pl.len()).cast(pl.Float64).alias('missing'),
                             pl.lit(99.).alias('constant'),
                             pl.lit(-4.).alias('one_finite_value'))
    dropped = ['missing', 'constant', 'one_finite_value']
    columns = ['volume_5m', *dropped, 'mass_down_10']
    prediction = gbm_predict(train, test, columns, 'liq_down_usd')
    expected = gbm_predict(train, test, ['volume_5m', 'mass_down_10'], 'liq_down_usd')
    np.testing.assert_array_equal(prediction, expected)
    warnings = [r for r in caplog.records if 'Dropped GBM' in r.message]
    assert len(warnings) == 1
    assert all(c in warnings[0].message for c in dropped)


@pytest.mark.parametrize('columns', [[], ['missing', 'constant']])
def test_gbm_no_usable_columns_predicts_training_log_mean(columns):
    train, test = world(n=200)
    train = train.with_columns(pl.lit(float('inf')).alias('missing'),
                               pl.lit(7.).alias('constant'))
    test = test.with_columns(pl.lit(1.).alias('missing'), pl.lit(99.).alias('constant'))
    prediction = gbm_predict(train, test, columns, 'liq_down_usd')
    np.testing.assert_array_equal(prediction, np.full(test.height,
                                 np.log1p(train['liq_down_usd'].to_numpy()).mean()))


def test_gbm_dropped_columns_recorded_per_variant_and_csv(tmp_path):
    train, test = world(n=200)
    train = train.with_columns(pl.lit(float('nan')).alias('time_to_next_funding'),
                               pl.lit(0.).alias('total_up'),
                               pl.lit(float('nan')).alias('nearest_down_bp'))
    test = test.with_columns(pl.lit(1.).alias('time_to_next_funding'),
                             pl.lit(1.).alias('nearest_down_bp'))
    result = evaluate(train, test, 'down', model='gbm', n_boot=2)
    expected = {'a_gbm_dropped_columns': 'time_to_next_funding',
                'b_gbm_dropped_columns': 'time_to_next_funding,total_up',
                'c_gbm_dropped_columns': 'time_to_next_funding,total_up,nearest_down_bp',
                'gbm_dropped_columns': 'time_to_next_funding,total_up,nearest_down_bp'}
    for key, value in expected.items():
        assert result[key].to_list() == [value] * 3
    path = tmp_path / 'scores.csv'
    result.write_csv(path)
    assert_frame_equal(result.select(list(expected)), pl.read_csv(path).select(list(expected)))
    ridge = evaluate(train, test, 'down', n_boot=2)
    assert ridge['gbm_dropped_columns'].to_list() == [''] * 3


def test_ridge_missing_and_constant_columns_are_harmless():
    train, test = world(n=200)
    train = train.with_columns(pl.lit(float('nan')).alias('missing'),
                               pl.lit(7.).alias('constant'))
    test = test.with_columns(pl.lit(123.).alias('missing'), pl.lit(99.).alias('constant'))
    expected = ridge_predict(train, test, ['volume_5m'], 'liq_down_usd')
    actual = ridge_predict(train, test, ['volume_5m', 'missing', 'constant'], 'liq_down_usd')
    np.testing.assert_allclose(actual, expected, rtol=0, atol=1e-14)


def test_gbm_no_localised_signal_majority_of_seeds():
    no_claim = []
    for seed in range(7):
        train, test = world(seed=seed, signal=False)
        row = evaluate(train, test, 'down', model='gbm', n_boot=30, seed=seed).row(0, named=True)
        no_claim.append(row['c_minus_b_spearman_lo'] <= 0)
    assert sum(no_claim) > len(no_claim) // 2


def test_gbm_nonlinear_baselines_beat_ridge():
    rng = np.random.default_rng(10)
    n = 4000
    x, z = rng.uniform(-1, 1, (2, n))
    frame = pl.DataFrame({
        't': np.arange(n, dtype=np.int64) * 60 * NS, 'sym': ['A'] * n,
        'realized_vol_5m': x + 1, 'realized_vol_60m': z + 1,
        'total_down': rng.uniform(0, 100, n), 'total_up': rng.uniform(0, 100, n),
        'mass_down_10': rng.uniform(0, 10, n),
        'liq_down_usd': np.expm1(2 + 2 * x * z + rng.normal(0, .05, n)),
        'liq_up_usd': rng.uniform(0, 10, n),
    })
    train, test = frame.head(n // 2), frame.tail(n // 2).with_columns(pl.col('t') + DAY)
    ridge = evaluate(train, test, 'down', n_boot=2).row(0, named=True)
    gbm = evaluate(train, test, 'down', model='gbm', n_boot=2).row(0, named=True)
    assert ridge['model'] == 'ridge'
    assert gbm['a_spearman'] > .8
    assert gbm['a_spearman'] > ridge['a_spearman'] + .5


def test_gbm_transforms_preserve_missing_and_train_only_fit(monkeypatch):
    from sklearn.ensemble import HistGradientBoostingRegressor
    from wicker_research.liqcausal import GBM_PARAMS, gbm_predict

    train, test = world()
    train = train.with_columns(pl.when(pl.int_range(pl.len()) % 7 == 0).then(None)
                               .otherwise(pl.col('volume_5m')).alias('volume_5m'))
    test = test.with_columns(pl.lit(float('inf')).alias('total_up'))
    columns = ['volume_5m', 'total_up', 'mass_down_10']
    captured = {}
    original_fit = HistGradientBoostingRegressor.fit

    def fit(estimator, x, y, **kwargs):
        captured['x'], captured['y'] = x.copy(), y.copy()
        assert all(estimator.get_params()[key] == value for key, value in GBM_PARAMS.items())
        return original_fit(estimator, x, y, **kwargs)

    monkeypatch.setattr(HistGradientBoostingRegressor, 'fit', fit)
    pred = gbm_predict(train, test, columns, 'liq_down_usd')
    assert np.isfinite(pred).all()
    np.testing.assert_allclose(captured['x'][:, 0], np.log1p(train['volume_5m'].to_numpy()))
    np.testing.assert_allclose(captured['y'], np.log1p(train['liq_down_usd'].to_numpy()))
    extended = pl.concat([test, test.tail(1).with_columns(pl.lit(1e20).alias('volume_5m'))])
    np.testing.assert_array_equal(pred, gbm_predict(train, extended, columns, 'liq_down_usd')[:-1])


def test_ridge_does_not_import_sklearn():
    import subprocess
    import sys

    script = '''
import sys
import numpy as np
import polars as pl
from wicker_research.liqcausal import DAY, NS, evaluate
assert not any(k == 'sklearn' or k.startswith('sklearn.') for k in sys.modules)
n = 120
frame = pl.DataFrame({'t': np.arange(n, dtype=np.int64) * 60 * NS,
                      'sym': ['A'] * n, 'volume_5m': np.arange(n) + 1.,
                      'total_down': np.ones(n), 'total_up': np.ones(n),
                      'liq_down_usd': np.arange(n) + 1., 'liq_up_usd': np.ones(n)})
evaluate(frame.head(60), frame.tail(60).with_columns(pl.col('t') + DAY), 'down', n_boot=2)
assert not any(k == 'sklearn' or k.startswith('sklearn.') for k in sys.modules)
'''
    result = subprocess.run([sys.executable, '-c', script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_gbm_cli_suffix_preserves_ridge(tmp_path, monkeypatch):
    train, test = world(n=1200)
    frame = pl.concat([train, test])
    monkeypatch.setattr('wicker_research.liqcausal.load_day', lambda roots, sym: data())
    monkeypatch.setattr('wicker_research.liqcausal.moments', lambda *args, **kwargs: frame)
    args = ['--root=1970-01-01=unused', '--root=1970-01-02=unused',
            '--train', '1970-01-01', '--test', '1970-01-02', '--syms', 'A',
            '--out', str(tmp_path), '--bootstrap', '2']
    main(args)
    originals = {side: (tmp_path / f'A_{side}.csv').read_bytes() for side in ('down', 'up')}
    main([*args, '--model', 'gbm'])
    for side in ('down', 'up'):
        assert (tmp_path / f'A_{side}.csv').read_bytes() == originals[side]
        assert pl.read_csv(tmp_path / f'A_{side}_gbm.csv')['model'].to_list() == ['gbm'] * 3


def test_unknown_model_rejected():
    train, test = world(n=200)
    with pytest.raises(ValueError, match='model must be ridge/gbm'):
        evaluate(train, test, 'down', model='unknown', n_boot=2)
