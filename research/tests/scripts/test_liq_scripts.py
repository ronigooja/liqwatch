"""Synthetic regression checks for reproducible liquidation diagnostics."""
import json

import numpy as np
import polars as pl
import pytest

from scripts.liq_decompose import (LEVERAGES, crossed_levels, decompose,
                                   load_ratios, main as decompose_main, position_shares)
from scripts.liq_event_inspect import inspect, main as event_main
from scripts.liq_fit_weights import evaluate, fit_weights, main as fit_main
from scripts.liq_trend_split import (bootstrap, check_totals, main as trend_main,
                                     split_rows, trend_features)
from wicker_research.liqcal import INPUT_SCHEMA, NS, moments
from wicker_research.liqmap import Brackets, liquidation_map


def trades(prices=(100.,), times=(1,), makers=None):
    return pl.DataFrame({'rw': np.asarray(times, dtype=np.int64) * NS,
                         'price': list(prices), 'qty': [1.] * len(prices),
                         'buyer_is_maker': makers or [False] * len(prices)})


def marks(prices=(100., 160., 40.), times=(0, 120, 360)):
    return pl.DataFrame({'rw': np.asarray(times, dtype=np.int64) * NS, 'mark': list(prices)})


def brackets(cap=2):
    return Brackets(np.array([0.]), np.array([0.]), np.array([0.]), np.array([float(cap)]))


def liquidations():
    return pl.DataFrame(schema=INPUT_SCHEMA['liquidations'])


def masses(tr, ma, *, mark_survival=False):
    return {(side, lev): mass.sum() for side, lev, _, mass, _ in crossed_levels(
        tr, ma, 60 * NS, 100., 40., 160., brackets(), None, mark_survival=mark_survival)}


def test_single_allowed_leverage_matches_map():
    tr, ma = trades(), marks()
    # Default weights start at 5, so use the 14-level distribution with only L=2
    # admitted by the cap. Both-side allocation must match bucket_both.
    oi = (np.array([0, 300_000], dtype=np.int64), np.array([10., 11.]))
    tr = trades(times=(1,))
    t = 300 * NS
    decomposition = {(s, l): float(mass.sum()) for s, l, _, mass, _ in crossed_levels(
        tr, ma, t, 100., 40., 160., brackets(), oi)}
    result = liquidation_map(tr, t, brackets=brackets(), open_interest=oi,
                            oi_mode='bucket_both', leverage_weights={l: 1 for l in LEVERAGES})
    assert decomposition['up', 2] == pytest.approx(result.q_up(160) - result.q_up(100))
    assert decomposition['down', 2] == pytest.approx(result.q_down(40) - result.q_down(100))
    assert decomposition['up', 2] > 0
    assert sum(v for (s, l), v in decomposition.items() if l != 2) == 0


def test_default_weights_single_allowed_level_matches():
    tr, ma = trades(), marks()
    b = brackets(5)
    candidates = {(s, l): mass.sum() for s, l, _, mass, _ in crossed_levels(
        tr, ma, 60 * NS, 100., 40., 160., b, None)}
    # Per-trade normalization gives all default weight to L=5, not 0.3.
    long_map = liquidation_map(tr, 60 * NS, brackets=b)
    short_map = liquidation_map(tr.with_columns(pl.lit(True).alias('buyer_is_maker')), 60 * NS, brackets=b)
    assert candidates['down', 5] == pytest.approx(long_map.q_down(40) - long_map.q_down(100))
    assert candidates['up', 5] == pytest.approx(short_map.q_up(160) - short_map.q_up(100))


def test_ratio_publication_delay_and_shares(tmp_path):
    path = tmp_path / 'ls.csv'
    pl.DataFrame({'timestamp': [0, 300_000], 'top_pos_ratio': [3., 9.]}).write_csv(path)
    ratios = load_ratios(path)
    assert position_shares(300 * NS - 1, ratios) == (.5, .5)
    assert position_shares(300 * NS, ratios) == (.75, .25)
    assert position_shares(600 * NS - 1, ratios) == (.75, .25)
    assert position_shares(600 * NS, ratios) == (.9, .1)
    parts = {(s, l): m.sum() for s, l, _, m, _ in crossed_levels(
        trades(), marks(), 300 * NS, 100., 40., 160., brackets(), None, ratios=ratios)}
    assert parts['down', 2] == pytest.approx(75.)
    assert parts['up', 2] == pytest.approx(25.)


@pytest.mark.parametrize('side,trade_price,mark_price', [('up', 160., 100.), ('down', 40., 100.),
                                                       ('up', 100., 160.), ('down', 100., 40.)])
def test_trade_and_mark_survival_differ(side, trade_price, mark_price):
    tr = trades((100., trade_price), (1, 2))
    ma = marks((100., mark_price), (0, 2))
    trade_mass = masses(tr, ma)[side, 2]
    mark_mass = masses(tr, ma, mark_survival=True)[side, 2]
    if trade_price != 100.:
        assert mark_mass > trade_mass
    else:
        assert trade_mass > mark_mass


def test_fit_recovers_noiseless_weights():
    pytest.importorskip('scipy', reason='SciPy NNLS unavailable in the research environment')
    rng = np.random.default_rng(7)
    matrix = rng.uniform(0, 100, (100, len(LEVERAGES)))
    weights = np.arange(len(LEVERAGES)) / 10
    np.testing.assert_allclose(fit_weights(matrix, matrix @ weights), weights, atol=1e-10)


def fit_frame():
    matrix = np.eye(len(LEVERAGES))
    columns = {f'{prefix}_{lev}': matrix[:, j] for prefix in ('up', 'dn') for j, lev in enumerate(LEVERAGES)}
    return pl.DataFrame({**columns, 'obs_up': np.arange(14, dtype=float), 'obs_down': np.arange(14, dtype=float)})


def test_fit_reports_gate_and_excludes_zero_predictions(capsys, tmp_path):
    pytest.importorskip('scipy', reason='SciPy NNLS unavailable in the research environment')
    frame = fit_frame()
    test = frame.with_columns(pl.when(pl.int_range(pl.len()) == 0).then(1000.).otherwise(pl.col('obs_up')).alias('obs_up'))
    result = evaluate({'a': frame, 'b': test}, ['a'], ['b'])
    row = next(r for r in result['days'] if r['day'] == 'b' and r['side'] == 'up')
    assert row['ratio'] == pytest.approx(1.)
    assert row['n_positive'] == 13
    assert result['test']['within_gate'] == 2
    for day, data in [('a', frame), ('b', test)]:
        data.write_csv(tmp_path / f'{day}.csv')
    fit_main(['--train', 'a', '--test', 'b', '--inputs', f'a={tmp_path}/a.csv', f'b={tmp_path}/b.csv'])
    assert json.loads(capsys.readouterr().out)['test']['cells'] == 2
    with pytest.raises(ValueError, match='disjoint'):
        evaluate({'a': frame}, ['a'], ['a'])


def test_trend_features_ignore_at_and_after_t():
    tr = trades((100., 101., 102., 103.), (0, 3599, 3600, 3601))
    ma = marks((100., 101., 102., 103.), (0, 3599, 3600, 3601))
    baseline = trend_features(tr, ma, [3600 * NS])
    changed_tr = tr.with_columns(pl.when(pl.col('rw') >= 3600 * NS).then(1e10).otherwise(pl.col('price')).alias('price'))
    changed_ma = ma.with_columns(pl.when(pl.col('rw') >= 3600 * NS).then(1e10).otherwise(pl.col('mark')).alias('mark'))
    for a, b in zip(baseline, trend_features(changed_tr, changed_ma, [3600 * NS])):
        np.testing.assert_array_equal(a, b)
    assert baseline[0][0] == pytest.approx(100.)
    assert baseline[2][0]


def test_split_conditioning_and_hourly_bootstrap():
    tr, ma = trades((100., 101.), (0, 3599)), marks((100., 101.), (0, 3599))
    m = pl.DataFrame({'t': [3600 * NS], 'pred_up': [2.], 'pred_down': [0.], 'obs_up': [4.], 'obs_down': [999.]})
    frame, eligible = split_rows(m, tr, ma, 'day', 'SYM')
    assert eligible == 1
    assert frame.filter(pl.col('side') == 'down')['obs'].sum() == 0
    assert frame['block'].unique().to_list() == [1]
    result = bootstrap(frame, count=20)
    assert result['blocks'] == 1
    assert result['valid'] == 0
    check_totals(m, pl.DataFrame({'up_sum_pred': [2.], 'up_sum_obs': [4.],
                                 'down_sum_pred': [0.], 'down_sum_obs': [0.]}), 'SYM')


def data_files(tmp_path):
    root = tmp_path / 'root'
    tr, ma, liq = trades((100.,), (1,)), marks((100., 100., 160., 40.), (0, 300, 301, 600)), liquidations()
    liq = pl.DataFrame({'rw': [301 * NS], 'side': ['BUY'], 'avg_price': [150.], 'filled_qty': [2.]})
    for name, data in [('trades', tr), ('mark', ma), ('liquidations', liq)]:
        directory = root / name
        directory.mkdir(parents=True)
        data.with_columns(pl.lit('SYM').alias('sym')).write_parquet(directory / 'part.parquet')
    bpath, opath = tmp_path / 'brackets.csv', tmp_path / 'oi.parquet'
    pl.DataFrame({'symbol': ['SYM'], 'tier': [1], 'lower': [0.], 'upper': [1e12],
                  'max_leverage': [5.], 'mmr': [0.], 'maint_amount': [0.]}).write_csv(bpath)
    pl.DataFrame({'symbol': ['SYM', 'SYM'], 'timestamp': [0, 300_000], 'sum_oi': [10., 11.]}).write_parquet(opath)
    return tr, ma, liq, ['--roots', str(root), '--sym', 'SYM', '--brackets', str(bpath), '--oi', str(opath)]


def test_decompose_and_event_cli(tmp_path):
    tr, ma, liq, args = data_files(tmp_path)
    output = tmp_path / 'dec.csv'
    decompose_main([*args, '--out', str(output)])
    result = pl.read_csv(output)
    row = result.filter(pl.col('t') == 300 * NS).row(0, named=True)
    assert row['up_5'] == pytest.approx(100.)
    assert row['obs_up'] == 300.
    oi = (np.array([0, 300_000]), np.array([10., 11.]))
    frame = moments(tr, ma, liq, 'SYM', params={'brackets': brackets(5), 'open_interest': oi, 'oi_mode': 'bucket_both'})
    frame.write_csv(tmp_path / 'moments.csv')
    report = inspect(tr, ma, liq, brackets(5), oi, frame)
    assert report['max_prediction_error'] == pytest.approx(0.)
    assert report['leverage_mass']['up'][0] > 0
    event_main([*args, '--moments', str(tmp_path / 'moments.csv'), '--out', str(tmp_path / 'events.json')])
    assert json.loads((tmp_path / 'events.json').read_text())['max_prediction_error'] == pytest.approx(0.)


def test_empty_decompose():
    empty = pl.DataFrame(schema=INPUT_SCHEMA['trades'])
    assert decompose(empty, marks(), liquidations(), brackets(), None).is_empty()


def test_trend_cli_empty_eligibility(tmp_path, capsys):
    tr, ma, liq, args = data_files(tmp_path)
    frame = moments(tr, ma, liq, 'SYM')
    path = tmp_path / 'moments.csv'
    frame.write_csv(path)
    out = tmp_path / 'features.csv'
    trend_main(['--input', f'day,SYM,{path},{args[1]}', '--out', str(out), '--bootstrap', '10'])
    report = json.loads(capsys.readouterr().out)
    assert report['eligibility'][0]['eligible'] == 0
    assert pl.read_csv(out).height == 0
