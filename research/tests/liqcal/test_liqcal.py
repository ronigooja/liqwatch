"""Hand-computed crossings, calibration and permutation controls."""
import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest
from polars.testing import assert_frame_equal

from wicker_research.liqcal import (
    INPUT_SCHEMA, MOMENT_SCHEMA, baseline, conditional_by_move, fit_scale,
    load_day, main, moments, move_baseline, partial_spearman, score,
)

NS = 1_000_000_000


def day():
    trades = pl.DataFrame({'rw': [0], 'price': [100.], 'qty': [2.], 'buyer_is_maker': [False]})
    mark = pl.DataFrame({'rw': [0, 60 * NS, 120 * NS, 180 * NS], 'mark': [100., 100., 90., 90.]})
    liq = pl.DataFrame({'rw': [90 * NS], 'side': ['SELL'], 'avg_price': [90.], 'filled_qty': [1.]})
    return trades, mark, liq


def evaluate(data):
    return moments(*data, 'BTCUSDT', step_s=60, horizon_s=60, params={'leverage_weights': {10: 1}})


def test_crossing_and_flat():
    result = evaluate(day())
    assert result['t'].to_list() == [0, 60 * NS, 120 * NS]
    assert result['pred_down'].to_list() == [0., 200., 0.]
    assert result['obs_down'].to_list() == [0., 90., 0.]
    assert result['pred_up'].sum() == 0
    assert result.columns == list(MOMENT_SCHEMA)
    assert result['move_down'].to_list() == [0., .1, 0.]
    assert result['move_up'].to_list() == [0., 0., 0.]
    row = score(result).row(0, named=True)
    assert row['down_n_moments'] == 3
    assert row['down_n_crossed'] == 1
    assert row['down_ratio'] == .45
    assert row['up_n_crossed'] == 0
    assert np.isnan(row['up_ratio'])
    assert fit_scale(result) == .45


def test_causal_append():
    original = day()
    extra = [pl.concat([df, df.tail(1).with_columns(pl.lit(600 * NS).alias('rw'))]) for df in original]
    before = evaluate(original)
    after = evaluate(extra).filter(pl.col('t') <= before['t'][-1])
    assert_frame_equal(before, after)


def test_window_counts_and_symbol_filter():
    data = [df.with_columns(pl.lit('BTCUSDT').alias('sym')) for df in day()]
    mixed = [pl.concat([df, df.with_columns(pl.lit('OTHER').alias('sym'))]) for df in data]
    assert_frame_equal(evaluate(data), evaluate(mixed))
    assert moments(*mixed, 'BTCUSDT', min_trades=2).is_empty()
    assert moments(*day(), 'BTCUSDT', horizon_s=60, params={'window_ns': NS}).height == 1


def scored_frame():
    return pl.DataFrame({'sym': ['A'] * 5 + ['B'] * 5,
                         'pred_down': [0., 1., 2., 3., 4.] * 2,
                         'pred_up': [0., 0., 1., 1., 2.] * 2,
                         'obs_down': [100., 2., 4., 6., 8.] * 2,
                         'obs_up': [100., 100., 2., 2., 4.] * 2})


def test_baseline_seed_and_scores():
    df = scored_frame()
    assert_frame_equal(baseline(df, 15), baseline(df, 15))
    assert not baseline(df, 15).equals(baseline(df, 16))
    row = score(df).row(0, named=True)
    assert row['down_spearman'] == pytest.approx(1.)
    assert row['up_spearman'] == pytest.approx(1.)
    assert row['down_ratio'] == 2
    assert fit_scale(df) == 2
    assert row['total_n_crossed'] == 8
    assert row['down_baseline_spearman_p95'] < row['down_spearman']


def test_baseline_stays_within_symbols():
    df = scored_frame().with_columns(
        pl.when(pl.col('sym') == 'A').then(1.).otherwise(100.).alias('obs_down'))
    row = baseline(df, 0).row(0, named=True)
    assert row['down_baseline_ratio_p05'] == row['down_baseline_ratio_p95']


@pytest.mark.parametrize('partition_liquidations', [False, True])
def test_load_day_and_cli(tmp_path, partition_liquidations):
    for root_name in ('first', 'second'):
        root = tmp_path / root_name
        for name, frame in zip(INPUT_SCHEMA, day()):
            path = root / name / 'date=20261105'
            if name != 'liquidations' or partition_liquidations:
                path /= 'sym=BTCUSDT'
            else:
                frame = frame.with_columns(pl.lit('BTCUSDT').alias('sym'))
            path.mkdir(parents=True)
            pq.write_table(frame.to_arrow(), path / 'part-0.parquet')
    first = tmp_path / 'first'
    loaded = load_day(first, 'BTCUSDT')
    for actual, expected in zip(loaded, day()):
        assert_frame_equal(actual, expected)
    assert all(df.is_empty() for df in load_day(first, 'OTHER'))
    combined = load_day([first, tmp_path / 'second'], 'BTCUSDT')
    assert [df.height for df in combined] == [2, 8, 2]
    out, dump, bins = tmp_path / 'score.csv', tmp_path / 'moments.csv', tmp_path / 'bins.csv'
    main([str(first), '--syms', 'BTCUSDT', '--out', str(out), '--horizon', '60',
          '--moments', str(dump), '--bins', str(bins)])
    assert pl.read_csv(out).height == 1
    assert pl.read_csv(dump).height == 3
    conditional = pl.read_csv(bins)
    assert conditional.columns == ['sym', 'side', 'bin', 'n', 'mean_pred', 'mean_obs', 'spearman']
    assert conditional['n'].sum() == 2


def test_empty_and_invalid():
    empty = evaluate(tuple(pl.DataFrame(schema=schema) for schema in INPUT_SCHEMA.values()))
    assert empty.is_empty()
    assert score(empty)['total_n_crossed'][0] == 0
    assert np.isnan(fit_scale(empty))
    with pytest.raises(ValueError):
        moments(*day(), 'BTCUSDT', step_s=0)
    with pytest.raises(ValueError):
        baseline(empty, 0, n=0)


def test_short_and_horizon_boundaries():
    trades, mark, liq = day()
    trades = trades.with_columns(pl.lit(True).alias('buyer_is_maker'))
    mark = mark.with_columns(pl.Series('mark', [100., 110., 100., 100.]))
    liq = pl.concat([
        liq.with_columns(pl.lit('BUY').alias('side'), pl.lit(0, dtype=pl.Int64).alias('rw')),
        liq.with_columns(pl.lit('BUY').alias('side'), pl.lit(60 * NS).alias('rw')),
    ])
    result = evaluate((trades, mark, liq))
    assert result['pred_up'][0] == 200
    assert result['obs_up'][0] == 90
    assert result['obs_up'][1] == 0
    assert result['move_up'][0] == .1


def synthetic(pred, obs, move):
    return pl.DataFrame({'sym': ['A'] * len(pred), 'pred_down': pred,
                         'pred_up': np.zeros(len(pred)), 'obs_down': obs,
                         'obs_up': np.zeros(len(pred)), 'move_down': move,
                         'move_up': np.zeros(len(pred))})


def test_move_only_signal():
    rng = np.random.default_rng(41)
    move = rng.uniform(.01, .1, 2000)
    obs = np.round(move, 2)
    pred = move + rng.uniform(0, .02, len(move))
    df = synthetic(pred, obs, move)
    assert abs(partial_spearman(df, 'down')) < .1
    exact = synthetic(move**2, move**3, move)
    assert np.isnan(partial_spearman(exact, 'down'))
    row = move_baseline(synthetic(move**2, 7 * move**1.5, move)).row(0, named=True)
    assert row['down_move_spearman'] == pytest.approx(1)
    assert row['down_move_k'] == pytest.approx(7)
    assert row['down_move_b'] == pytest.approx(1.5)


def test_independent_map_signal():
    rng = np.random.default_rng(19)
    pred = rng.uniform(1, 10, 2000)
    move = rng.uniform(.01, .1, len(pred))
    df = synthetic(pred, pred + rng.normal(0, .1, len(pred)), move)
    row = score(df, n=2).row(0, named=True)
    assert row['down_partial_spearman'] > .5
    assert abs(row['down_move_spearman']) < .1
    assert row['total_partial_spearman'] == pytest.approx(row['down_partial_spearman'])


@pytest.mark.parametrize('side', ['down', 'up', 'total'])
def test_conditional_counts_and_ties(side):
    df = scored_frame().with_columns(
        pl.Series('move_down', [.01, .01, .02, .03, .04] * 2),
        pl.Series('move_up', [.01, .01, .02, .02, .03] * 2))
    table = conditional_by_move(df, side)
    assert table['n'].sum() == score(df, n=1)[f'{side}_n_crossed'][0]
    assert all(np.isnan(v) for v, n in zip(table['spearman'], table['n']) if n < 5)


def test_total_move_and_observed_masks():
    df = synthetic(np.arange(1., 13.), np.arange(1., 13.), np.arange(1., 13.) / 100)
    df = df.with_columns(pl.lit(1000.).alias('obs_up'),
                         pl.Series('move_up', np.arange(12., 0., -1) / 100))
    row = move_baseline(df).row(0, named=True)
    assert np.isnan(row['total_move_spearman'])
    assert row['down_move_spearman'] == pytest.approx(1)
    table = conditional_by_move(df, 'total', bins=1)
    assert table['mean_obs'][0] == pytest.approx(6.5)


@pytest.mark.parametrize('kind', ['empty', 'short', 'constant', 'nan', 'zero_obs'])
def test_move_undefined(kind):
    n = 0 if kind == 'empty' else 2 if kind == 'short' else 12
    move = np.full(n, np.nan) if kind == 'nan' else np.ones(n)
    obs = np.zeros(n) if kind == 'zero_obs' else np.ones(n)
    df = synthetic(np.ones(n), obs, move)
    for side in ('down', 'up', 'total'):
        assert np.isnan(partial_spearman(df, side))
        row = move_baseline(df).row(0, named=True)
        assert np.isnan(row[f'{side}_move_k'])
        assert np.isnan(row[f'{side}_move_b'])
        assert conditional_by_move(df, side)['n'].sum() == (n if side != 'up' else 0)
    score(df, n=1)


def test_move_validation():
    df = synthetic(np.ones(3), np.ones(3), np.ones(3))
    with pytest.raises(ValueError):
        conditional_by_move(df, 'down', bins=0)
    with pytest.raises(ValueError):
        partial_spearman(df, 'invalid')


def test_brackets_oi_moments_and_cli(tmp_path, monkeypatch):
    from wicker_research.liqmap import load_brackets

    bracket_path = tmp_path / 'brackets.csv'
    bracket_path.write_text('symbol,tier,lower,upper,max_leverage,mmr,maint_amount\n'
                            'BTCUSDT,1,0,10000,10,0.01,0\n')
    oi = pl.DataFrame({'symbol': ['BTCUSDT'] * 2, 'timestamp': [0, 300_000],
                       'sum_oi': [100., 103.]})
    oi.write_parquet(tmp_path / 'oi_BTCUSDT.parquet')
    tr, _, liq = day()
    tr = tr.with_columns(pl.lit(NS).alias('rw'))
    mark = pl.DataFrame({'rw': [0, 300 * NS, 360 * NS], 'mark': [100., 100., 80.]})
    data = (tr, mark, liq)
    params = {'brackets': load_brackets(bracket_path)['BTCUSDT'],
              'open_interest': (oi['timestamp'].to_numpy(), oi['sum_oi'].to_numpy()),
              'oi_mode': 'bucket'}
    result = moments(*data, 'BTCUSDT', horizon_s=60, params=params)
    assert result.filter(pl.col('t') == 300 * NS)['pred_down'].item() == 300.
    assert result.filter(pl.col('t') < 300 * NS)['pred_down'].sum() == 0
    monkeypatch.setattr('wicker_research.liqcal.load_day', lambda roots, sym: data)
    out, dump = tmp_path / 'score.csv', tmp_path / 'moments.csv'
    args = [str(tmp_path), '--syms', 'BTCUSDT', '--out', str(out), '--moments', str(dump),
            '--horizon', '60', '--brackets', str(bracket_path), '--oi', str(tmp_path),
            '--oi-mode', 'bucket']
    main(args)
    assert pl.read_csv(out)['down_sum_pred'].item() == 300.
    assert_frame_equal(pl.read_csv(dump), result)
    with pytest.raises(SystemExit):
        main([str(tmp_path), '--syms', 'OTHER', '--out', str(out),
              '--brackets', str(bracket_path)])
    with pytest.raises(SystemExit):
        main([str(tmp_path), '--syms', 'BTCUSDT', '--out', str(out), '--oi-mode', 'bucket'])
