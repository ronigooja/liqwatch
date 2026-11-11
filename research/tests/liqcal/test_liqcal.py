"""Hand-computed crossings, calibration and permutation controls."""
import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest
from polars.testing import assert_frame_equal

from wicker_research.liqcal import INPUT_SCHEMA, baseline, fit_scale, load_day, main, moments, score

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
    out, dump = tmp_path / 'score.csv', tmp_path / 'moments.csv'
    main([str(first), '--syms', 'BTCUSDT', '--out', str(out), '--horizon', '60', '--moments', str(dump)])
    assert pl.read_csv(out).height == 1
    assert pl.read_csv(dump).height == 3


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
