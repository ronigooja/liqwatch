"""Shared CLI map-window and trade-slicing regression checks."""
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from wicker_research import liqcal, liqcausal

NS = liqcal.NS
HOUR = 3600 * NS


def day():
    t = 12 * HOUR
    trades = pl.DataFrame({
        'rw': [t - 2 * HOUR, t - HOUR, t - HOUR // 2, t + NS],
        'price': [100.] * 4, 'qty': [1., 2., 3., 4.],
        'buyer_is_maker': [False] * 4,
    })
    mark = pl.DataFrame({'rw': [t, t + 60 * NS], 'mark': [100., 80.]})
    return trades, mark, pl.DataFrame(schema=liqcal.INPUT_SCHEMA['liquidations'])


def cli_args(module, out):
    common = ['--syms', 'A', '--out', str(out), '--horizon', '60']
    if module is liqcal:
        return ['unused', *common]
    return ['--root=1970-01-01=unused', '--root=1970-01-02=unused',
            '--train=1970-01-01', '--test=1970-01-02', *common]


@pytest.mark.parametrize('module', [liqcal, liqcausal])
@pytest.mark.parametrize('hours', [None, '1', '1.2345678901234567'])
def test_cli_window_reaches_map_and_slice(module, hours, monkeypatch, tmp_path):
    class ReachedMap(Exception):
        pass

    expected = round(float(hours or 8) * HOUR)
    trades, mark, liquidations = day()
    loaded = iter([(trades, mark, liquidations),
                   (trades.head(0), mark.head(0), liquidations)])
    monkeypatch.setattr(module, 'load_day', lambda *args: next(loaded))

    def spy(frame, t, **params):
        assert type(params['window_ns']) is int
        assert params['window_ns'] == expected
        assert_frame_equal(frame, trades.filter(
            (pl.col('rw') > t - expected) & (pl.col('rw') <= t)))
        raise ReachedMap

    monkeypatch.setattr(module, 'liquidation_map', spy)
    args = cli_args(module, tmp_path / 'out')
    if hours is not None:
        args += ['--window-hours', hours]
    with pytest.raises(ReachedMap):
        module.main(args)


@pytest.mark.parametrize('module', [liqcal, liqcausal])
def test_one_hour_ignores_older_trades(module, monkeypatch):
    build_map = module.liquidation_map
    maps, slices = [], []

    def spy(frame, t, **params):
        q = build_map(frame, t, **params)
        maps.append(q)
        slices.append(frame['qty'].to_list())
        return q

    monkeypatch.setattr(module, 'liquidation_map', spy)
    results = [module.moments(*day(), 'A', horizon_s=60,
                             params={'window_ns': hours * HOUR, 'leverage_weights': {10: 1}})
               for hours in (1, 8)]
    assert slices == [[3.], [1., 2., 3.]]
    assert [q.down_notional.sum() for q in maps] == [300., 600.]
    column = 'pred_down' if module is liqcal else 'total_down'
    assert [frame[column].item() for frame in results] == [300., 600.]
    default = module.moments(*day(), 'A', horizon_s=60,
                             params={'leverage_weights': {10: 1}})
    assert_frame_equal(default, results[1])


@pytest.mark.parametrize('module', [liqcal, liqcausal])
@pytest.mark.parametrize('hours', ['0', '-1', 'nan', 'inf', '1e300', '1e-20'])
def test_cli_rejects_invalid_window(module, hours, tmp_path, capsys):
    with pytest.raises(SystemExit) as exc:
        module.main([*cli_args(module, tmp_path / 'out'), f'--window-hours={hours}'])
    assert exc.value.code == 2
    assert '--window-hours must be finite and positive' in capsys.readouterr().err
