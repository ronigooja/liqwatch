"""Synthetic liquidation estimates with hand-computed boundaries and crossings."""
import numpy as np
import polars as pl
import pytest

from wicker_research.liqmap import Brackets, VERSION, liquidation_map, load_brackets


def trades(prices, makers=None, times=None, quantities=None):
    n = len(prices)
    return pl.DataFrame({
        'rw': np.arange(n, dtype=np.int64) if times is None else np.asarray(times, dtype=np.int64),
        'price': np.asarray(prices, dtype=float),
        'qty': np.ones(n) if quantities is None else np.asarray(quantities, dtype=float),
        'buyer_is_maker': np.zeros(n, dtype=bool) if makers is None else np.asarray(makers, dtype=bool),
    })


def test_single_long():
    result = liquidation_map(trades([100], quantities=[2]), 0, leverage_weights={10: 1})
    assert VERSION == '1'
    np.testing.assert_allclose(result.down_prices, [90.5])
    np.testing.assert_allclose(result.q_down([90, 90.5, 90.6]), [200, 200, 0])
    assert result.q_down(90.5) == 200
    assert result.q_up(200) == 0


def test_single_short():
    result = liquidation_map(trades([100], [True]), 0, leverage_weights={10: 1})
    np.testing.assert_allclose(result.up_prices, [109.5])
    level = result.up_prices[0]
    np.testing.assert_allclose(result.q_up([109.4, level, 109.6]), [0, 100, 100])
    assert result.q_down(0) == 0


def test_causal():
    df = trades([100, 101, 50, 200], [False, True, False, True])
    a = liquidation_map(df.head(2), 1)
    b = liquidation_map(df, 1)
    for name in ('down_prices', 'down_notional', 'up_prices', 'up_notional'):
        np.testing.assert_array_equal(getattr(a, name), getattr(b, name))
    np.testing.assert_array_equal(a.q_down([0, 90, 200]), b.q_down([0, 90, 200]))
    np.testing.assert_array_equal(a.q_up([0, 110, 200]), b.q_up([0, 110, 200]))


@pytest.mark.parametrize('maker,later', [(False, 90), (True, 110)])
def test_already_liquidated(maker, later):
    df = trades([100, later, 100], [maker, maker, maker], quantities=[1, 0, 0])
    result = liquidation_map(df, 2, leverage_weights={10: 1})
    assert result.q_down(0) == 0
    assert result.q_up(1000) == 0


def test_crossing_is_strict_and_ties_use_input_order():
    level = 100 * (1 - 1 / 10 + .005)
    df = trades([100, level], times=[0, 0], quantities=[1, 0])
    result = liquidation_map(df, 0, leverage_weights={10: 1})
    assert result.q_down(level) == 100
    df = trades([100, 90], times=[0, 0], quantities=[1, 0])
    assert liquidation_map(df, 0, leverage_weights={10: 1}).q_down(0) == 0


def test_weights_normalized_and_scale():
    result = liquidation_map(trades([100]), 0, leverage_weights={5: 2, 10: 6}, scale=2)
    np.testing.assert_allclose(result.down_prices, [80.5, 90.5])
    np.testing.assert_allclose(result.down_notional, [50, 150])
    np.testing.assert_allclose(result.q_down([0, 85, 100]), [200, 150, 0])


def test_empty_window_and_zero_scale():
    for df, t, kwargs in [(trades([]), 0, {}), (trades([100]), 2, {'window_ns': 1}),
                          (trades([100]), 0, {'scale': 0})]:
        result = liquidation_map(df, t, **kwargs)
        np.testing.assert_array_equal(result.q_down([0, 100]), [0, 0])
        np.testing.assert_array_equal(result.q_up([0, 100]), [0, 0])
        assert result.levels('down', 3).is_empty()


def test_window_boundaries_and_unsorted_input():
    df = trades([100, 1, 1000, 100], times=[10, 0, 11, 1])
    result = liquidation_map(df, 10, window_ns=10, leverage_weights={10: 1})
    assert result.q_down(0) == 200
    assert len(result.down_prices) == 2


def test_invalid_liquidation_prices_dropped():
    result = liquidation_map(trades([100]), 0, leverage_weights={.5: 1}, mmr=0)
    assert result.q_down(0) == 0


@pytest.mark.parametrize('maker,side', [(False, 'down'), (True, 'up')])
def test_levels(maker, side):
    df = trades([100, 100.01, 105], [maker] * 3, quantities=[1, 2, 1])
    result = liquidation_map(df, 2, leverage_weights={10: 1})
    levels = result.levels(side, 1)
    factor = .905 if side == 'down' else 1.095
    np.testing.assert_allclose(levels['notional'], [300.02])
    np.testing.assert_allclose(levels['price'], [(100 * 100 + 100.01 * 200.02) / 300.02 * factor])
    assert result.levels(side, 0).is_empty()


@pytest.mark.parametrize('kwargs', [
    {'window_ns': 0}, {'mmr': -1}, {'scale': np.inf}, {'bin_bp': 0},
    {'leverage_weights': {}}, {'leverage_weights': {10: 0}},
    {'leverage_weights': {0: 1}}, {'leverage_weights': {10: -1}},
])
def test_invalid_parameters(kwargs):
    with pytest.raises(ValueError):
        liquidation_map(trades([100]), 0, **kwargs)


def test_invalid_inputs_and_level_requests():
    with pytest.raises(ValueError):
        liquidation_map(trades([np.nan]), 0)
    result = liquidation_map(trades([100]), 0)
    with pytest.raises(ValueError):
        result.levels('long', 1)
    with pytest.raises(ValueError):
        result.levels('down', -1)


def bracket_rules():
    return Brackets(np.array([0., 200.]), np.array([.01, .02]),
                    np.array([0., 2.]), np.array([50., 10.]))


@pytest.mark.parametrize('maker,expected', [(False, [91., 91., 91.5]),
                                            (True, [108.5, 109., 109.])])
def test_bracket_lookup_and_deduction(maker, expected):
    result = liquidation_map(trades([100.] * 3, [maker] * 3, quantities=[1, 2, 4]),
                             2, brackets=bracket_rules(), leverage_weights={10: 1})
    np.testing.assert_allclose(result.up_prices if maker else result.down_prices, expected)
    assert result.q_up(1000) + result.q_down(0) == 700


def test_leverage_caps_renormalize_per_trade():
    result = liquidation_map(trades([100, 100], quantities=[1, 2]), 1,
                             brackets=bracket_rules(), leverage_weights={10: 1, 50: 1, 100: 2})
    np.testing.assert_allclose(result.down_prices, [91, 91, 99])
    np.testing.assert_allclose(result.down_notional, [50, 200, 50])
    dropped = liquidation_map(trades([100]), 0, brackets=bracket_rules(),
                              leverage_weights={100: 1})
    assert dropped.q_down(0) == 0


def test_load_brackets(tmp_path):
    path = tmp_path / 'brackets.csv'
    path.write_text('symbol,tier,lower,upper,max_leverage,mmr,maint_amount\n'
                    'A,2,200,1000,10,0.02,2\nA,1,0,200,50,0.01,0\n'
                    'B,1,0,1000,20,0.03,0\n')
    tables = load_brackets(path)
    assert set(tables) == {'A', 'B'}
    np.testing.assert_array_equal(tables['A'].lower, [0, 200])
    np.testing.assert_array_equal(tables['A'].amount, [0, 2])
    path.write_text('symbol,tier,lower,upper,max_leverage,mmr,maint_amount\n'
                    'A,1,0,100,50,0.01,0\nA,2,200,1000,10,0.02,2\n')
    with pytest.raises(ValueError, match='contiguous'):
        load_brackets(path)


def test_oi_bucket_allocation_and_causality():
    ns = 1_000_000_000
    df = trades([100, 100, 100, 100, 100], [False, False, True, False, True],
                times=np.array([1, 100, 200, 301, 601]) * ns, quantities=[1, 2, 1, 4, 9])
    oi = (np.array([0, 300_000, 600_000]), np.array([100., 102., 101.]))
    kwargs = {'leverage_weights': {10: 1}, 'oi_mode': 'bucket'}
    result = liquidation_map(df, 600 * ns, open_interest=oi, **kwargs)
    np.testing.assert_allclose(result.down_notional, [50, 100])
    np.testing.assert_allclose(result.up_notional, [50])
    later = (np.r_[oi[0], 900_000], np.r_[oi[1], 10000.])
    after = liquidation_map(df, 600 * ns, open_interest=later, **kwargs)
    for name in ('down_prices', 'down_notional', 'up_prices', 'up_notional'):
        np.testing.assert_array_equal(getattr(result, name), getattr(after, name))
    assert liquidation_map(df, 299 * ns, oi=oi, **kwargs).q_down(0) == 0
    assert liquidation_map(df, 600 * ns, window_ns=599 * ns, oi=oi, **kwargs).q_down(0) == 0
    missing = (np.array([0, 600_000]), np.array([100., 102.]))
    assert liquidation_map(df, 600 * ns, oi=missing, **kwargs).q_down(0) == 0


def test_oi_brackets_use_raw_entry_notional_and_quantity():
    ns = 1_000_000_000
    df = trades([100], times=[ns], quantities=[4])
    oi = (np.array([0, 300_000]), np.array([100., 101.]))
    result = liquidation_map(df, 300 * ns, oi=oi, oi_mode='bucket',
                             brackets=bracket_rules(), leverage_weights={10: 1, 50: 1})
    np.testing.assert_allclose(result.down_prices, [91.5])
    np.testing.assert_allclose(result.down_notional, [100.])


@pytest.mark.parametrize('kwargs', [
    {'oi_mode': 'other'}, {'oi_mode': 'bucket'},
    {'oi_mode': 'bucket', 'oi': ([0, 0], [1., 2.])},
    {'oi_mode': 'bucket', 'oi': ([0, 300_000], [1., -2.])},
    {'oi_mode': 'bucket', 'oi': ([0, 300_000], [1., np.nan])},
    {'oi_mode': 'bucket', 'oi': ([0., 300_000.], [1., 2.])},
    {'oi_mode': 'bucket', 'oi': ([0], [1., 2.])},
    {'oi': ([0], [1.]), 'open_interest': ([0], [1.])},
    {'brackets': Brackets(np.array([1.]), np.array([.01]), np.array([0.]), np.array([50.]))},
    {'brackets': Brackets(np.array([0.]), np.array([.01]), np.array([0.]), np.array([0.]))},
])
def test_invalid_brackets_and_oi(kwargs):
    with pytest.raises(ValueError):
        liquidation_map(trades([100]), 0, **kwargs)


def test_default_options_leave_original_map_unchanged():
    df = trades([100, 101], [False, True])
    original = liquidation_map(df, 1)
    optional = liquidation_map(df, 1, brackets=None, oi_mode=None,
                               open_interest=(np.array([0]), np.array([1.])))
    for name in ('down_prices', 'down_notional', 'up_prices', 'up_notional'):
        np.testing.assert_array_equal(getattr(original, name), getattr(optional, name))
