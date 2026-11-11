"""Synthetic liquidation estimates with hand-computed boundaries and crossings."""
import numpy as np
import polars as pl
import pytest

from wicker_research.liqmap import VERSION, liquidation_map


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
