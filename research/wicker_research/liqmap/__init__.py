"""Causal, single-symbol liquidation-notional estimates, not observed positions.

All defaults are uncalibrated assumptions: an eight-hour window, aggressor-side
openings, fixed leverage weights, maintenance margin, unit scale and 10 bp bins.
Trade prices proxy the liquidation-trigger path, not recorded mark prices. A
position is removed only if a later trade strictly crosses its liquidation
price within (t - window_ns, t]. Stable input order breaks receive-time ties.
Trade notional is allocated across normalized leverage weights without leverage
multiplication. Scale must later be calibrated; no open-interest adjustment is
performed here. Only the supplied symbol and venue are represented.
"""
from dataclasses import dataclass, field

import numpy as np
import polars as pl

VERSION = '1'
DEFAULT_LEVERAGE_WEIGHTS = {5: .3, 10: .3, 20: .2, 50: .15, 100: .05}


@dataclass
class LiqMap:
    """Ascending liquidation prices and their USD notionals, one array per side."""

    down_prices: np.ndarray
    down_notional: np.ndarray
    up_prices: np.ndarray
    up_notional: np.ndarray
    bin_bp: float = 10.0
    _down_cumulative: np.ndarray = field(init=False, repr=False)
    _up_cumulative: np.ndarray = field(init=False, repr=False)

    def __post_init__(self):
        self._down_cumulative = np.r_[np.cumsum(self.down_notional[::-1])[::-1], 0.]
        self._up_cumulative = np.r_[0., np.cumsum(self.up_notional)]

    def q_down(self, price):
        """USD notional with liquidation price >= price; accepts scalars or arrays."""
        return self._down_cumulative[np.searchsorted(self.down_prices, price, side='left')]

    def q_up(self, price):
        """USD notional with liquidation price <= price; accepts scalars or arrays."""
        return self._up_cumulative[np.searchsorted(self.up_prices, price, side='right')]

    def levels(self, side: str, n: int) -> pl.DataFrame:
        """Largest n bins as price/notional, descending by USD notional.

        Side is 'down' or 'up'. Bins are [r**k, r**(k+1)) USD for
        r = 1 + bin_bp / 10,000; price is the notional-weighted mean.
        Equal-notional bins are returned in ascending price order.
        """
        if side not in ('down', 'up'):
            raise ValueError("side must be 'down' or 'up'")
        if not isinstance(n, (int, np.integer)) or n < 0:
            raise ValueError('n must be a nonnegative integer')
        prices = self.down_prices if side == 'down' else self.up_prices
        notionals = self.down_notional if side == 'down' else self.up_notional
        if not len(prices) or n == 0:
            return pl.DataFrame(schema={'price': pl.Float64, 'notional': pl.Float64})
        bins = np.floor(np.log(prices) / np.log1p(self.bin_bp / 10_000))
        _, inverse = np.unique(bins, return_inverse=True)
        totals = np.bincount(inverse, weights=notionals)
        centers = np.bincount(inverse, weights=prices * notionals) / totals
        order = np.argsort(-totals, kind='stable')[:n]
        return pl.DataFrame({'price': centers[order], 'notional': totals[order]})


def liquidation_map(trades: pl.DataFrame, t_ns: int, *,
                    window_ns: int = 8 * 60 * 60 * 1_000_000_000,
                    leverage_weights: dict[float, float] | None = None,
                    mmr: float = .005, scale: float = 1.0,
                    bin_bp: float = 10.0) -> LiqMap:
    """Allocate trade notionals to surviving liquidation levels at t_ns.

    Input columns are rw (receive nanoseconds), price, qty, buyer_is_maker.
    Supply one symbol only. Prices must be positive and quantities nonnegative.
    Leverage must be finite and positive; weights finite, nonnegative, with a
    positive total. Nonpositive liquidation prices are discarded without
    reallocating their weights. Crossing uses subsequent trades only, excluding
    the opening trade itself. All parameters are assumptions, not calibrated.
    """
    if not isinstance(window_ns, (int, np.integer)) or window_ns <= 0:
        raise ValueError('window_ns must be a positive integer')
    if not np.isfinite(mmr) or mmr < 0 or not np.isfinite(scale) or scale < 0:
        raise ValueError('mmr and scale must be finite and nonnegative')
    if not np.isfinite(bin_bp) or bin_bp <= 0:
        raise ValueError('bin_bp must be finite and positive')
    distribution = DEFAULT_LEVERAGE_WEIGHTS if leverage_weights is None else leverage_weights
    leverages = np.asarray(list(distribution), dtype=float)
    weights = np.asarray(list(distribution.values()), dtype=float)
    if (not len(weights) or not np.all(np.isfinite(leverages) & (leverages > 0))
            or not np.all(np.isfinite(weights) & (weights >= 0)) or weights.max() == 0):
        raise ValueError('invalid leverage weights')
    weights = weights / weights.max()
    weights /= weights.sum()
    frame = trades.filter((pl.col('rw') > t_ns - window_ns) & (pl.col('rw') <= t_ns))
    if frame.null_count().select(pl.sum_horizontal('rw', 'price', 'qty', 'buyer_is_maker')).item():
        raise ValueError('trade inputs must not be null')
    frame = frame.sort('rw', maintain_order=True)
    prices = frame['price'].to_numpy().astype(float)
    qty = frame['qty'].to_numpy().astype(float)
    if not np.all(np.isfinite(prices) & (prices > 0) & np.isfinite(qty) & (qty >= 0)):
        raise ValueError('prices must be positive and quantities nonnegative, both finite')
    is_long = ~frame['buyer_is_maker'].to_numpy()
    notional = prices * qty * scale
    later_min = np.r_[np.minimum.accumulate(prices[:0:-1])[::-1], np.inf][:len(prices)]
    later_max = np.r_[np.maximum.accumulate(prices[:0:-1])[::-1], -np.inf][:len(prices)]
    down_prices, down_notional, up_prices, up_notional = [], [], [], []
    for leverage, weight in zip(leverages, weights):
        if weight == 0:
            continue
        down = prices * (1 - 1 / leverage + mmr)
        up = prices * (1 + 1 / leverage - mmr)
        down_valid = is_long & (down > 0) & (later_min >= down) & (notional > 0)
        up_valid = ~is_long & (up > 0) & (later_max <= up) & (notional > 0)
        down_prices.append(down[down_valid])
        down_notional.append(notional[down_valid] * weight)
        up_prices.append(up[up_valid])
        up_notional.append(notional[up_valid] * weight)

    def sorted_arrays(levels, amounts):
        p, q = np.concatenate(levels), np.concatenate(amounts)
        order = np.argsort(p, kind='stable')
        return p[order], q[order]

    down_p, down_q = sorted_arrays(down_prices, down_notional)
    up_p, up_q = sorted_arrays(up_prices, up_notional)
    return LiqMap(down_p, down_q, up_p, up_q, bin_bp)
