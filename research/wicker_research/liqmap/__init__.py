"""Causal, single-symbol liquidation-notional estimates, not observed positions.

All defaults are uncalibrated assumptions: an eight-hour window, aggressor-side
openings, fixed leverage weights, maintenance margin, unit scale and 10 bp bins.
Trade prices proxy the liquidation-trigger path, not recorded mark prices. A
position is removed only if a later trade strictly crosses its liquidation
price within (t - window_ns, t]. Stable input order breaks receive-time ties.
Trade notional is allocated across normalized leverage weights without leverage
multiplication. Scale must later be calibrated. Optional OI anchoring counts only
positive net changes, not gross openings. Only the supplied symbol and venue are
represented; positions opened before the window (including long-held ones) are missing.
Optional bucket_both treats positive net OI changes as openings on both sides;
see liquidation_map for allocation and bucket rules.

Isolated-margin equity equals maintenance P*qty*r - A at liquidation:
long P = (p*(1-1/L) - A/qty)/(1-r), short P = (p*(1+1/L) + A/qty)/(1+r).
The tier is approximated from entry notional p*qty, not liquidation notional;
without brackets r=mmr and A=0.
"""
from dataclasses import dataclass, field

import numpy as np
import polars as pl

VERSION = '1'
DEFAULT_LEVERAGE_WEIGHTS = {5: .3, 10: .3, 20: .2, 50: .15, 100: .05}


@dataclass
class Brackets:
    """Entry-notional tier lower bounds, rates, USDT deductions and leverage caps."""

    lower: np.ndarray
    mmr: np.ndarray
    amount: np.ndarray
    max_leverage: np.ndarray


def _validate_brackets(brackets):
    arrays = [np.asarray(getattr(brackets, name), dtype=float)
              for name in ('lower', 'mmr', 'amount', 'max_leverage')]
    if (any(a.ndim != 1 for a in arrays) or not len(arrays[0])
            or any(len(a) != len(arrays[0]) for a in arrays)
            or any(not np.isfinite(a).all() for a in arrays)):
        raise ValueError('bracket arrays must be finite, nonempty and equally sized')
    lower, rate, amount, cap = arrays
    if (lower[0] != 0 or np.any(np.diff(lower) <= 0) or np.any(rate < 0)
            or np.any(rate >= 1) or np.any(amount < 0) or np.any(cap <= 0)
            or np.any(amount > lower * rate)):
        raise ValueError('invalid bracket bounds, rates, amounts or leverage caps')
    return arrays


def load_brackets(path) -> dict[str, Brackets]:
    """Load symbol tiers from CSV; rule limits do not identify trader leverage.

    The 2026-11-11 table is an approximation for 2026-11-04..06, not a
    reconstruction of the historical rules. The last tier extends upward.
    """
    frame = pl.read_csv(path)
    required = {'symbol', 'tier', 'lower', 'upper', 'max_leverage', 'mmr', 'maint_amount'}
    if not required <= set(frame.columns) or frame.is_empty():
        raise ValueError('bracket CSV requires symbol, tier, lower, upper, max_leverage, mmr, maint_amount')
    if frame.select(*sorted(required)).null_count().to_numpy().any():
        raise ValueError('bracket CSV must not contain nulls')
    result = {}
    for key, group in frame.partition_by('symbol', as_dict=True).items():
        group = group.sort('lower')
        bracket = Brackets(*(group[name].to_numpy().astype(float)
                             for name in ('lower', 'mmr', 'maint_amount', 'max_leverage')))
        _validate_brackets(bracket)
        upper = group['upper'].to_numpy().astype(float)
        if (not np.isfinite(upper).all() or np.any(upper <= bracket.lower)
                or not np.array_equal(upper[:-1], bracket.lower[1:])
                or group['tier'].n_unique() != group.height or not key[0]):
            raise ValueError('bracket tiers must have unique IDs and contiguous bounds')
        result[key[0]] = bracket
    return result


def _oi_quantities(frame, qty, t_ns, window_ns, open_interest):
    """Allocate only buckets with start > t_ns-window_ns and end <= t_ns."""
    if not isinstance(open_interest, (tuple, list)) or len(open_interest) != 2:
        raise ValueError('open_interest must be (timestamps_ms, sum_oi)')
    times, values = [np.asarray(a) for a in open_interest]
    if (times.ndim != 1 or values.ndim != 1 or len(times) != len(values)
            or not np.issubdtype(times.dtype, np.integer)):
        raise ValueError('OI timestamps must be integer milliseconds and arrays equally sized')
    values = values.astype(float)
    if (np.any(np.diff(times) <= 0) or not np.isfinite(values).all() or np.any(values < 0)):
        raise ValueError('OI timestamps must increase strictly and values be finite and nonnegative')
    bucket_ns = 300_000_000_000
    starts = frame['rw'].to_numpy().astype(np.int64) // bucket_ns * bucket_ns
    unique, inverse = np.unique(starts, return_inverse=True)
    ends = unique + bucket_ns
    left = np.searchsorted(times, unique // 1_000_000)
    right = np.searchsorted(times, ends // 1_000_000)
    delta = np.zeros(len(unique))
    valid = ((unique > t_ns - window_ns) & (ends <= t_ns)
             & (left < len(times)) & (right < len(times)))
    idx = np.flatnonzero(valid)
    idx = idx[(times[left[idx]] == unique[idx] // 1_000_000)
              & (times[right[idx]] == ends[idx] // 1_000_000)]
    delta[idx] = np.maximum(values[right[idx]] - values[left[idx]], 0.)
    totals = np.bincount(inverse, weights=qty, minlength=len(unique))
    factor = np.divide(delta, totals, out=np.zeros_like(delta), where=totals > 0)
    return qty * factor[inverse]


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
                    bin_bp: float = 10.0, brackets: Brackets | None = None,
                    open_interest=None, oi=None, oi_mode: str | None = None) -> LiqMap:
    """Allocate trade notionals to surviving liquidation levels at t_ns.

    Input columns are rw (receive nanoseconds), price, qty, buyer_is_maker.
    Supply one symbol only. Prices must be positive and quantities nonnegative.
    Leverage must be finite and positive; weights finite, nonnegative, with a
    positive total. Nonpositive liquidation prices are discarded without
    reallocating their weights. Crossing uses subsequent trades only, excluding
    the opening trade itself. All parameters are assumptions, not calibrated.

    Exact isolated-margin prices are (p*(1-1/L) - A/qty)/(1-r) for longs
    and (p*(1+1/L) + A/qty)/(1+r) for shorts, with maintenance P*qty*r - A.
    Without brackets r=mmr and A=0; rates must be in [0, 1).

    Brackets use each raw trade's entry price * qty, not leverage multiplied
    notional: this entry-tier approximation does not retier at liquidation.
    A trade is a fragment, not a full position. Tiers and maintenance
    deductions use that entry quantity even when allocation is OI-scaled. Allowed
    leverage weights renormalize per trade; no allowed weight drops the trade.
    Today's brackets (2026-11-11) approximate 2026-11-04..06 rules.

    open_interest (alias oi) is (integer timestamps_ms, sum_oi in base units).
    oi_mode='bucket' uses only complete epoch-aligned five-minute buckets wholly
    within the window: start >= t_ns-window_ns+1 ns and end <= t_ns, matching
    the strict trade-window lower bound, with exact OI samples at both boundaries.
    Missing samples and incomplete buckets open nothing. Positive net OI change d
    is allocated proportional to base aggressor volume: long share is buy/total,
    short share sell/total (equivalently (1 +/- signed_volume/total)/2). Thus
    allocated base quantity totals d before scale and bracket/survival filtering;
    scale remains an explicit multiplier. Zero/negative changes open nothing.
    This conservative proxy misses offsetting openings/closings and pre-window
    long-held positions; it is not observed gross opening flow. None keeps all trades.

    oi_mode='bucket_both' uses the same bucket rules but assumes each new contract
    opens both a long and a short: each trade of quantity q in bucket volume Q
    receives d*q/Q base units on each side, regardless of aggressor side. Each
    side totals d before scale, bracket and survival filtering; d <= 0 opens nothing.
    """
    if oi_mode not in (None, 'bucket', 'bucket_both'):
        raise ValueError("oi_mode must be None, 'bucket' or 'bucket_both'")
    if oi is not None and open_interest is not None:
        raise ValueError('supply only one of oi and open_interest')
    open_interest = oi if oi is not None else open_interest
    if oi_mode is not None and open_interest is None:
        raise ValueError(f"oi_mode='{oi_mode}' requires open_interest")
    tier_arrays = _validate_brackets(brackets) if brackets is not None else None
    if not isinstance(window_ns, (int, np.integer)) or window_ns <= 0:
        raise ValueError('window_ns must be a positive integer')
    if not np.isfinite(mmr) or not 0 <= mmr < 1 or not np.isfinite(scale) or scale < 0:
        raise ValueError('mmr must be in [0, 1); scale must be finite and nonnegative')
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
    opened_qty = (_oi_quantities(frame, qty, t_ns, window_ns, open_interest)
                  if oi_mode is not None else qty)
    later_min = np.r_[np.minimum.accumulate(prices[:0:-1])[::-1], np.inf][:len(prices)]
    later_max = np.r_[np.maximum.accumulate(prices[:0:-1])[::-1], -np.inf][:len(prices)]
    if oi_mode == 'bucket_both':
        is_long = np.repeat([True, False], len(prices))
        prices, qty, opened_qty, later_min, later_max = (
            np.tile(a, 2) for a in (prices, qty, opened_qty, later_min, later_max))
    notional = prices * opened_qty * scale
    if tier_arrays is not None:
        lower, rates, amounts, caps = tier_arrays
        tier = np.searchsorted(lower, prices * qty, side='right') - 1
        rates, amounts, caps = rates[tier], amounts[tier], caps[tier]
        allowed_weights = (leverages[:, None] <= caps) * weights[:, None]
        totals = allowed_weights.sum(axis=0)
        allowed_weights = np.divide(allowed_weights, totals, out=np.zeros_like(allowed_weights),
                                    where=totals > 0)
        deduction = np.divide(amounts, qty, out=np.zeros_like(qty), where=qty > 0)
    down_prices, down_notional, up_prices, up_notional = [], [], [], []
    for j, (leverage, weight) in enumerate(zip(leverages, weights)):
        if weight == 0:
            continue
        down = prices * (1 - 1 / leverage) / (1 - mmr)
        up = prices * (1 + 1 / leverage) / (1 + mmr)
        if tier_arrays is not None:
            weight = allowed_weights[j]
            down = (prices * (1 - 1 / leverage) - deduction) / (1 - rates)
            up = (prices * (1 + 1 / leverage) + deduction) / (1 + rates)
        down_valid = is_long & (down > 0) & (later_min >= down) & (notional > 0)
        up_valid = ~is_long & (up > 0) & (later_max <= up) & (notional > 0)
        if tier_arrays is not None:
            down_valid &= weight > 0
            up_valid &= weight > 0
        down_prices.append(down[down_valid])
        down_notional.append(notional[down_valid] * (weight[down_valid] if tier_arrays is not None else weight))
        up_prices.append(up[up_valid])
        up_notional.append(notional[up_valid] * (weight[up_valid] if tier_arrays is not None else weight))

    def sorted_arrays(levels, amounts):
        p, q = np.concatenate(levels), np.concatenate(amounts)
        order = np.argsort(p, kind='stable')
        return p[order], q[order]

    down_p, down_q = sorted_arrays(down_prices, down_notional)
    up_p, up_q = sorted_arrays(up_prices, up_notional)
    return LiqMap(down_p, down_q, up_p, up_q, bin_bp)
