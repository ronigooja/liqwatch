import numpy as np
import polars as pl
import pyarrow.parquet as pq

from liqwatch_research.labels import NS, labels, write_parquet


def trades(prices):
    n = len(prices)
    return pl.DataFrame({'rw': np.arange(n, dtype=np.int64) * NS, 'price': prices, 'qty': [1.] * n, 'buyer_is_maker': [False] * n})


def test_causal_and_deterministic():
    p = np.exp(np.arange(90) * 0.001 + np.sin(np.arange(90)) * 0.001)
    a = labels(trades(p))
    p[60:] *= 2
    b = labels(trades(p))
    assert a['sigma_30'][:60].equals(b['sigma_30'][:60])
    assert a.equals(labels(trades(np.exp(np.arange(90) * 0.001 + np.sin(np.arange(90)) * 0.001))))


def test_risk_hand_computed():
    p = np.exp(np.array([0, .01, .02, .00, .03, .01, .01, .01, .01]))
    out = labels(trades(p), k=.4, half_life=1, spike_lookback=1)
    assert out['risk_up_5'][1] is True
    assert out['risk_down_5'][1] is True
    np.testing.assert_allclose(out['range_max_5'][1], .02, atol=1e-12)
    np.testing.assert_allclose(out['range_min_5'][1], -.01, atol=1e-12)


def test_ambiguity_and_exclusivity():
    # At second 3 both extremes occur, although the last trade closes higher.
    df = trades(np.exp(np.array([0, .01, .02, .03, .03, .03, .03, .03, .03, .03])))
    extra = pl.DataFrame({'rw': [3 * NS + 1], 'price': [float(np.exp(.00))], 'qty': [1.], 'buyer_is_maker': [False]})
    out = labels(pl.concat([df, extra]), spike_lookback=1, delta=.005)
    assert out['ambiguous_5'][2] is True
    assert out['first_touch_5'][2] == 'retrace'
    vals = out['first_touch_5'].drop_nulls().to_list()
    assert all(x in ('extension', 'retrace', 'neither') for x in vals)
    assert sum(vals.count(x) for x in ('extension', 'retrace', 'neither')) == len(vals)


def test_gap_and_metadata(tmp_path):
    out = labels(trades(np.exp(np.arange(50) * .001)), quality=pl.DataFrame({'start_rw': [10*NS], 'end_rw': [11*NS]}), spike_lookback=1)
    assert out['valid_5'][4] is True
    assert out['valid_5'][5] is False
    assert out['valid_5'][11] is False
    assert out['valid_5'][12] is True
    assert out['valid_5'][-1] is False
    assert out['risk_up_5'][5] is None
    path = tmp_path / 'labels.parquet'
    write_parquet(out, path)
    assert pq.read_metadata(path).metadata[b'label_code_version'] == b'1'
