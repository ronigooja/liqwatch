"""How far mark price follows the contract price versus the index. Reads Parquet trades and mark.

Usage: python mark_follow.py ROOT SYMBOL...
For each symbol, per second: contract = last trade price at or before the mark record's receive time.
Reports the gap of contract and mark from index (bp), the share of seconds where mark equals the
funding-adjusted index (Price 1), and the response of mark to contract-vs-index gaps by bucket.
"""
import sys

import numpy as np
import pyarrow.dataset as ds


def load(root, sym):
    t = ds.dataset(f'{root}/trades', partitioning='hive').to_table(filter=ds.field('sym') == sym, columns=['rw', 'price']).sort_by('rw').to_pydict()
    m = ds.dataset(f'{root}/mark', partitioning='hive').to_table(filter=ds.field('sym') == sym).sort_by('rw').to_pydict()
    return {k: np.asarray(v) for k, v in t.items()}, {k: np.asarray(v) for k, v in m.items()}


def study(root, sym):
    t, m = load(root, sym)
    if len(t['rw']) == 0 or len(m['rw']) == 0:
        return None
    j = np.searchsorted(t['rw'], m['rw'], side='right') - 1
    ok = j >= 0
    last = t['price'][j[ok]]
    mark, idx = m['mark'][ok], m['index'][ok]
    fr, nft, e = m['funding_rate'][ok], m['next_funding_T'][ok], m['E'][ok]
    c_gap = (last - idx) / idx * 1e4
    m_gap = (mark - idx) / idx * 1e4
    left = np.clip((nft - e) / (8 * 3600 * 1000), 0, 1)
    p1 = idx * (1 + fr * left)
    p1_gap = (p1 - idx) / idx * 1e4
    out = dict(sym=sym, n=int(ok.sum()),
               contract_gap_p50=float(np.median(c_gap)), contract_gap_p99abs=float(np.percentile(np.abs(c_gap), 99)),
               mark_gap_p50=float(np.median(m_gap)), mark_gap_p99abs=float(np.percentile(np.abs(m_gap), 99)),
               mark_equals_p1=float(np.mean(np.abs(mark - p1) <= 1e-9 * idx + 1e-12)),
               mark_equals_last=float(np.mean(np.abs(mark - last) <= 1e-12 + 1e-9 * last)))
    big = np.abs(c_gap - np.median(c_gap)) > 5
    out['n_contract_far_from_index'] = int(big.sum())
    if big.sum() >= 20:
        out['mark_response_ratio'] = float(np.median((m_gap[big] - p1_gap[big]) / (c_gap[big] - p1_gap[big])))
    return out


if __name__ == '__main__':
    root = sys.argv[1]
    for s in sys.argv[2:]:
        r = study(root, s)
        print(r)
