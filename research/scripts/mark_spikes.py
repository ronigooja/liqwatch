"""Does mark price follow the contract price during short deviations? Read-only, uses Parquet datasets.

Per symbol on a 1 s grid: gap = (last trade - index) / index, dev = (mark - index) / index, both in bp.
Both are measured relative to their own trailing 300 s median, so a persistent basis is removed and only short moves remain.
A spike second is one where |gap - median| >= threshold. pass = (dev - median) / (gap - median) on spike seconds.
Usage: mark_spikes.py OUT_CSV THRESHOLD_BP ROOT [ROOT ...] [--syms A,B,...]
"""

import csv
import sys

import numpy as np
import pyarrow.dataset as ds

WINDOW = 300


def load(roots, name, sym, cols):
    parts = []
    for root in roots:
        t = ds.dataset(f'{root}/{name}', partitioning='hive').to_table(filter=ds.field('sym') == sym, columns=cols)
        if t.num_rows:
            parts.append(t)
    if not parts:
        return None
    import pyarrow as pa
    return pa.concat_tables(parts).sort_by('rw').to_pydict()


def trailing_median(x):
    from numpy.lib.stride_tricks import sliding_window_view
    pad = np.concatenate([np.full(WINDOW - 1, x[0]), x])
    return np.median(sliding_window_view(pad, WINDOW), axis=1)


def one(roots, sym, threshold):
    m = load(roots, 'mark', sym, ['rw', 'mark', 'index'])
    t = load(roots, 'trades', sym, ['rw', 'price'])
    if m is None or t is None:
        return None
    mrw, mk, ix = np.array(m['rw']), np.array(m['mark']), np.array(m['index'])
    trw, tp = np.array(t['rw']), np.array(t['price'])
    j = np.searchsorted(trw, mrw, side='right') - 1
    ok = (j >= 0) & (ix > 0)
    if ok.sum() < 3600:
        return None
    gap = (tp[j[ok]] - ix[ok]) / ix[ok] * 1e4
    dev = (mk[ok] - ix[ok]) / ix[ok] * 1e4
    gm, dm = trailing_median(gap), trailing_median(dev)
    g, d = gap - gm, dev - dm
    spike = np.abs(g) >= threshold
    n = int(spike.sum())
    row = dict(sym=sym, seconds=int(ok.sum()), spike_seconds=n)
    if n == 0:
        return row
    ratio = d[spike] / g[spike]
    runs = np.diff(np.concatenate([[0], spike.astype(int), [0]]))
    lengths = np.nonzero(runs == -1)[0] - np.nonzero(runs == 1)[0]
    row.update(spike_runs=len(lengths), median_run_s=float(np.median(lengths)),
               pass_median=float(np.median(ratio)), pass_p25=float(np.quantile(ratio, 0.25)),
               pass_p75=float(np.quantile(ratio, 0.75)), mark_eq_last=float(np.mean(mk[ok][spike] == tp[j[ok]][spike])))
    return row


def symbols(root):
    from pathlib import Path
    return sorted(p.name.split('=', 1)[1] for p in Path(f'{root}/mark').glob('date=*/sym=*') if p.name.startswith('sym='))


def main(argv):
    out, threshold = argv[0], float(argv[1])
    rest = argv[2:]
    only = None
    if '--syms' in rest:
        k = rest.index('--syms')
        only = rest[k + 1].split(',')
        rest = rest[:k]
    roots = rest
    syms = only or symbols(roots[0])
    cols = ['sym', 'seconds', 'spike_seconds', 'spike_runs', 'median_run_s', 'pass_median', 'pass_p25', 'pass_p75', 'mark_eq_last']
    with open(out, 'w', newline='') as f:
        w = csv.DictWriter(f, cols)
        w.writeheader()
        for i, s in enumerate(syms):
            try:
                r = one(roots, s, threshold)
            except Exception as e:
                print('error', s, repr(e)[:120], flush=True)
                continue
            if r:
                w.writerow(r)
                f.flush()
            if i % 50 == 0:
                print(i, len(syms), flush=True)


if __name__ == '__main__':
    main(sys.argv[1:])
