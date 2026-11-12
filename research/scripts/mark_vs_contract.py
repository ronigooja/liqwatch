"""How far does mark price follow the contract price versus the index? Read-only, uses Parquet datasets.

Per symbol, at each mark record (1 s): gap = (last trade - index) / index, dev = (mark - index) / index.
pass_through = dev / gap on seconds where |gap| is in the top 1% (how much of the contract-index gap reaches mark).
"""

import sys

import numpy as np
import pyarrow.dataset as ds

SYMS = ['BTCUSDT', 'ETHUSDT', 'ZECUSDT', 'ESPORTSUSDT', 'PARTIUSDT', 'BULLAUSDT', 'ALPINEUSDT', 'HOTUSDT',
        '1000CATUSDT', 'GUSDT', 'COWUSDT', 'REDUSDT', 'XANUSDT', 'POWRUSDT', 'FLOCKUSDT']


def load(root, name, sym, cols):
    t = ds.dataset(f'{root}/{name}', partitioning='hive').to_table(filter=ds.field('sym') == sym, columns=cols)
    return t.sort_by('rw').to_pydict()


def main(root):
    print(f'{"symbol":12s} {"n":>5s} {"|gap| bp":>9s} {"|dev| bp":>9s} {"p99|gap|":>9s} {"pass99":>7s} {"corr(dev,gap)":>14s}')
    for sym in SYMS:
        m = load(root, 'mark', sym, ['rw', 'mark', 'index'])
        t = load(root, 'trades', sym, ['rw', 'price'])
        if not m['rw'] or not t['rw']:
            continue
        mrw, mk, ix = np.array(m['rw']), np.array(m['mark']), np.array(m['index'])
        trw, tp = np.array(t['rw']), np.array(t['price'])
        j = np.searchsorted(trw, mrw, side='right') - 1
        ok = (j >= 0) & (ix > 0)
        gap = (tp[j[ok]] - ix[ok]) / ix[ok] * 1e4
        dev = (mk[ok] - ix[ok]) / ix[ok] * 1e4
        top = np.abs(gap) >= np.quantile(np.abs(gap), 0.99)
        pt = float(np.median(dev[top] / gap[top])) if top.any() else float('nan')
        c = float(np.corrcoef(dev, gap)[0, 1]) if gap.std() > 0 and dev.std() > 0 else float('nan')
        print(f'{sym:12s} {ok.sum():5d} {np.abs(gap).mean():9.2f} {np.abs(dev).mean():9.2f} '
              f'{np.quantile(np.abs(gap), 0.99):9.2f} {pt:7.2f} {c:14.2f}')


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else '/tmp/liqwatch-pq4')
