"""Synthetic end-of-day diagnostic output."""
import importlib.util
from pathlib import Path

import polars as pl
import pytest


def test_summary_uses_end_of_day_maps(tmp_path, monkeypatch, capsys):
    script = Path(__file__).parents[2] / 'scripts' / 'oi_both_summary.py'
    spec = importlib.util.spec_from_file_location('oi_both_summary', script)
    summary = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(summary)
    ns = 1_000_000_000
    trades = pl.DataFrame({'rw': [86101 * ns, 86102 * ns], 'price': [100., 100.],
                           'qty': [1., 3.], 'buyer_is_maker': [False, True]})
    monkeypatch.setattr(summary, 'load_day', lambda roots, sym: (trades, None, None))
    oi_path, brackets = tmp_path / 'oi_A.parquet', tmp_path / 'brackets.csv'
    pl.DataFrame({'symbol': ['A', 'A'], 'timestamp': [86100000, 86400000],
                  'sum_oi': [10., 14.]}).write_parquet(oi_path)
    brackets.write_text('symbol,tier,lower,upper,max_leverage,mmr,maint_amount\n'
                        'A,1,0,10000,10,0.01,0\n')
    summary.main(['unused', '--date', '1970-01-01', '--sym', 'A', '--oi', str(oi_path),
                  '--brackets', str(brackets)])
    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == 'mode,long_notional,short_notional,long_short_ratio'
    assert lines[1].split(',')[0] == 'bucket'
    assert lines[2].split(',')[0] == 'bucket_both'
    assert [float(v) for v in lines[1].split(',')[1:]] == pytest.approx([100, 300, 1 / 3])
    assert [float(v) for v in lines[2].split(',')[1:]] == pytest.approx([400, 400, 1])
