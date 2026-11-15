import json
import sys

import pytest
import zstandard

from liqwatch_research.book.__main__ import main
from liqwatch_research.replay import read_records


@pytest.mark.parametrize('version', [1, 2])
@pytest.mark.parametrize('compressed', [False, True])
def test_book_cli_raw_formats(tmp_path, monkeypatch, capsys, version, compressed):
    directory = tmp_path / 'binance' / 'X' / '20260101'
    directory.mkdir(parents=True)
    path = directory / ('depth.a.jsonl' + ('.zst' if compressed else ''))
    record = {'v': version, 'rw': 7, 'rm': 3,
              'p': {'data': {'E': 1, 'U': 1, 'u': 1, 'pu': 0, 'b': [], 'a': []}}}
    if version == 1:
        record.update(ch='depth', sym='X', conn='legacy')
    data = (json.dumps(record) + '\n').encode()
    path.write_bytes(zstandard.ZstdCompressor().compress(data) if compressed else data)
    snapshot = directory / ('snapshot.p.jsonl' + ('.zst' if compressed else ''))
    snapshot_record = {'v': version, 'rw': 8, 'rm': 4,
                       'p': {'status': 200, 'body': {'lastUpdateId': 1, 'bids': [], 'asks': []}}}
    if version == 1:
        snapshot_record.update(ch='snapshot', sym='X', conn='legacy')
    snapshot_data = (json.dumps(snapshot_record) + '\n').encode()
    snapshot.write_bytes(zstandard.ZstdCompressor().compress(snapshot_data) if compressed else snapshot_data)
    output = tmp_path / 'quality.parquet'
    monkeypatch.setattr(sys, 'argv', ['book', '--validate', str(path), str(snapshot), '--output', str(output)])
    main()
    assert 'X: diffs=1' in capsys.readouterr().out
    assert output.exists()


@pytest.mark.parametrize('compressed', [False, True])
def test_mixed_append_and_payload_boundaries(tmp_path, compressed):
    directory = tmp_path / 'mexc' / 'BTC_USDT' / '20260101'
    directory.mkdir(parents=True)
    path = directory / ('ticker.a.jsonl' + ('.zst' if compressed else ''))
    legacy = {'v': 1, 'rw': 1, 'ch': 'ticker', 'sym': 'BTC_USDT', 'conn': 'old',
              'p': {'t': 42, 'data': {'cts': 43}}}
    compact = {'v': 2, 'rw': 2, 'rm': 3,
               'p': {'t': 44, 'data': {'cts': 45}, 'sample': {'channel': 'push.ticker', 'symbol': 'BTC_USDT'}}}
    raw = b''.join((json.dumps(row) + '\n').encode() for row in (legacy, compact))
    path.write_bytes(zstandard.ZstdCompressor().compress(raw) if compressed else raw)
    assert list(read_records([path])) == [legacy, dict(compact, venue='mexc', ch='ticker', sym='BTC_USDT')]


def test_funding_history_metadata(tmp_path):
    directory = tmp_path / 'kucoin' / 'XBTUSDTM' / '20260101'
    directory.mkdir(parents=True)
    path = directory / 'funding.p.jsonl'
    row = {'v': 2, 'rw': 5, 'rm': 3,
           'meta': {'last_row_timestamp': 42, 'snapshot': False},
           'p': {'data': [{'timepoint': 42, 'fundingRate': '0.0001'}]}}
    path.write_text(json.dumps(row) + '\n')
    assert list(read_records([path])) == [dict(row, venue='kucoin', ch='funding', sym='XBTUSDTM')]
