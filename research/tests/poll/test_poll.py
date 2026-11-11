import hashlib
import json
from datetime import datetime, timezone

import pyarrow.parquet as pq
import pytest
import requests

from wicker_research.convert import convert
from wicker_research.poll import (BracketsStructureError, Config, Poller, RawWriter,
                                  RetryPoll, validate_brackets)
from wicker_research.replay import Replay, read_records


class Clock:
    def __init__(self, timestamp='2026-11-11T12:00:00+00:00'):
        self.now = datetime.fromisoformat(timestamp).timestamp()
        self.mono = 0

    def time(self):
        return self.now

    def monotonic(self):
        return self.mono

    def sleep(self, seconds):
        self.now += seconds
        self.mono += seconds


class Response:
    def __init__(self, body, status=200, headers=None):
        self.body, self.status_code, self.headers = body, status, headers or {}
        self.content = json.dumps(body).encode()
        self.text = self.content.decode()
        self.closed = False

    def json(self):
        return self.body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def close(self):
        self.closed = True


class Session:
    def __init__(self, *responses):
        self.responses, self.calls = list(responses), []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class Writer:
    def __init__(self):
        self.records = []

    def rotate(self):
        pass

    def write(self, *args):
        self.records.append(args)


def brackets():
    return {'code': '000000', 'message': None, 'messageDetail': None,
        'success': True, 'data': {'brackets': [dict(symbol='BTCUSDT', updateTime=123,
        notionalLimit='100', riskBrackets=[dict(bracketSeq=1, bracketNotionalFloor='0',
        bracketNotionalCap='100', bracketMaintenanceMarginRate='0.01',
        cumFastMaintenanceAmount='0', minOpenPosLeverage=1, maxOpenPosLeverage=20)])]}}


def exchange():
    def row(symbol, **kwargs):
        return dict(symbol=symbol, status=kwargs.get('status', 'TRADING'),
                    quoteAsset=kwargs.get('quoteAsset', 'USDT'),
                    contractType=kwargs.get('contractType', 'PERPETUAL'))
    return {'symbols': [row('BTCUSDT'), row('CLOSEDUSDT', status='BREAK'),
                        row('BTCUSDC', quoteAsset='USDC'),
                        row('QUARTERUSDT', contractType='CURRENT_QUARTER')]}


def poller(tmp_path, *responses, config=None, clock=None):
    return Poller(Session(*responses), Writer(), tmp_path / 'state.json',
                  config=config, clock=clock or Clock())


def test_writer_layout_rotation_restart_and_meta(tmp_path):
    clock = Clock()
    writer = RawWriter(tmp_path, clock)
    writer.write('openInterest', 'BTCUSDT', {'openInterest': '1'}, {'used_weight_1m': 3})
    clock.sleep(3600)
    writer.rotate()
    writer.write('constituents', 'BTCUSDT', {'constituents': []})
    writer.close()
    paths = sorted(tmp_path.glob('*/*.jsonl.zst'))
    assert [p.name for p in paths] == ['poll-2026111112.p.jsonl.zst', 'poll-2026111113.p.jsonl.zst']
    rows = list(read_records(paths))
    assert set(rows[0]) == {'v', 'wseq', 'conn', 'cseq', 'rw', 'rm', 'ch', 'sym', 'p', 'meta'}
    assert rows[0]['p'] == {'openInterest': '1'}
    assert rows[0]['meta']['used_weight_1m'] == 3
    assert rows[0]['rm'] == 0 and rows[1]['rm'] == 3600 * 10**9
    assert [r['wseq'] for r in rows] == [1, 2]
    for path in paths:
        meta = json.loads(path.with_name(path.name + '.meta.json').read_text())
        assert meta == dict(size=path.stat().st_size, chunks=[path.name],
                            sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    writer = RawWriter(tmp_path, clock)
    writer.write('exchangeInfo', '', exchange())
    writer.close()
    assert (paths[1].parent / 'poll-2026111113.p.r1.jsonl.zst').exists()


def test_writer_lock_and_crash_recovery(tmp_path):
    writer = RawWriter(tmp_path, Clock())
    writer.write('brackets', 'BTCUSDT', brackets()['data']['brackets'][0])
    with pytest.raises(OSError):
        RawWriter(tmp_path, Clock())
    writer.file.close()
    writer.lock.close()
    restarted = RawWriter(tmp_path, Clock())
    restarted.close()
    assert len(list(read_records(tmp_path.glob('*/*.jsonl.zst')))) == 1


def test_startup_schedule_filter_and_hash(tmp_path):
    response = Response(brackets())
    p = poller(tmp_path, Response(exchange()), response, Response({'openInterest': '2'},
               headers={'X-MBX-USED-WEIGHT-1M': '17'}), Response({'constituents': []}))
    for _ in range(4):
        p.step()
    assert p.state['symbols'] == ['BTCUSDT']
    assert [r[0] for r in p.writer.records] == ['exchangeInfo', 'brackets', 'openInterest', 'constituents']
    assert p.writer.records[1][3]['response_sha256'] == hashlib.sha256(response.content).hexdigest()
    assert p.writer.records[2][3]['used_weight_1m'] == 17
    assert p.session.calls[1][0] == 'POST' and p.session.calls[1][2]['json'] == {}
    assert p.jobs['openInterest', 'BTCUSDT'] == p.clock.time() + 300
    assert all(r.closed for r in [response])


def test_daily_and_five_minute_schedule_restart(tmp_path):
    clock = Clock('2026-11-11T23:59:00+00:00')
    p = poller(tmp_path, Response(exchange()), Response(brackets()),
               Response({}), Response({}), clock=clock)
    for _ in range(4):
        p.step()
    midnight = datetime(2026, 11, 12, tzinfo=timezone.utc).timestamp()
    assert p.jobs['brackets', ''] == midnight + 300
    assert p.jobs['openInterest', 'BTCUSDT'] == midnight
    same_day = Poller(Session(), Writer(), p.state_path, clock=clock)
    assert same_day.jobs['brackets', ''] == midnight + 300
    clock.sleep(61)
    restarted = Poller(Session(), Writer(), p.state_path, clock=clock)
    assert restarted.jobs['brackets', ''] == clock.time()  # missing today's snapshot


@pytest.mark.parametrize('symbols, expected', [(None, ['BTCUSDT', 'ETHUSDT']),
                                              (('ETHUSDT',), ['ETHUSDT'])])
def test_symbol_filter_jobs_and_full_snapshots(tmp_path, symbols, expected):
    body = exchange()
    body['symbols'].append(dict(body['symbols'][0], symbol='ETHUSDT'))
    bracket_body = brackets()
    bracket_body['data']['brackets'].append(
        dict(bracket_body['data']['brackets'][0], symbol='ETHUSDT'))
    p = poller(tmp_path, Response(body), Response(bracket_body), config=Config(symbols=symbols))
    p.step()
    p.step()
    p._schedule()
    assert p.state['symbols'] == ['BTCUSDT', 'ETHUSDT']
    for channel in ('openInterest', 'constituents'):
        assert sorted(sym for ch, sym in p.jobs if ch == channel) == expected
    assert p.writer.records[0][2] == body
    assert [r[1] for r in p.writer.records if r[0] == 'brackets'] == ['BTCUSDT', 'ETHUSDT']
    restarted = Poller(Session(), Writer(), p.state_path, config=Config(symbols=symbols), clock=p.clock)
    for channel in ('openInterest', 'constituents'):
        assert sorted(sym for ch, sym in restarted.jobs if ch == channel) == expected


def test_missing_symbols_warn_once_per_day_and_persist(tmp_path, caplog):
    config = Config(symbols=('BTCUSDT', 'MISSINGUSDT', 'CLOSEDUSDT', 'BTCUSDC', 'QUARTERUSDT'))
    p = poller(tmp_path, Response(exchange()), config=config)
    p.step()
    expected = ['BTCUSDC', 'CLOSEDUSDT', 'MISSINGUSDT', 'QUARTERUSDT']
    assert p.state['missing_symbols'] == expected
    assert json.loads(p.state_path.read_text())['missing_symbols'] == expected
    assert len(caplog.records) == 1 and caplog.records[0].levelname == 'WARNING'
    assert all(sym in caplog.text for sym in expected)
    p._schedule()
    restarted = Poller(Session(), Writer(), p.state_path, config=config, clock=p.clock)
    assert len(caplog.records) == 1
    assert {sym for _, sym in restarted.jobs if sym} == {'BTCUSDT'}
    p.clock.sleep(86400)
    restarted._schedule()
    restarted._schedule()
    assert len(caplog.records) == 2
    restarted.session.responses.append(Response(exchange()))
    restarted.jobs = {('exchangeInfo', ''): p.clock.time()}
    restarted.step()
    assert len(caplog.records) == 2


@pytest.mark.parametrize('value', ['', ',', 'BTCUSDT,', ',BTCUSDT', 'BTCUSDT,,ETHUSDT',
                                  'btcusdt', 'BTCUSDT,ethusdt', 'BTC USDT', 'BTCUSDT;ETHUSDT'])
def test_cli_invalid_symbols_rejected(tmp_path, monkeypatch, capsys, value):
    from wicker_research.poll.__main__ import main

    raw = tmp_path / 'raw'
    monkeypatch.setattr('sys.argv', ['wickerpoll', '--raw', str(raw), '--syms', value])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
    assert 'comma-separated uppercase symbols without empty items' in capsys.readouterr().err
    assert not raw.exists()


def test_cli_parses_symbols(tmp_path, monkeypatch):
    from wicker_research.poll import __main__ as cli

    configs = []

    class StoppedPoller:
        def __init__(self, session, writer, state_path, config):
            configs.append(config)

        def run(self):
            raise KeyboardInterrupt

    monkeypatch.setattr(cli, 'Poller', StoppedPoller)
    monkeypatch.setattr('sys.argv', ['wickerpoll', '--raw', str(tmp_path), '--syms',
                                    'BTCUSDT,ETHUSDT', '--weight-cap', '120'])
    assert cli.main() == 0
    assert configs[0].symbols == ('BTCUSDT', 'ETHUSDT')
    assert configs[0].weight_cap == 120


@pytest.mark.parametrize('symbols', [(), ('',), ('btcusdt',), ('BTCUSDT,ETHUSDT',),
                                    ('BTC USDT',), (None,), 'BTCUSDT'])
def test_invalid_symbol_config(symbols):
    with pytest.raises(ValueError, match='uppercase symbols'):
        Config(symbols=symbols)


def test_weight_cap_and_header_backoff(tmp_path):
    p = poller(tmp_path, Response({}, headers={'x-mbx-used-weight-1m': '241'}), Response({}))
    p._request('openInterest', 'BTCUSDT')
    p.clock.sleep(1)
    with pytest.raises(RetryPoll) as exc:
        p._request('constituents', 'BTCUSDT')
    assert exc.value.delay == 59
    assert len(p.session.calls) == 1
    p.clock.sleep(59)
    p._request('openInterest', 'BTCUSDT')
    assert len(p.session.calls) == 2


def test_local_weight_and_rate_caps(tmp_path):
    p = poller(tmp_path, Response({}), Response({}), config=Config(weight_cap=2))
    p._request('openInterest', 'BTCUSDT')
    with pytest.raises(RetryPoll) as exc:
        p._request('openInterest', 'ETHUSDT')
    assert exc.value.delay == 0.5
    p.clock.sleep(0.5)
    p._request('openInterest', 'ETHUSDT')
    p.clock.sleep(0.5)
    with pytest.raises(RetryPoll) as exc:
        p._request('constituents', 'BTCUSDT')
    assert exc.value.delay == 59


@pytest.mark.parametrize('status', [418, 429])
@pytest.mark.parametrize('retry', ['90', 'Wed, 11 Nov 2026 12:01:30 GMT'])
def test_retry_after_blocks_endpoint_not_other_jobs(tmp_path, status, retry):
    response = Response({}, status, {'Retry-After': retry})
    p = poller(tmp_path, response, Response({}))
    with pytest.raises(RetryPoll) as exc:
        p._request('openInterest', 'BTCUSDT')
    assert exc.value.delay == 90
    p.clock.sleep(1)
    with pytest.raises(RetryPoll):
        p._request('openInterest', 'ETHUSDT')
    p._request('constituents', 'BTCUSDT')
    assert response.closed and len(p.session.calls) == 2


@pytest.mark.parametrize('failure', [requests.Timeout('timeout'), Response({}, 503),
                                   Response({}, 400)])
def test_failed_poll_retained_and_retried(tmp_path, failure, caplog):
    p = poller(tmp_path, failure, Response(exchange()))
    p.jobs = {('exchangeInfo', ''): p.clock.time(), ('brackets', ''): p.clock.time() + 100}
    p.step()
    assert not p.writer.records and p.failures['exchangeInfo', ''] == 1
    assert 'deferred' in caplog.text
    p.clock.sleep(1)
    p.step()
    assert p.writer.records[0][0] == 'exchangeInfo'
    assert ('exchangeInfo', '') not in p.failures


@pytest.mark.parametrize('mutation', ['top', 'entry', 'tier', 'empty', 'duplicate',
                                     'legacy_list', 'missing_brackets', 'object_brackets',
                                     'missing_success', 'false_success', 'missing_code'])
def test_schema_change_no_partial_valid_records(tmp_path, mutation):
    body = brackets()
    if mutation == 'top':
        del body['data']
    elif mutation == 'entry':
        del body['data']['brackets'][0]['updateTime']
    elif mutation == 'tier':
        bad = json.loads(json.dumps(body['data']['brackets'][0]))
        bad['symbol'] = 'ETHUSDT'
        del bad['riskBrackets'][0]['cumFastMaintenanceAmount']
        body['data']['brackets'].append(bad)
    elif mutation == 'duplicate':
        body['data']['brackets'].append(body['data']['brackets'][0])
    elif mutation == 'legacy_list':
        body['data'] = body['data']['brackets']
    elif mutation == 'missing_brackets':
        del body['data']['brackets']
    elif mutation == 'object_brackets':
        body['data']['brackets'] = {}
    elif mutation == 'missing_success':
        del body['success']
    elif mutation == 'false_success':
        body['success'] = False
    elif mutation == 'missing_code':
        del body['code']
    else:
        body['data']['brackets'] = []
    p = poller(tmp_path, Response(body))
    p.jobs = {('brackets', ''): p.clock.time(), ('exchangeInfo', ''): p.clock.time() + 100}
    with pytest.raises(BracketsStructureError):
        p.step()
    assert [r[0] for r in p.writer.records] == ['brackets_error']
    assert p.writer.records[0][2] == body
    assert 'brackets' not in p.state


def test_valid_brackets():
    assert validate_brackets(brackets()) == brackets()['data']['brackets']


def test_brackets_nullable_update_time(tmp_path):
    body = brackets()
    body['data']['brackets'][0]['updateTime'] = None
    p = poller(tmp_path, Response(body))
    p.jobs['exchangeInfo', ''] += 100
    p.step()
    assert p.writer.records[0][2]['updateTime'] is None


def test_brackets_split_one_record_per_symbol(tmp_path):
    body = brackets()
    second = json.loads(json.dumps(body['data']['brackets'][0]))
    second['symbol'] = 'ETHUSDT'
    body['data']['brackets'].append(second)
    response = Response(body)
    p = poller(tmp_path, response)
    p.jobs['exchangeInfo', ''] += 100
    p.step()
    assert [(r[0], r[1], r[2]) for r in p.writer.records] == [
        ('brackets', entry['symbol'], entry) for entry in body['data']['brackets']]
    assert all(r[3]['response_sha256'] == hashlib.sha256(response.content).hexdigest()
               for r in p.writer.records)


def test_running_daily_job_waits_until_0005(tmp_path):
    clock = Clock('2026-11-11T23:59:00+00:00')
    p = poller(tmp_path, Response(exchange()), Response(brackets()), clock=clock)
    p.step()
    p.step()
    clock.sleep(300)
    p.jobs.pop(('openInterest', 'BTCUSDT'), None)
    p.jobs.pop(('constituents', 'BTCUSDT'), None)
    p.state['symbols'] = []
    assert p.step() == 1
    assert len(p.session.calls) == 2
    clock.sleep(60)
    p.session.responses.append(Response(exchange()))
    p.step()
    assert len(p.session.calls) == 3


def test_exponential_retry_and_malformed_retry_after(tmp_path):
    p = poller(tmp_path, requests.Timeout(), requests.Timeout(), Response(exchange()))
    p.jobs['brackets', ''] += 100
    p.step()
    p.clock.sleep(1)
    p.step()
    assert p.jobs['exchangeInfo', ''] == p.clock.time() + 2
    p.clock.sleep(2)
    p.step()
    assert p.writer.records[0][0] == 'exchangeInfo'
    p.session.responses.append(Response({}, 429, {'Retry-After': 'invalid'}))
    with pytest.raises(RetryPoll) as exc:
        p._request('openInterest', 'BTCUSDT')
    assert exc.value.delay == 60


def test_non_json_brackets_preserved_with_hash(tmp_path):
    class NonJSON(Response):
        def json(self):
            raise ValueError('not JSON')
    response = NonJSON('html')
    p = poller(tmp_path, response)
    p.jobs['exchangeInfo', ''] += 100
    with pytest.raises(BracketsStructureError):
        p.step()
    ch, _, body, meta, _ = p.writer.records[0]
    assert ch == 'brackets_error' and body == response.text
    assert meta['response_sha256'] == hashlib.sha256(response.content).hexdigest()


def test_additive_replay_and_parquet_roundtrip(tmp_path):
    writer = RawWriter(tmp_path / 'raw', Clock())
    channels = ('brackets', 'exchangeInfo', 'constituents', 'openInterest', 'brackets_error')
    for channel in channels:
        writer.write(channel, 'BTCUSDT', {'nested': [1, {'x': '2'}]}, {'used_weight_1m': 7})
    writer.close()
    paths = list((tmp_path / 'raw').glob('*/*.jsonl.zst'))
    assert [e.kind for e in Replay(paths)] == list(channels)
    counts, _, _ = convert(paths, tmp_path / 'out')
    assert counts == {'poll_' + ch: 1 for ch in channels}
    for channel in channels:
        path = next((tmp_path / 'out' / ('poll_' + channel)).rglob('*.parquet'))
        row = pq.read_table(path).to_pylist()[0]
        assert json.loads(row['payload_json']) == {'nested': [1, {'x': '2'}]}
        assert json.loads(row['meta_json']) == {'used_weight_1m': 7}


@pytest.mark.parametrize('kwargs', [dict(weight_cap=0), dict(oi_weight=0),
                                   dict(oi_rps=3), dict(constituents_rps=2)])
def test_invalid_config(kwargs):
    with pytest.raises(ValueError):
        Config(**kwargs)
