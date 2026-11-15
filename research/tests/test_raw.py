import unittest

from liqwatch_research.raw import with_identity


class IdentityTests(unittest.TestCase):
    def test_paths(self):
        cases = [
            ('raw/okx/BTC-USDT-SWAP/20260101/trade.a.jsonl', 'okx', 'BTC-USDT-SWAP', 'trade'),
            ('raw/kucoin/_all/20260101/contracts.p.jsonl', 'kucoin', '', 'contracts'),
            ('raw/binance/gap/20260101/gap.jsonl.zst', 'binance', '', 'gap'),
            ('raw/binance/牛来USDT/20260101/aggTrade.jsonl', 'binance', '牛来USDT', 'aggTrade'),
        ]
        for path, venue, symbol, channel in cases:
            with self.subTest(path=path):
                self.assertEqual(with_identity({'v': 2, 'rw': 42, 'p': {'ts': 1}}, path),
                                 {'v': 2, 'rw': 42, 'p': {'ts': 1}, 'venue': venue,
                                  'sym': symbol, 'ch': channel})

    def test_legacy(self):
        old = {'v': 1, 'ch': 'trade', 'sym': 'X', 'conn': 'c', 'p': {}}
        self.assertEqual(with_identity(old, '/old/flat.zst'), old)

    def test_missing_identity_is_explicit(self):
        with self.assertRaises(ValueError):
            with_identity({'v': 2, 'p': {}}, '/renamed.jsonl')
