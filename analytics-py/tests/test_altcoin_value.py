import unittest

from app.services.altcoin import compute_altcoin_total_value_ex_btc


class AltcoinValueTests(unittest.TestCase):
    def test_formula(self):
        total = 1_000_000_000_000
        btc_dom = 50.0
        expected = 500_000_000_000
        self.assertEqual(compute_altcoin_total_value_ex_btc(total, btc_dom), expected)

    def test_missing_inputs(self):
        self.assertIsNone(compute_altcoin_total_value_ex_btc(None, 50.0))
        self.assertIsNone(compute_altcoin_total_value_ex_btc(1_000_000_000_000, None))
        self.assertIsNone(compute_altcoin_total_value_ex_btc(0, 50.0))
        self.assertIsNone(compute_altcoin_total_value_ex_btc(1_000_000_000_000, 0))


if __name__ == "__main__":
    unittest.main()
