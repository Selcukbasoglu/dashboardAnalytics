import unittest

from app.services.global_overview import normalize_coingecko_global, select_global_overview


class GlobalOverviewTests(unittest.TestCase):
    def test_primary_wins_when_valid(self):
        primary = {"total_mcap_usd": 10, "btc_dominance_pct": 50, "source": "coinpaprika"}
        secondary = {"total_mcap_usd": 20, "btc_dominance_pct": 40, "source": "coingecko"}
        selected = select_global_overview(primary, secondary)
        self.assertEqual(selected.get("source"), "coinpaprika")

    def test_fallback_to_secondary_when_primary_invalid(self):
        primary = {"total_mcap_usd": 0, "btc_dominance_pct": 0, "source": "coinpaprika"}
        secondary = {"total_mcap_usd": 20, "btc_dominance_pct": 40, "source": "coingecko"}
        selected = select_global_overview(primary, secondary)
        self.assertEqual(selected.get("source"), "coingecko")

    def test_normalize_coingecko_handles_missing(self):
        self.assertIsNone(normalize_coingecko_global({}))


if __name__ == "__main__":
    unittest.main()
