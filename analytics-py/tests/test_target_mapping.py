import unittest

from app.models import NewsItem
from app.services.event_store import _relevance_targets


def _targets(item: NewsItem) -> dict[str, float]:
    return {asset: score for asset, score in _relevance_targets(item)}


class TargetMappingTests(unittest.TestCase):
    def test_stablecoin_regulation_targets(self):
        item = NewsItem(
            title="SEC enforcement action targets stablecoin issuer",
            url="http://example.com",
            source="example.com",
            event_type="REGULATION_LEGAL",
            tags=["Reg", "Stablecoin"],
        )
        t = _targets(item)
        self.assertTrue(all(k in t for k in ("BTC", "ETH", "ALTS", "STABLES")))
        self.assertGreater(t["STABLES"], t["BTC"])
        self.assertGreaterEqual(t["STABLES"], t["ALTS"])

    def test_etf_flow_targets_btc(self):
        item = NewsItem(
            title="Spot ETF inflow accelerates after approval",
            url="http://example.com/etf",
            source="example.com",
            tags=["ETF"],
        )
        t = _targets(item)
        self.assertGreater(t["BTC"], t["ALTS"])
        self.assertGreater(t["BTC"], t["ETH"])

    def test_l2_defi_targets_alts(self):
        item = NewsItem(
            title="Arbitrum L2 rollup activity surges in DeFi",
            url="http://example.com/l2",
            source="example.com",
        )
        t = _targets(item)
        self.assertGreater(t["ALTS"], t["ETH"])
        self.assertGreater(t["ALTS"], t["BTC"])

    def test_btc_specific_targets(self):
        item = NewsItem(
            title="Bitcoin climbs as BTC demand rises",
            url="http://example.com/btc",
            source="example.com",
        )
        t = _targets(item)
        self.assertGreater(t["BTC"], t["ETH"])
        self.assertGreater(t["BTC"], t["ALTS"])

    def test_exchange_regulation_targets_stables(self):
        item = NewsItem(
            title="Regulators tighten exchange custody requirements",
            url="http://example.com/exchange",
            source="example.com",
            tags=["Reg"],
        )
        t = _targets(item)
        self.assertGreater(t["STABLES"], t["BTC"])

    def test_defaults_present(self):
        item = NewsItem(
            title="Minor corporate update",
            url="http://example.com/minor",
            source="example.com",
            news_scope="COMPANY",
        )
        t = _targets(item)
        self.assertTrue(all(k in t for k in ("BTC", "ETH", "ALTS", "STABLES")))
        self.assertTrue(all(0 <= v <= 1 for v in t.values()))


if __name__ == "__main__":
    unittest.main()
