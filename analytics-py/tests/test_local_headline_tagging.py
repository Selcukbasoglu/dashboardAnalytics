import unittest

from app.models import NewsItem
from app.services.portfolio_engine import build_local_headlines_for_llm, compute_news_impact


class LocalHeadlineTaggingTests(unittest.TestCase):
    def test_build_local_headlines_assigns_symbol_sector_and_theme_tags(self):
        alias_map = {
            "symbols": {
                "ASTOR": {
                    "aliases": ["ASTOR", "Astor Enerji"],
                    "asset_class": "BIST",
                    "sector": "UTILITIES",
                },
                "SOKM": {
                    "aliases": ["SOKM", "Sok Market"],
                    "asset_class": "BIST",
                    "sector": "RETAIL",
                },
            }
        }
        holdings = [
            {"symbol": "ASTOR", "asset_class": "BIST", "sector": "UTILITIES", "weight": 0.30},
            {"symbol": "SOKM", "asset_class": "BIST", "sector": "RETAIL", "weight": 0.20},
        ]
        rows = build_local_headlines_for_llm(
            [
                NewsItem(
                    title="Astor Enerji halka arz kararini revize etti",
                    url="https://www.ekonomim.com/finans/haberler/borsa/astor-enerji-halka-arz-haberi-1",
                    source="ekonomim.com",
                )
            ],
            alias_map,
            holdings,
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        tags = set(row.get("tags") or [])
        self.assertIn("LOCAL_TR_HEADLINE", tags)
        self.assertIn("LOCAL_SCRAPE", tags)
        self.assertIn("PORTFOLIO_SYMBOL_MATCH", tags)
        self.assertIn("PORTFOLIO_NEWS", tags)
        self.assertIn("PORTFOLIO_SECTOR_MATCH", tags)
        self.assertIn("BIST_SYMBOL_MATCH", tags)
        self.assertIn("HALKA_ARZ_THEME", tags)
        self.assertEqual(row.get("portfolioSymbols"), ["ASTOR"])
        self.assertEqual(row.get("portfolioSectors"), ["UTILITIES"])
        self.assertGreater(int(row.get("relevanceHint") or 0), 0)

    def test_build_local_headlines_prioritizes_portfolio_matches(self):
        alias_map = {
            "symbols": {
                "ASTOR": {
                    "aliases": ["ASTOR", "Astor Enerji"],
                    "asset_class": "BIST",
                    "sector": "UTILITIES",
                }
            }
        }
        holdings = [{"symbol": "ASTOR", "asset_class": "BIST", "sector": "UTILITIES", "weight": 0.30}]
        rows = build_local_headlines_for_llm(
            [
                NewsItem(
                    title="Kuresel piyasalarda karisik seyir",
                    url="https://www.paraanaliz.com/2026/borsa/kuresel-piyasalarda-karisik-seyir-g-11/",
                    source="paraanaliz.com",
                ),
                NewsItem(
                    title="Astor Enerji icin SPK onayi",
                    url="https://www.paraanaliz.com/2026/sirketler/astor-enerji-icin-spk-onayi-g-12/",
                    source="paraanaliz.com",
                ),
            ],
            alias_map,
            holdings,
        )

        self.assertEqual(rows[0]["portfolioSymbols"], ["ASTOR"])
        self.assertGreaterEqual(rows[0].get("relevanceHint", 0), rows[1].get("relevanceHint", 0))

    def test_compute_news_impact_adds_portfolio_news_tag_on_direct_match(self):
        alias_map = {
            "symbols": {
                "ASTOR": {
                    "aliases": ["ASTOR", "Astor Enerji"],
                    "asset_class": "BIST",
                    "sector": "INDUSTRIALS",
                }
            }
        }
        items = [
            NewsItem(
                title="Astor Enerji yeni yatirim planini acikladi",
                url="https://www.ekonomim.com/finans/haberler/borsa/astor-enerji-yatirim-planini-acikladi-haberi-1",
                source="ekonomim.com",
                relevance_score=70,
                quality_score=70,
            )
        ]
        out, summary = compute_news_impact(items, alias_map, flow_score=None, risk_flags=[], event_points=None)
        self.assertEqual(len(out), 1)
        self.assertIn("ASTOR", out[0]["matchedSymbols"])
        self.assertIn("direct", summary)
        self.assertIn("PORTFOLIO_NEWS", items[0].tags)
        self.assertIn("PORTFOLIO_SYMBOL_MATCH", items[0].tags)


if __name__ == "__main__":
    unittest.main()
