import unittest
from datetime import datetime, timezone

from app.models import NewsItem
from app.services.news_pricing import build_announcement_tracker, compute_news_pricing_model


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class NewsPricingTrackerTests(unittest.TestCase):
    def test_tracker_detects_portfolio_upcoming_announcement(self):
        alias_map = {
            "symbols": {
                "ASTOR": {
                    "aliases": ["Astor Enerji", "ASTOR"],
                    "sector": "INDUSTRIALS",
                }
            }
        }
        holdings = [{"symbol": "ASTOR", "sector": "INDUSTRIALS", "weight": 0.4}]
        top_news = [
            NewsItem(
                title="Astor Enerji 12 Subat tarihinde 2025 bilancosunu aciklayacak",
                url="https://example.com/astor-bilanco",
                source="kap.org.tr",
                publishedAtISO=_now_iso(),
            )
        ]

        tracker = build_announcement_tracker(top_news, [], holdings, alias_map, news_horizon="24h")
        upcoming = tracker.get("portfolio_upcoming") or []
        self.assertTrue(upcoming)
        self.assertEqual(upcoming[0]["symbol"], "ASTOR")
        self.assertEqual(upcoming[0]["event_type"], "EARNINGS")
        monthly = (tracker.get("monthly_plan") or {}).get("items") or []
        self.assertTrue(monthly)
        self.assertEqual(monthly[0]["symbol"], "ASTOR")

    def test_tracker_detects_flagship_ceo_statement_and_links_symbols(self):
        alias_map = {
            "symbols": {
                "AMD": {
                    "aliases": ["AMD", "Advanced Micro Devices"],
                    "sector": "SEMICONDUCTORS",
                }
            }
        }
        holdings = [{"symbol": "AMD", "sector": "SEMICONDUCTORS", "weight": 0.35}]
        top_news = [
            NewsItem(
                title="NVIDIA CEO Jensen Huang says AI demand remains strong",
                url="https://example.com/nvda-ceo",
                source="reuters.com",
                publishedAtISO=_now_iso(),
            )
        ]

        tracker = build_announcement_tracker(top_news, [], holdings, alias_map, news_horizon="7d")
        rows = tracker.get("sector_ceo_statements") or []
        self.assertTrue(rows)
        self.assertEqual(rows[0]["sector"], "SEMICONDUCTORS")
        self.assertIn("AMD", rows[0].get("linked_symbols") or [])

    def test_tracker_monthly_plan_includes_product_launch_event(self):
        alias_map = {
            "symbols": {
                "AMD": {
                    "aliases": ["AMD", "Advanced Micro Devices"],
                    "sector": "SEMICONDUCTORS",
                }
            }
        }
        holdings = [{"symbol": "AMD", "sector": "SEMICONDUCTORS", "weight": 0.35}]
        top_news = [
            NewsItem(
                title="AMD will launch new AI accelerator lineup next week",
                url="https://example.com/amd-launch",
                source="reuters.com",
                publishedAtISO=_now_iso(),
            )
        ]
        tracker = build_announcement_tracker(top_news, [], holdings, alias_map, news_horizon="24h")
        monthly = (tracker.get("monthly_plan") or {}).get("items") or []
        self.assertTrue(monthly)
        self.assertEqual(monthly[0]["event_type"], "PRODUCT")
        self.assertLessEqual(int(monthly[0]["eta_days"]), 30)
        by_type = (tracker.get("monthly_plan") or {}).get("by_event_type") or []
        self.assertTrue(any((row.get("event_type") == "PRODUCT") for row in by_type))

    def test_tracker_maps_etf_constituent_news_to_fund_symbol(self):
        alias_map = {
            "symbols": {
                "SIL": {
                    "aliases": ["SIL", "Global X Silver Miners ETF"],
                    "sector": "METALS_MINERS",
                }
            }
        }
        holdings = [{"symbol": "SIL", "sector": "METALS_MINERS", "weight": 0.3}]
        top_news = [
            NewsItem(
                title="Pan American Silver will report earnings next week",
                url="https://example.com/paas-earnings",
                source="reuters.com",
                publishedAtISO=_now_iso(),
                entities=["Pan American Silver", "PAAS"],
            )
        ]
        tracker = build_announcement_tracker(top_news, [], holdings, alias_map, news_horizon="24h")

        monthly = (tracker.get("monthly_plan") or {}).get("items") or []
        sil_rows = [row for row in monthly if row.get("symbol") == "SIL"]
        self.assertTrue(sil_rows)
        row = sil_rows[0]
        self.assertEqual(row.get("event_type"), "EARNINGS")
        self.assertEqual(row.get("match_scope"), "fund_constituent")
        related = row.get("related_constituents") or []
        self.assertTrue(any((c.get("symbol") == "PAAS") for c in related))

        summary = tracker.get("summary") or {}
        self.assertGreaterEqual(int(summary.get("fund_constituent_event_count") or 0), 1)
        self.assertGreaterEqual(int(summary.get("funds_with_constituent_signal") or 0), 1)

    def test_pricing_model_outputs_symbol_and_market_regime(self):
        holdings = [
            {"symbol": "AMD", "weight": 0.4, "sector": "SEMICONDUCTORS"},
            {"symbol": "SOKM", "weight": 0.2, "sector": "CONSUMER_RETAIL"},
        ]
        tracker = {
            "portfolio_upcoming": [{"symbol": "AMD", "event_type": "EARNINGS", "eta_days": 3}],
            "sector_ceo_statements": [],
        }
        news_items = [
            {
                "title": "AMD guidance raised for data center demand",
                "impact_by_symbol": {"AMD": 0.26},
                "impact_by_symbol_direct": {"AMD": 0.24},
                "impact_by_symbol_indirect": {"AMD": 0.02},
                "low_signal": False,
                "evidence": {"reaction": {"AMD": {"pre_30m_ret": 0.01, "post_30m_ret": 0.03}}},
            },
            {
                "title": "Retail spending softens amid inflation pressure",
                "impact_by_symbol": {"SOKM": -0.12},
                "impact_by_symbol_direct": {"SOKM": -0.10},
                "impact_by_symbol_indirect": {"SOKM": -0.02},
                "low_signal": False,
                "evidence": {"reaction": {"SOKM": {"pre_30m_ret": 0.02, "post_30m_ret": 0.01}}},
            },
        ]

        model = compute_news_pricing_model(news_items, holdings, tracker)
        self.assertIn("symbol_pricing", model)
        self.assertTrue(model["symbol_pricing"])
        symbols = {row["symbol"] for row in model["symbol_pricing"]}
        self.assertIn("AMD", symbols)
        self.assertIn(model.get("market_regime"), {"BULLISH_PRICING", "BEARISH_PRICING", "NEUTRAL"})


if __name__ == "__main__":
    unittest.main()
