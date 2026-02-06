import unittest
from datetime import datetime, timedelta, timezone

from app.engine.news_engine import ambiguous_ticker_allowed, has_strong_symbol_context, select_queries
from app.providers.rss import _filter_items


def _rfc822(dt: datetime) -> str:
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")


class NewsFormulaSearchFixesTests(unittest.TestCase):
    def test_symbol_context_regex_detects_parens_and_dollar(self):
        self.assertTrue(has_strong_symbol_context("Shares of Apple (AAPL) climbed", "AAPL", False))
        self.assertTrue(has_strong_symbol_context("$BTC breaks resistance", "BTC", True))

    def test_cop28_is_not_treated_as_cop_ticker(self):
        self.assertFalse(ambiguous_ticker_allowed("COP", "cop28 climate summit opens"))

    def test_query_selector_spreads_across_list(self):
        queries = ["q0", "q1", "q2", "q3", "q4", "q5"]
        selected = select_queries(queries, 4)
        self.assertEqual(selected[0], "q0")
        self.assertEqual(len(selected), 4)
        self.assertIn("q5", selected)

    def test_rss_filter_respects_timespan_for_dated_items(self):
        now = datetime.now(timezone.utc)
        old = now - timedelta(days=10)
        items = [
            {"title": "fresh item", "url": "https://ex.com/fresh", "published": _rfc822(now)},
            {"title": "old item", "url": "https://ex.com/old", "published": _rfc822(old)},
        ]
        out = _filter_items(items, "item", max_items=10, strict=False, timespan="24h")
        titles = {it["title"] for it in out}
        self.assertIn("fresh item", titles)
        self.assertNotIn("old item", titles)


if __name__ == "__main__":
    unittest.main()
