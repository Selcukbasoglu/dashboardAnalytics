import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.infra.cache import cache_key
from app.services import portfolio_engine as pe
from app.models import NewsItem


class DummyCache:
    def __init__(self):
        self.store = {}

    def get(self, key: str):
        return self.store.get(key)

    def set(self, key: str, value, ttl_seconds: int):
        self.store[key] = value


class DummyPipeline:
    def __init__(self, cache, top_news=None):
        self.cache = cache
        self._top_news = top_news or []
        self.run_called = False
        self.db = SimpleNamespace()

    def run(self, req):
        self.run_called = True
        flow = SimpleNamespace(flow_score=None, event_study=[])
        risk = SimpleNamespace(flags=[])
        return SimpleNamespace(top_news=self._top_news, flow=flow, risk=risk)


def _news_item():
    return {
        "title": "Test headline",
        "url": "http://example.com",
        "source": "example.com",
        "relevance_score": 50,
        "quality_score": 50,
    }


@patch("app.services.portfolio_engine.fetch_daily_history", return_value=list(range(1, 40)))
@patch("app.services.portfolio_engine.fetch_price", return_value=(100.0, "test", "USD"))
@patch("app.services.portfolio_engine.load_portfolio_holdings", return_value=[{"symbol": "ASTOR", "qty": 10.0}])
class PortfolioCacheTests(unittest.TestCase):
    def test_portfolio_news_cache_fallback_order(self, *_):
        alias_map = pe.load_aliases()
        wl_key = ",".join(sorted(set(pe._build_portfolio_watchlist(alias_map, [{"symbol": "ASTOR", "qty": 10.0}]))))
        key_primary = cache_key("news", "24h", wl_key)
        key_all = cache_key("news", "24h", "all")
        key_empty = cache_key("news", "24h", "")

        with patch.dict(os.environ, {"PORTFOLIO_PIPELINE_ENABLED": "false"}):
            cache = DummyCache()
            cache.store[key_empty] = [_news_item()]
            pipeline = DummyPipeline(cache)
            out = pe.build_portfolio(pipeline, base_currency="TRY", news_horizon="24h")
            self.assertIn("portfolio_news_cache_hit=empty", out.get("debug_notes", []))
            self.assertEqual(out.get("newsImpact", {}).get("coverage", {}).get("total"), 1)

            cache = DummyCache()
            cache.store[key_all] = [_news_item()]
            pipeline = DummyPipeline(cache)
            out = pe.build_portfolio(pipeline, base_currency="TRY", news_horizon="24h")
            self.assertIn("portfolio_news_cache_hit=all", out.get("debug_notes", []))

            cache = DummyCache()
            cache.store[key_primary] = [_news_item()]
            pipeline = DummyPipeline(cache)
            out = pe.build_portfolio(pipeline, base_currency="TRY", news_horizon="24h")
            self.assertIn("portfolio_news_cache_hit=primary", out.get("debug_notes", []))

    def test_portfolio_pipeline_writes_primary_cache(self, *_):
        alias_map = pe.load_aliases()
        wl_key = ",".join(sorted(set(pe._build_portfolio_watchlist(alias_map, [{"symbol": "ASTOR", "qty": 10.0}]))))
        key_primary = cache_key("news", "24h", wl_key)

        with patch.dict(os.environ, {"PORTFOLIO_PIPELINE_ENABLED": "true"}):
            cache = DummyCache()
            pipeline = DummyPipeline(cache, top_news=[NewsItem(**_news_item())])
            out = pe.build_portfolio(pipeline, base_currency="TRY", news_horizon="24h")
            self.assertTrue(pipeline.run_called)
            self.assertIn(key_primary, cache.store)
            self.assertIn("portfolio_pipeline_used=True", out.get("debug_notes", []))

    def test_pipeline_disabled_cache_miss_explained(self, *_):
        with patch.dict(os.environ, {"PORTFOLIO_PIPELINE_ENABLED": "false"}):
            cache = DummyCache()
            pipeline = DummyPipeline(cache)
            out = pe.build_portfolio(pipeline, base_currency="TRY", news_horizon="24h")
            self.assertIn("portfolio_news_cache_hit=miss", out.get("debug_notes", []))
            self.assertIn("portfolio_pipeline_used=False", out.get("debug_notes", []))
            self.assertEqual(out.get("newsImpact", {}).get("coverage", {}).get("total"), 0)


if __name__ == "__main__":
    unittest.main()
