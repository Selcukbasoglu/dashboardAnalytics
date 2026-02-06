import unittest
from datetime import datetime, timezone

from app.config import Settings
from app.models import RiskPanel
from app.services.forecasting import ClusterImpact, _aggregate_news_signal


def _settings() -> Settings:
    return Settings(
        openai_api_key=None,
        openai_model="gpt-5-mini",
        enable_openai_summary=False,
        request_timeout=1.0,
        cache_ttl_seconds=1,
        redis_url="",
        database_url="sqlite:///:memory:",
        retention_days=1,
        news_ingest_interval_minutes=30,
        impact_half_life_hours=12.0,
        weights={"market": 0.6, "news": 0.4},
        news_rank_weights={"relevance": 0.45, "quality": 0.30, "impact": 0.15, "scope": 0.10},
        news_rank_profiles={},
        news_rank_profile=None,
        news_rank_profile_auto=True,
        thresholds={"flip_hysteresis": 0.12, "neutral_band_pct": 0.0015, "min_confidence": 0.35},
        min_hold_minutes={"15m": 20, "1h": 75, "3h": 200, "6h": 340},
        source_tiers={"primary": [], "tier1": [], "tier2": [], "social": []},
    )


class ForecastNewsScoreTests(unittest.TestCase):
    def test_news_score_nonzero_with_clusters(self):
        now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        cluster = ClusterImpact(
            cluster_id="c1",
            headline="Major regulatory update",
            source_tier="tier1",
            tags=["Reg"],
            direction=1,
            impact=80.0,
            credibility=0.8,
            severity=0.7,
            ts_utc=now_iso,
            targets=[("BTC", 0.7), ("ETH", 0.5), ("ALTS", 0.8), ("STABLES", 0.9)],
        )
        score, top, contribs = _aggregate_news_signal([cluster], _settings(), "BTC", RiskPanel())
        self.assertGreater(score, 0.0)
        self.assertEqual(len(top), 1)
        self.assertTrue(contribs)

    def test_neutral_clusters_do_not_add_directional_bias(self):
        now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        cluster = ClusterImpact(
            cluster_id="c2",
            headline="Uncertain policy commentary",
            source_tier="tier1",
            tags=["Macro"],
            direction=0,
            impact=85.0,
            credibility=0.9,
            severity=0.7,
            ts_utc=now_iso,
            targets=[("BTC", 0.8)],
        )
        score, top, contribs = _aggregate_news_signal([cluster], _settings(), "BTC", RiskPanel())
        self.assertEqual(score, 0.0)
        self.assertEqual(len(top), 1)
        self.assertTrue(contribs)
        self.assertEqual(contribs[0].get("contrib"), 0.0)

    def test_news_score_zero_when_no_clusters(self):
        score, top, contribs = _aggregate_news_signal([], _settings(), "BTC", RiskPanel())
        self.assertEqual(score, 0.0)
        self.assertEqual(top, [])
        self.assertEqual(contribs, [])


if __name__ == "__main__":
    unittest.main()
