import unittest
from datetime import datetime, timezone

from app.config import Settings
from app.engine.news_engine import final_rank_score, normalize_rank_weights
from app.infra.db import init_db
from app.models import NewsItem
from app.services.event_store import store_events


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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
        source_tiers={"primary": ["example.com"], "tier1": [], "tier2": [], "social": []},
    )


class ScoreScalingTests(unittest.TestCase):
    def test_rank_score_impacts_order(self):
        base = NewsItem(
            title="Test",
            url="http://example.com",
            source="example.com",
            relevance_score=70,
            quality_score=70,
            impact_potential=0,
            scope_score=0,
        )
        high = NewsItem(
            title="Test 2",
            url="http://example.com/2",
            source="example.com",
            relevance_score=70,
            quality_score=70,
            impact_potential=80,
            scope_score=0,
        )
        self.assertGreater(final_rank_score(high), final_rank_score(base))

    def test_rank_score_bounds(self):
        item = NewsItem(
            title="Bounds",
            url="http://example.com/3",
            source="example.com",
            relevance_score=100,
            quality_score=100,
            impact_potential=100,
            scope_score=100,
        )
        score = final_rank_score(item)
        self.assertGreaterEqual(score, 0)
        self.assertLessEqual(score, 100.0)
        self.assertAlmostEqual(score, 100.0, places=2)

    def test_event_impact_scaled_to_100(self):
        settings = _settings()
        db = init_db(settings.database_url)
        item = NewsItem(
            title="War risk escalates in region",
            url="http://example.com/war",
            source="example.com",
            tags=["War"],
            event_type="SANCTIONS_GEOPOLITICS",
            publishedAtISO=_now_iso(),
        )
        stored = store_events(db, [item], settings)
        self.assertEqual(stored, 1)
        row = db.fetchone("SELECT impact_score FROM events LIMIT 1")
        self.assertIsNotNone(row)
        impact = float(row["impact_score"] or 0.0)
        self.assertGreaterEqual(impact, 1.0)
        self.assertLessEqual(impact, 100.0)

    def test_rank_weights_are_normalized(self):
        out = normalize_rank_weights({"relevance": 3.0, "quality": 1.0, "impact": 0.0, "scope": -5.0})
        self.assertAlmostEqual(sum(out.values()), 1.0, places=6)
        self.assertGreaterEqual(out["relevance"], 0.0)
        self.assertGreaterEqual(out["quality"], 0.0)
        self.assertEqual(out["scope"], 0.0)


if __name__ == "__main__":
    unittest.main()
