import unittest
from datetime import datetime, timedelta, timezone

from app.engine.news_engine import annotate_items, build_cluster_id, canonicalize_url, dedup_clusters, select_representative
from app.models import NewsItem


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


class DedupeStabilityTests(unittest.TestCase):
    def test_canonical_url_dedup(self):
        base = "https://example.com/story/123"
        now = datetime.now(timezone.utc)
        items = [
            NewsItem(title="Story A", url=f"{base}?utm_source=twitter", source="a.com", publishedAtISO=_iso(now)),
            NewsItem(title="Story A updated", url=f"{base}?utm_campaign=foo", source="b.com", publishedAtISO=_iso(now + timedelta(hours=3))),
            NewsItem(title="Story A", url=f"{base}?ref=home", source="c.com", publishedAtISO=_iso(now + timedelta(hours=6))),
        ]
        annotated, meta = annotate_items(items, 0.0)
        deduped = dedup_clusters(annotated, meta)
        self.assertEqual(len(deduped), 1)
        canon = canonicalize_url(base)
        self.assertEqual(deduped[0].dedup_cluster_id, build_cluster_id(f"url::{canon}"))

    def test_representative_selection_order(self):
        now = datetime.now(timezone.utc)
        items = [
            NewsItem(title="A", url="http://ex.com/a", source="a.com", relevance_score=60, quality_score=80, publishedAtISO=_iso(now)),
            NewsItem(title="B", url="http://ex.com/b", source="b.com", relevance_score=70, quality_score=80, publishedAtISO=_iso(now - timedelta(hours=1))),
            NewsItem(title="C", url="http://ex.com/c", source="c.com", relevance_score=70, quality_score=70, publishedAtISO=_iso(now + timedelta(hours=1))),
        ]
        meta = {}
        for item in items:
            meta[id(item)] = {"published": datetime.fromisoformat(item.publishedAtISO.replace("Z", "+00:00"))}
        rep = select_representative(items, meta)
        self.assertEqual(rep.title, "B")


if __name__ == "__main__":
    unittest.main()
