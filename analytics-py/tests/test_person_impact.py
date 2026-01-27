import unittest
from datetime import datetime, timezone

from app.engine.person_impact import extract_person_event, score_person_impact
from app.engine.news_engine import final_rank_score
from app.models import NewsItem


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _item(title: str, tags: list[str], entities: list[str]):
    return {
        "title": title,
        "description": "",
        "tags": tags,
        "entities": entities,
        "publishedAtISO": _now_iso(),
        "quality_score": 80,
    }


class PersonImpactTests(unittest.TestCase):
    def test_central_bank_hawkish(self):
        item = _item(
            "Jerome Powell says rate hike likely as inflation persistent",
            ["PERSONAL", "CENTRAL_BANK_HEADS"],
            ["Jerome Powell"],
        )
        event = extract_person_event(item)
        self.assertIsNotNone(event)
        self.assertEqual(event["stance"], "HAWKISH")
        self.assertIn("risk_off", event["asset_class_bias"])
        impact, _ = score_person_impact(event, item)
        self.assertGreaterEqual(impact, 70)

    def test_central_bank_dovish(self):
        item = _item(
            "Christine Lagarde signals rate cut soon and easing",
            ["PERSONAL", "CENTRAL_BANK_HEADS"],
            ["Christine Lagarde"],
        )
        event = extract_person_event(item)
        self.assertEqual(event["stance"], "DOVISH")
        self.assertIn("risk_on", event["asset_class_bias"])

    def test_regulator_enforcement(self):
        item = _item(
            "Gary Gensler announces enforcement lawsuit against crypto exchange",
            ["PERSONAL", "REGULATORS"],
            ["Gary Gensler"],
        )
        event = extract_person_event(item)
        self.assertEqual(event["stance"], "RISK_ESCALATE")
        self.assertIn("crypto_reg_risk", event["asset_class_bias"])

    def test_regulator_relief(self):
        item = _item(
            "Rostin Behnam says approval framework for crypto licenses",
            ["PERSONAL", "REGULATORS"],
            ["Rostin Behnam"],
        )
        event = extract_person_event(item)
        self.assertEqual(event["stance"], "RISK_DEESCALATE")
        self.assertIn("risk_on", event["asset_class_bias"])

    def test_energy_cut(self):
        item = _item(
            "Prince Abdulaziz bin Salman says OPEC will cut output by 1 million bpd",
            ["PERSONAL", "ENERGY_MINISTERS"],
            ["Prince Abdulaziz bin Salman"],
        )
        event = extract_person_event(item)
        self.assertEqual(event["stance"], "RISK_ESCALATE")
        self.assertIn("energy_up", event["asset_class_bias"])

    def test_energy_increase(self):
        item = _item(
            "Haitham Al Ghais says OPEC to increase output next month",
            ["PERSONAL", "ENERGY_MINISTERS"],
            ["Haitham Al Ghais"],
        )
        event = extract_person_event(item)
        self.assertEqual(event["stance"], "RISK_DEESCALATE")
        self.assertIn("energy_down", event["asset_class_bias"])

    def test_rank_score_uses_impact_potential(self):
        base = NewsItem(
            title="Test",
            url="http://example.com",
            source="example.com",
            relevance_score=80,
            quality_score=80,
        )
        high = NewsItem(
            title="Test 2",
            url="http://example.com/2",
            source="example.com",
            relevance_score=80,
            quality_score=80,
            impact_potential=100,
        )
        self.assertGreater(final_rank_score(high), final_rank_score(base))


if __name__ == "__main__":
    unittest.main()
