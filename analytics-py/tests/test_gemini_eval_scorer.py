import unittest

from app.evals.gemini_summary.scorer import evaluate_case, score_summary
from app.llm import gemini_client


class GeminiEvalScorerTests(unittest.TestCase):
    def _payload(self):
        return {
            "asOfISO": "2026-02-08T12:00:00Z",
            "baseCurrency": "TRY",
            "newsHorizon": "24h",
            "period": "daily",
            "coverage": {"matched": 4, "total": 8},
            "stats": {"coverage_ratio": 0.5},
            "riskFlags": ["FX_RISK_UP"],
            "topNews": [
                {
                    "title": "AMD raises AI guidance after strong demand",
                    "source": "reuters.com",
                    "publishedAtISO": "2026-02-08T09:00:00Z",
                }
            ],
            "localHeadlines": [
                {
                    "title": "ASTOR siparis akisi gucleniyor",
                    "source": "kap.org.tr",
                    "publishedAtISO": "2026-02-08T08:00:00Z",
                    "tags": ["PORTFOLIO_SYMBOL_MATCH"],
                    "portfolioSymbols": ["ASTOR"],
                    "portfolioSectors": ["POWER_UTILITIES"],
                    "relevanceHint": 8,
                }
            ],
            "relatedNews": {
                "ASTOR": [{"title": "ASTOR order pipeline improves", "direction": "positive", "impactScore": 0.16}],
                "AMD": [{"title": "AI demand lifts AMD outlook", "direction": "positive", "impactScore": 0.21}],
            },
            "holdings": [
                {"symbol": "ASTOR", "weight": 0.3, "asset_class": "BIST", "sector": "POWER_UTILITIES"},
                {"symbol": "AMD", "weight": 0.2, "asset_class": "NASDAQ", "sector": "SEMICONDUCTORS"},
            ],
            "holdings_full": [
                {"symbol": "ASTOR", "weight": 0.3, "asset_class": "BIST", "sector": "POWER_UTILITIES"},
                {"symbol": "AMD", "weight": 0.2, "asset_class": "NASDAQ", "sector": "SEMICONDUCTORS"},
            ],
            "recommendations": [],
            "announcementTracker": {"portfolio_upcoming": [], "summary": {}},
            "newsPricingModel": {"market_pressure_score": 0.12, "market_regime": "BULLISH_PRICING", "symbol_pricing": []},
        }

    def test_rule_based_summary_scores_high_enough(self):
        payload = self._payload()
        prepared = gemini_client._prepare_payload(payload, budget_chars=5000)
        summary = gemini_client._build_rule_based_summary(prepared)

        result = score_summary(summary, payload)
        scores = result["scores"]

        self.assertGreaterEqual(scores["header_coverage"], 0.99)
        self.assertGreaterEqual(scores["evidence_density"], 0.5)
        self.assertGreaterEqual(scores["portfolio_grounding"], 0.5)
        self.assertGreaterEqual(scores["assumption_hygiene"], 0.66)
        self.assertGreaterEqual(scores["overall"], 0.68)

    def test_template_like_summary_fails_case_expectation(self):
        payload = self._payload()
        bad_summary = """Haber Temelli Icgoruler
- [KANIT:T1] basligina dayali etki notu.

Sektor Etkisi
- yetersiz veri.

Portfoy Hisse Etkisi
- yetersiz veri.

Portfoy Disi Pozitif Etkiler
- yetersiz veri.

Portfoy Disi Negatif Etkiler
- yetersiz veri.

Model Fikirleri (Varsayim)
- not"""

        case = {
            "id": "bad_case",
            "expect": {
                "min_scores": {"overall": 0.6, "non_template": 0.8},
                "forbid": ["basligina dayali etki notu"],
            },
        }

        evaluated = evaluate_case(case, bad_summary, payload)
        self.assertFalse(evaluated["passed"])
        self.assertTrue(any(v.startswith("forbidden_text:") for v in evaluated["violations"]))

    def test_case_profile_selection_applies_strict_thresholds(self):
        payload = self._payload()
        summary = "Haber Temelli Icgoruler\n- [KANIT:T1] ASTOR etkisi.\n\nSektor Etkisi\n- test\n\nPortfoy Hisse Etkisi\n- ASTOR\n\nPortfoy Disi Pozitif Etkiler\n- test\n\nPortfoy Disi Negatif Etkiler\n- test\n\nModel Fikirleri (Varsayim)\n- Model gorusu (ORTA): test (varsayim)"
        case = {
            "id": "profile_case",
            "expect_profiles": {
                "soft": {"min_scores": {"overall": 0.3}},
                "strict": {"min_scores": {"overall": 0.95}},
            },
        }
        soft_eval = evaluate_case(case, summary, payload, expect_profile="soft")
        strict_eval = evaluate_case(case, summary, payload, expect_profile="strict")
        self.assertTrue(soft_eval["passed"])
        self.assertFalse(strict_eval["passed"])


if __name__ == "__main__":
    unittest.main()
