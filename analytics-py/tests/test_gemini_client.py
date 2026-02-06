import json
import os
import unittest
from unittest.mock import patch

import requests

from app.llm import gemini_client


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text
        self.content = b"{}"

    def json(self):
        return self._payload


def _payload() -> dict:
    return {
        "asOfISO": "2026-02-06T12:00:00+03:00",
        "baseCurrency": "TRY",
        "newsHorizon": "24h",
        "period": "daily",
        "coverage": {"matched": 5, "total": 10},
        "stats": {"coverage_ratio": 0.5},
        "riskFlags": ["FX_RISK_UP"],
        "topNews": [
            {"title": "Fed keeps rates unchanged", "source": "reuters.com", "publishedAtISO": "2026-02-06T09:00:00Z"},
            {"title": "Oil output cuts extended", "source": "ft.com", "publishedAtISO": "2026-02-06T08:00:00Z"},
        ],
        "localHeadlines": [
            {"title": "BIST companies release earnings", "source": "kap.org.tr", "publishedAtISO": "2026-02-06T07:00:00Z"}
        ],
        "relatedNews": {
            "ASTOR": [{"title": "Grid demand rises", "impactScore": 0.2, "direction": "positive"}],
            "SOKM": [{"title": "Consumer demand softens", "impactScore": -0.1, "direction": "negative"}],
        },
        "holdings": [{"symbol": "ASTOR", "weight": 0.3, "asset_class": "BIST", "sector": "UTILITIES"}],
        "holdings_full": [{"symbol": "ASTOR", "weight": 0.3, "asset_class": "BIST", "sector": "UTILITIES"}],
        "recommendations": [],
    }


class GeminiClientTests(unittest.TestCase):
    def test_prepare_payload_compacts_and_keeps_news(self):
        payload = _payload()
        payload["topNews"] = payload["topNews"] * 30
        prepared = gemini_client._prepare_payload(payload, budget_chars=5000)
        dumped = json.dumps(prepared, ensure_ascii=True)
        self.assertLessEqual(len(dumped), 5000)
        self.assertTrue(prepared["topNews"])
        self.assertTrue(prepared["localHeadlines"])
        self.assertLessEqual(len(prepared["topNews"]), gemini_client.TOP_NEWS_MAX)

    def test_ensure_sections_adds_required_blocks(self):
        prepared = gemini_client._prepare_payload(_payload(), budget_chars=5000)
        text = gemini_client._ensure_sections("Kisa bir metin.", prepared)
        self.assertIn("Haber Temelli Icgoruler", text)
        self.assertIn("Model Fikirleri", text)

    def test_generate_retries_after_rate_limit(self):
        ok_payload = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"text": "Haber Temelli Icgoruler\n- [KANIT:T1] not\n\nModel Fikirleri (Varsayim)\n- Model gorusu (ORTA): not"}]
                    }
                }
            ]
        }
        with patch.dict(os.environ, {"GEMINI_API_KEY": "x"}, clear=False):
            with patch("app.llm.gemini_client.requests.post") as post, patch("app.llm.gemini_client.time.sleep"):
                post.side_effect = [
                    _FakeResponse(429, text="rate limited"),
                    _FakeResponse(200, payload=ok_payload),
                ]
                with patch("app.llm.gemini_client._call_openrouter_fallback") as fallback:
                    summary, err = gemini_client.generate_portfolio_summary(_payload(), timeout=0.1)
                    self.assertIsNotNone(summary)
                    self.assertIsNone(err)
                    self.assertEqual(post.call_count, 2)
                    fallback.assert_not_called()

    def test_generate_uses_openrouter_after_final_rate_limit(self):
        with patch.dict(os.environ, {"GEMINI_API_KEY": "x"}, clear=False):
            with patch("app.llm.gemini_client.requests.post") as post, patch("app.llm.gemini_client.time.sleep"):
                post.side_effect = [
                    _FakeResponse(429, text="rate limited"),
                    _FakeResponse(429, text="rate limited"),
                ]
                with patch("app.llm.gemini_client._call_openrouter_fallback", return_value=("Fallback text", None)):
                    summary, err = gemini_client.generate_portfolio_summary(_payload(), timeout=0.1)
                    self.assertIsNotNone(summary)
                    self.assertEqual(err, "fallback_openrouter")
                    self.assertIn("Model Fikirleri", summary)

    def test_generate_retries_without_system_instruction_after_400(self):
        call_log: list[tuple[str, bool]] = []
        ok_payload = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": (
                                    "Haber Temelli Icgoruler\n- [KANIT:T1] deneme\n\n"
                                    "Model Fikirleri (Varsayim)\n- Model gorusu (ORTA): deneme"
                                )
                            }
                        ]
                    }
                }
            ]
        }

        def _fake_post(url, headers=None, json=None, timeout=None):  # noqa: A002
            has_system = isinstance(json, dict) and "systemInstruction" in json
            call_log.append((url, has_system))
            if len(call_log) == 1:
                return _FakeResponse(400, text="invalid field: systemInstruction")
            return _FakeResponse(200, payload=ok_payload)

        with patch.dict(os.environ, {"GEMINI_API_KEY": "x"}, clear=False):
            with patch("app.llm.gemini_client.requests.post", side_effect=_fake_post):
                summary, err = gemini_client.generate_portfolio_summary(_payload(), timeout=0.1)
        self.assertIsNotNone(summary)
        self.assertIsNone(err)
        self.assertEqual(len(call_log), 2)
        self.assertTrue(call_log[0][1])
        self.assertFalse(call_log[1][1])

    def test_generate_switches_v1beta_to_v1_after_404(self):
        call_log: list[str] = []
        ok_payload = {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": (
                                    "Haber Temelli Icgoruler\n- [KANIT:T1] deneme\n\n"
                                    "Model Fikirleri (Varsayim)\n- Model gorusu (ORTA): deneme"
                                )
                            }
                        ]
                    }
                }
            ]
        }

        def _fake_post(url, headers=None, json=None, timeout=None):  # noqa: A002
            call_log.append(url)
            if len(call_log) == 1:
                return _FakeResponse(404, text="not found")
            return _FakeResponse(200, payload=ok_payload)

        with patch.dict(os.environ, {"GEMINI_API_KEY": "x"}, clear=False):
            with patch("app.llm.gemini_client.requests.post", side_effect=_fake_post):
                summary, err = gemini_client.generate_portfolio_summary(_payload(), timeout=0.1)
        self.assertIsNotNone(summary)
        self.assertIsNone(err)
        self.assertEqual(len(call_log), 2)
        self.assertIn("/v1beta/models", call_log[0])
        self.assertIn("/v1/models", call_log[1])

    def test_generate_uses_openrouter_after_timeout(self):
        with patch.dict(os.environ, {"GEMINI_API_KEY": "x"}, clear=False):
            with patch("app.llm.gemini_client.requests.post", side_effect=requests.Timeout):
                with patch("app.llm.gemini_client._call_openrouter_fallback", return_value=("Fallback timeout", None)):
                    summary, err = gemini_client.generate_portfolio_summary(_payload(), timeout=0.1)
        self.assertIsNotNone(summary)
        self.assertEqual(err, "fallback_openrouter")
        self.assertIn("Model Fikirleri", summary)

    def test_generate_returns_rate_limit_error_when_fallback_fails(self):
        with patch.dict(os.environ, {"GEMINI_API_KEY": "x"}, clear=False):
            with patch("app.llm.gemini_client.requests.post") as post, patch("app.llm.gemini_client.time.sleep"):
                post.side_effect = [
                    _FakeResponse(429, text="rate limited"),
                    _FakeResponse(429, text="rate limited"),
                ]
                with patch("app.llm.gemini_client._call_openrouter_fallback", return_value=(None, "missing_openrouter_key")):
                    summary, err = gemini_client.generate_portfolio_summary(_payload(), timeout=0.1)
        self.assertIsNone(summary)
        self.assertTrue((err or "").startswith("gemini_rate_limited:"))

    def test_build_prompt_contains_contract_sections(self):
        prepared = gemini_client._prepare_payload(_payload(), budget_chars=5000)
        prompt = gemini_client._build_prompt(prepared)
        self.assertIn("ZORUNLU CIKTI BASLIKLARI", prompt)
        self.assertIn("Haber Temelli Icgoruler", prompt)
        self.assertIn("Model Fikirleri (Varsayim)", prompt)
        self.assertIn("[KANIT:Tx/Lx]", prompt)

    def test_generate_returns_missing_key_without_env(self):
        with patch.dict(os.environ, {}, clear=True):
            summary, err = gemini_client.generate_portfolio_summary(_payload(), timeout=0.1)
        self.assertIsNone(summary)
        self.assertEqual(err, "missing_key")


if __name__ == "__main__":
    unittest.main()
