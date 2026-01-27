from __future__ import annotations

import os

from app.llm import openai_client


def test_summarize_article_openai_basic(monkeypatch):
    class DummyResp:
        def __init__(self, parsed):
            self.output_parsed = parsed

    class DummyClient:
        def __init__(self, **kwargs):
            self.responses = self

        def parse(self, **kwargs):
            return DummyResp(
                openai_client.ArticleSummary(
                    summary_tr="Ozet metni.",
                    why_it_matters="Kisa piyasa etkisi.",
                    key_points=["madde1", "madde2", "madde3"],
                    confidence=80,
                    data_missing=[],
                )
            )

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(openai_client, "OpenAI", DummyClient)
    openai_client._SUMMARY_CACHE.clear()

    out = openai_client.summarize_article_openai(
        title="Ornek Baslik",
        description="Kisa aciklama",
        content_text=None,
        source_domain="example.com",
        published_ts="2026-01-15T10:00:00Z",
        url="https://example.com/article",
        timeout=3.0,
    )

    assert out is not None
    assert out.summary_tr == "Ozet metni."
    assert "full_content" in out.data_missing
