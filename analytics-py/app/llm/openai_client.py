from __future__ import annotations

import hashlib
import json
import os
import time
from typing import List

import httpx
import redis
from cachetools import TTLCache
from pydantic import BaseModel, Field

try:
    from openai import OpenAI
    _OPENAI_IMPORT_ERROR: Exception | None = None
except Exception as exc:
    OpenAI = None  # type: ignore[assignment]
    _OPENAI_IMPORT_ERROR = exc


class ArticleSummary(BaseModel):
    summary_tr: str
    why_it_matters: str
    key_points: List[str] = Field(default_factory=list)
    confidence: int
    data_missing: List[str] = Field(default_factory=list)


class ChunkSummary(BaseModel):
    summary_tr: str
    key_points: List[str] = Field(default_factory=list)


_SUMMARY_TTL = 7 * 24 * 60 * 60
_SUMMARY_CACHE = TTLCache(maxsize=2048, ttl=_SUMMARY_TTL)
_REDIS_CLIENT: redis.Redis | None = None


def _get_redis_client() -> redis.Redis | None:
    global _REDIS_CLIENT
    if _REDIS_CLIENT is not None:
        return _REDIS_CLIENT
    url = os.getenv("REDIS_URL")
    if not url:
        return None
    try:
        client = redis.from_url(url, socket_timeout=1)
        client.ping()
        _REDIS_CLIENT = client
        return client
    except Exception:
        return None


def _cache_key(url: str | None, title: str) -> str:
    base = url or title
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:20]
    return f"summary:{digest}"


def _cache_get(key: str):
    client = _get_redis_client()
    if client is not None:
        try:
            data = client.get(key)
            if data:
                return json.loads(data)
        except Exception:
            pass
    return _SUMMARY_CACHE.get(key)


def _cache_set(key: str, value: dict):
    client = _get_redis_client()
    if client is not None:
        try:
            client.setex(key, _SUMMARY_TTL, json.dumps(value))
        except Exception:
            pass
    _SUMMARY_CACHE[key] = value


def _chunk_text(text: str, chunk_size: int = 3000) -> List[str]:
    chunks = []
    start = 0
    length = len(text)
    while start < length:
        end = min(length, start + chunk_size)
        chunks.append(text[start:end])
        start = end
    return chunks


def _summarize_chunk(client: OpenAI, model: str, title: str, chunk: str) -> ChunkSummary:
    sys_prompt = (
        "Turkce yaz. Basligi tekrar etme. 1-2 cumlelik parca ozeti ve 2-3 kisa madde uret."
    )
    user_payload = {"title": title, "chunk": chunk[:8000]}
    result = client.responses.parse(
        model=model,
        input=[
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": json.dumps(user_payload)},
        ],
        temperature=0,
        response_format=ChunkSummary,
    )
    return result.output_parsed


def summary_enabled() -> bool:
    flag = os.getenv("ENABLE_OPENAI_SUMMARY", "").lower() in ("1", "true", "yes", "on")
    if not flag:
        return False
    if OpenAI is None:
        return False
    if not os.getenv("OPENAI_API_KEY"):
        return False
    return True


def _disabled_summary(reason: str) -> ArticleSummary:
    return ArticleSummary(
        summary_tr="Ozet devre disi",
        why_it_matters=reason,
        key_points=[],
        confidence=0,
        data_missing=["openai_disabled", reason],
    )


def summarize_article_openai(
    title: str,
    description: str | None,
    content_text: str | None,
    source_domain: str | None,
    published_ts: str | None,
    url: str | None = None,
    timeout: float = 12.0,
) -> ArticleSummary | None:
    if not summary_enabled():
        reason = "summary_disabled"
        if os.getenv("ENABLE_OPENAI_SUMMARY", "").lower() not in ("1", "true", "yes", "on"):
            reason = "summary_flag_off"
        elif OpenAI is None:
            reason = "openai_client_unavailable"
        elif not os.getenv("OPENAI_API_KEY"):
            reason = "openai_key_missing"
        return _disabled_summary(reason)
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    model = os.getenv("OPENAI_MODEL", "gpt-5-mini")
    cache_key = _cache_key(url, title)
    cached = _cache_get(cache_key)
    if cached:
        return ArticleSummary(**cached)

    data_missing = []
    if not content_text:
        data_missing.append("full_content")
    if not description:
        data_missing.append("snippet")
    if not os.getenv("OPENAI_MODEL"):
        data_missing.append("openai_model_missing")

    if OpenAI is None:
        return None
    client = OpenAI(api_key=api_key, timeout=timeout)
    system_prompt = (
        "Turkce yaz. Basligi tekrar etme. Etiketleri tekrar yazma. "
        "1-2 cumlelik ozet uret ve piyasa etkisini 1 cumlede acikla."
    )

    if content_text and len(content_text) > 6000:
        chunks = _chunk_text(content_text)
        chunk_outputs = [_summarize_chunk(client, model, title, chunk) for chunk in chunks]
        combined = {
            "title": title,
            "description": description,
            "source_domain": source_domain,
            "published_ts": published_ts,
            "chunk_summaries": [c.model_dump() for c in chunk_outputs],
        }
        result = client.responses.parse(
            model=model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(combined)[:12000]},
            ],
            temperature=0,
            response_format=ArticleSummary,
        )
        summary = result.output_parsed
    else:
        payload = {
            "title": title,
            "description": description,
            "content_text": (content_text or "")[:12000],
            "source_domain": source_domain,
            "published_ts": published_ts,
            "data_missing": data_missing,
        }
        result = client.responses.parse(
            model=model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload)[:12000]},
            ],
            temperature=0,
            response_format=ArticleSummary,
        )
        summary = result.output_parsed

    summary.data_missing = list(dict.fromkeys(summary.data_missing + data_missing))
    if "full_content" in summary.data_missing:
        summary.confidence = min(summary.confidence, 55)
    if "snippet" in summary.data_missing:
        summary.confidence = min(summary.confidence, 45)
    summary.confidence = max(0, min(100, int(summary.confidence)))

    _cache_set(cache_key, summary.model_dump())
    return summary


def generate_summary(api_key: str, model: str, market: dict, news: list[dict], timeout: float) -> str:
    sys_prompt = (
        "Sen MacroQuant Intelligence Analyst'sin. "
        "Turkce yaz. TSİ kullan. Yatirim tavsiyesi verme. "
        "Kisa ve temkinli bir piyasa ozeti uret."
    )

    payload = {
        "model": model,
        "input": [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": json.dumps({"market": market, "news": news})[:12000]},
        ],
    }

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    with httpx.Client(timeout=timeout) as client:
        res = client.post("https://api.openai.com/v1/responses", json=payload, headers=headers)
        res.raise_for_status()
        data = res.json()
        return data.get("output_text", "").strip()
