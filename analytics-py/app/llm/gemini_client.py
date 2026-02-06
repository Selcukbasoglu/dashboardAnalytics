from __future__ import annotations

import json
import os
import time
from typing import Any

import requests


MAX_PROMPT_PAYLOAD_CHARS = 12000
TOP_NEWS_MAX = 24
LOCAL_NEWS_MAX = 16
HOLDINGS_FULL_MAX = 24
RELATED_SYMBOL_MAX = 8
RELATED_PER_SYMBOL_MAX = 2
HEADLINE_TITLE_MAX = 180


def _extract_text(payload: dict) -> str:
    candidates = payload.get("candidates") or []
    if not candidates:
        return ""
    parts = (candidates[0].get("content") or {}).get("parts") or []
    return "".join([p.get("text") or "" for p in parts if isinstance(p, dict)]).strip()


def _clean_title(title: str | None) -> str:
    text = (title or "").strip()
    if len(text) > HEADLINE_TITLE_MAX:
        return text[: HEADLINE_TITLE_MAX - 3].rstrip() + "..."
    return text


def _compact_headlines(items: list[dict] | None, limit: int, prefix: str) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for item in items or []:
        title = _clean_title(item.get("title"))
        if not title:
            continue
        key = title.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "id": f"{prefix}{len(out)+1}",
                "title": title,
                "source": (item.get("source") or "")[:40],
                "publishedAtISO": item.get("publishedAtISO"),
            }
        )
        if len(out) >= limit:
            break
    return out


def _compact_related_news(related_news: dict[str, list[dict]] | None) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    if not related_news:
        return out
    symbols = sorted(related_news.keys())[:RELATED_SYMBOL_MAX]
    for symbol in symbols:
        rows = related_news.get(symbol) or []
        packed: list[dict] = []
        seen: set[str] = set()
        for row in rows:
            title = _clean_title(row.get("title"))
            if not title:
                continue
            key = title.lower()
            if key in seen:
                continue
            seen.add(key)
            packed.append(
                {
                    "title": title,
                    "direction": row.get("direction"),
                    "impactScore": row.get("impactScore"),
                    "low_signal": bool(row.get("low_signal")),
                }
            )
            if len(packed) >= RELATED_PER_SYMBOL_MAX:
                break
        if packed:
            out[symbol] = packed
    return out


def _compact_holdings(rows: list[dict] | None, limit: int) -> list[dict]:
    out: list[dict] = []
    for row in sorted(rows or [], key=lambda x: float(x.get("weight") or 0.0), reverse=True)[:limit]:
        out.append(
            {
                "symbol": row.get("symbol"),
                "weight": row.get("weight"),
                "asset_class": row.get("asset_class"),
                "sector": row.get("sector"),
                "is_precious_metal": row.get("is_precious_metal"),
                "is_crypto": row.get("is_crypto"),
                "currency": row.get("currency"),
            }
        )
    return out


def _prepare_payload(payload: dict[str, Any], budget_chars: int = MAX_PROMPT_PAYLOAD_CHARS) -> dict[str, Any]:
    top_news = _compact_headlines(payload.get("topNews"), TOP_NEWS_MAX, "T")
    local_news = _compact_headlines(payload.get("localHeadlines"), LOCAL_NEWS_MAX, "L")
    result: dict[str, Any] = {
        "asOfISO": payload.get("asOfISO"),
        "baseCurrency": payload.get("baseCurrency"),
        "newsHorizon": payload.get("newsHorizon"),
        "period": payload.get("period"),
        "coverage": payload.get("coverage") or {},
        "stats": payload.get("stats") or {},
        "riskFlags": list(payload.get("riskFlags") or [])[:10],
        "topNews": top_news,
        "localHeadlines": local_news,
        "relatedNews": _compact_related_news(payload.get("relatedNews") or {}),
        "holdings": _compact_holdings(payload.get("holdings"), 12),
        "holdings_full": _compact_holdings(payload.get("holdings_full"), HOLDINGS_FULL_MAX),
        "recommendations": payload.get("recommendations") or [],
        "modelIdeasRequest": {
            "required": True,
            "count": 3,
            "style": "model_hipotezi",
            "must_mark_as_assumption": True,
        },
    }

    while len(json.dumps(result, ensure_ascii=True)) > budget_chars:
        if len(result["relatedNews"]) > 2:
            # Drop least important symbols first to preserve top/local headline evidence.
            last = sorted(result["relatedNews"].keys())[-1]
            result["relatedNews"].pop(last, None)
            continue
        if len(result["holdings_full"]) > 10:
            result["holdings_full"] = result["holdings_full"][:-2]
            continue
        if len(result["localHeadlines"]) > 6:
            result["localHeadlines"] = result["localHeadlines"][:-1]
            continue
        if len(result["topNews"]) > 10:
            result["topNews"] = result["topNews"][:-1]
            continue
        break
    return result


def _build_prompt(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=True)
    return (
        "Asagidaki veri paketine dayanarak portfoy ve haber analizi yap. "
        "Yalnizca verilen haber basliklarini kanit olarak kullan; dis bilgi kullanma. "
        "Ayni anda modelin kendi fikirlerini de uret ama bunlari acikca varsayim olarak etiketle.\n\n"
        "ZORUNLU CIKTI BASLIKLARI:\n"
        "1) Haber Temelli Icgoruler\n"
        "2) Sektor Etkisi\n"
        "3) Portfoy Hisse Etkisi\n"
        "4) Portfoy Disi Pozitif Etkiler\n"
        "5) Portfoy Disi Negatif Etkiler\n"
        "6) Model Fikirleri (Varsayim)\n\n"
        "KURALLAR:\n"
        "- Haber Temelli Icgoruler maddelerinde [KANIT:Tx/Lx] etiketi zorunlu.\n"
        "- Model Fikirleri maddelerinde 'Model gorusu' ve olasilik (DUSUK/ORTA/YUKSEK) zorunlu.\n"
        "- Model Fikirleri bolumu kanitsiz olabilir ama varsayim oldugunu acik yaz.\n"
        "- Yatirim tavsiyesi verme, emir cagrisi yapma.\n"
        "- Turkce yaz, TSI kullan.\n\n"
        "VERI PAKETI:\n"
        f"{serialized}"
    )


def _extract_openrouter_text(payload: dict) -> str:
    choices = payload.get("choices") or []
    if not choices:
        return ""
    content = (choices[0].get("message") or {}).get("content")
    if isinstance(content, list):
        return "".join([p.get("text") or "" for p in content if isinstance(p, dict)]).strip()
    return (content or "").strip()


def _call_openrouter_fallback(prompt: str, timeout: float) -> tuple[str | None, str | None]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        return None, "missing_openrouter_key"
    model = os.getenv("OPENROUTER_PORTFOLIO_MODEL", "meta-llama/llama-3.3-70b-instruct:free")
    base = os.getenv("OPENROUTER_API_BASE", "https://openrouter.ai/api/v1")
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    referer = os.getenv("OPENROUTER_HTTP_REFERER")
    title = os.getenv("OPENROUTER_X_TITLE")
    if referer:
        headers["HTTP-Referer"] = referer
    if title:
        headers["X-Title"] = title

    payload = {
        "model": model,
        "temperature": 0.2,
        "max_tokens": 900,
        "messages": [{"role": "user", "content": prompt}],
    }
    url = f"{base}/chat/completions"
    try:
        res = requests.post(url, headers=headers, json=payload, timeout=timeout)
        if res.status_code >= 300:
            return None, f"openrouter_status:{res.status_code}:{(res.text or '')[:120]}"
        data = res.json() if res.content else {}
        text = _extract_openrouter_text(data or {})
        if not text:
            return None, "openrouter_empty_response"
        return text, None
    except Exception as exc:
        return None, type(exc).__name__


def _ensure_sections(text: str, payload: dict[str, Any]) -> str:
    out = (text or "").strip()
    if not out:
        out = "Haber Temelli Icgoruler\n- Yetersiz veri.\n"
    lowered = out.lower()
    if "haber temelli icgoruler" not in lowered:
        fallback = []
        for row in (payload.get("topNews") or [])[:3]:
            fallback.append(f"- [KANIT:{row.get('id')}] {row.get('title')} basligina dayali etki notu.")
        out = "Haber Temelli Icgoruler\n" + ("\n".join(fallback) if fallback else "- Yetersiz veri.") + "\n\n" + out
        lowered = out.lower()
    if "model fikirleri" not in lowered:
        top = (payload.get("topNews") or [])
        hint = top[0]["title"] if top else "makro akim"
        out = (
            out
            + "\n\nModel Fikirleri (Varsayim)\n"
            + f"- Model gorusu (ORTA): '{hint}' temasinin surmesi halinde risk algisi degisebilir. (varsayim)\n"
            + "- Model gorusu (DUSUK): Haber yogunlugu duserse fiyatlama hizi yavaslayabilir. (varsayim)\n"
        )
    return out.strip()


def generate_portfolio_summary(payload: dict[str, Any], timeout: float = 8.0) -> tuple[str | None, str | None]:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return None, "missing_key"
    model = (
        os.getenv("GEMINI_PORTFOLIO_MODEL")
        or os.getenv("GEMINI_MODEL")
        or os.getenv("GEMINI_MODEL_PRIMARY")
        or "gemini-2.5-flash"
    )
    base = os.getenv("GEMINI_API_BASE", "https://generativelanguage.googleapis.com/v1beta/models")
    system_text = "Turkce yaz. TSİ kullan. Yatirim tavsiyesi verme."

    prepared_payload = _prepare_payload(payload, budget_chars=MAX_PROMPT_PAYLOAD_CHARS)
    prompt = _build_prompt(prepared_payload)
    url = f"{base}/{model}:generateContent"

    def _post(target_url: str, use_system: bool) -> requests.Response:
        body = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.2, "maxOutputTokens": 900},
        }
        if use_system:
            body["systemInstruction"] = {"parts": [{"text": system_text}]}
        return requests.post(
            target_url,
            headers={"x-goog-api-key": api_key, "Content-Type": "application/json"},
            json=body,
            timeout=timeout,
        )

    use_system_instruction = True
    for attempt in range(2):
        try:
            res = _post(url, use_system_instruction)
            if res.status_code == 404 and "/v1beta/models" in url:
                url = url.replace("/v1beta/models", "/v1/models")
                use_system_instruction = False
                res = _post(url, use_system_instruction)
            if res.status_code == 400 and "systemInstruction" in (res.text or ""):
                use_system_instruction = False
                if attempt == 0:
                    continue
                res = _post(url, use_system_instruction)
            if res.status_code in (429, 503):
                if attempt == 0:
                    time.sleep(0.3)
                    continue
                text, err = _call_openrouter_fallback(prompt, timeout)
                if text:
                    return _ensure_sections(text, prepared_payload), "fallback_openrouter"
                return None, f"gemini_rate_limited:{err or (res.text or '')[:120]}"
            if res.status_code >= 300:
                return None, f"status:{res.status_code}:{(res.text or '')[:120]}"
            data = res.json() if res.content else {}
            text = _extract_text(data or {})
            if not text:
                return None, "empty_response"
            return _ensure_sections(text, prepared_payload), None
        except requests.Timeout:
            if attempt == 0:
                continue
            text, err = _call_openrouter_fallback(prompt, timeout)
            if text:
                return _ensure_sections(text, prepared_payload), "fallback_openrouter"
            return None, f"timeout:{err or 'gemini_timeout'}"
        except Exception as exc:
            return None, type(exc).__name__
    return None, "request_failed"
