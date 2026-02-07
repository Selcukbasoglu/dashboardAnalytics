from __future__ import annotations

import json
import os
import re
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
SUMMARY_HEADERS = [
    "Haber Temelli Icgoruler",
    "Sektor Etkisi",
    "Portfoy Hisse Etkisi",
    "Portfoy Disi Pozitif Etkiler",
    "Portfoy Disi Negatif Etkiler",
    "Model Fikirleri (Varsayim)",
]


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


def _compact_str_list(values: Any, limit: int, max_len: int = 32, upper: bool = False) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        text = str(raw or "").strip()
        if not text:
            continue
        if upper:
            text = text.upper()
        if len(text) > max_len:
            text = text[:max_len]
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= limit:
            break
    return out


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
                "tags": _compact_str_list(item.get("tags"), 6, max_len=36, upper=True),
                "portfolioSymbols": _compact_str_list(item.get("portfolioSymbols"), 4, max_len=12, upper=True),
                "portfolioSectors": _compact_str_list(item.get("portfolioSectors"), 3, max_len=24, upper=True),
                "relevanceHint": int(item.get("relevanceHint") or 0),
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


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _portfolio_symbols(payload: dict[str, Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for row in (payload.get("holdings") or []) + (payload.get("holdings_full") or []):
        sym = (row.get("symbol") or "").strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out


def _evidence_id_map(payload: dict[str, Any]) -> dict[str, str]:
    ids: dict[str, str] = {}
    for row in (payload.get("topNews") or []) + (payload.get("localHeadlines") or []):
        title = (row.get("title") or "").strip().lower()
        rid = (row.get("id") or "").strip()
        if title and rid:
            ids[title] = rid
    return ids


def _direction_text(direction: Any, impact: float) -> str:
    d = str(direction or "").lower()
    if d == "positive" or impact > 0.05:
        return "pozitif"
    if d == "negative" or impact < -0.05:
        return "negatif"
    return "notr"


def _probability_text(impact_abs: float) -> str:
    if impact_abs >= 0.25:
        return "YUKSEK"
    if impact_abs >= 0.10:
        return "ORTA"
    return "DUSUK"


def _related_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    evidence_ids = _evidence_id_map(payload)
    rows: list[dict[str, Any]] = []
    for symbol, items in (payload.get("relatedNews") or {}).items():
        for row in items or []:
            title = _clean_title(row.get("title"))
            if not title:
                continue
            impact = _safe_float(row.get("impactScore"))
            rows.append(
                {
                    "symbol": symbol,
                    "title": title,
                    "impact": impact,
                    "direction": _direction_text(row.get("direction"), impact),
                    "evidence_id": evidence_ids.get(title.lower()),
                }
            )
    rows.sort(key=lambda x: abs(x["impact"]), reverse=True)
    return rows


def _headline_sentiment(title: str) -> str:
    lower = title.lower()
    pos_terms = (
        "surge",
        "rally",
        "record",
        "beat",
        "inflow",
        "approval",
        "growth",
        "guidance raised",
        "up",
        "yuksel",
        "rekor",
        "onay",
        "pozitif",
        "guclu",
        "halka arz",
    )
    neg_terms = (
        "fall",
        "drop",
        "lawsuit",
        "ban",
        "risk",
        "cut",
        "decline",
        "outflow",
        "downgrade",
        "war",
        "tariff",
        "dus",
        "zayif",
        "yasak",
        "satis",
        "baski",
        "kayip",
    )
    if any(term in lower for term in pos_terms):
        return "positive"
    if any(term in lower for term in neg_terms):
        return "negative"
    return "neutral"


def _headline_priority(row: dict[str, Any]) -> float:
    score = _safe_float(row.get("relevanceHint"))
    tags = {str(t).upper() for t in (row.get("tags") or [])}
    if "PORTFOLIO_SYMBOL_MATCH" in tags:
        score += 4.0
    if "PORTFOLIO_SECTOR_MATCH" in tags:
        score += 2.0
    if "HALKA_ARZ_THEME" in tags:
        score += 1.0
    if "REGULATORY_THEME" in tags:
        score += 1.0
    score += len(row.get("portfolioSymbols") or []) * 0.8
    score += len(row.get("portfolioSectors") or []) * 0.4
    return score


def _iter_prioritized_headlines(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    rows.extend(payload.get("localHeadlines") or [])
    rows.extend(payload.get("topNews") or [])
    rows = [r for r in rows if (r.get("title") or "").strip()]
    rows.sort(key=_headline_priority, reverse=True)
    return rows


def _build_evidence_lines(payload: dict[str, Any], limit: int = 4) -> list[str]:
    lines: list[str] = []
    rows = _related_rows(payload)
    used: set[str] = set()
    for row in rows:
        if row["symbol"] in used:
            continue
        used.add(row["symbol"])
        eid = row["evidence_id"] or "T?"
        lines.append(
            f"- [KANIT:{eid}] {row['symbol']}: '{row['title']}' haberinde {row['direction']} etki sinyali "
            f"(etki skoru {row['impact']:+.2f})."
        )
        if len(lines) >= limit:
            break
    if lines:
        return lines

    for row in _iter_prioritized_headlines(payload)[: max(3, limit)]:
        rid = row.get("id") or "T?"
        title = row.get("title") or "baslik"
        symbols = [str(s).upper() for s in (row.get("portfolioSymbols") or []) if s]
        sectors = [str(s).upper() for s in (row.get("portfolioSectors") or []) if s]
        if symbols:
            lines.append(f"- [KANIT:{rid}] '{title}' haberi portfoy sembolleri ({', '.join(symbols[:3])}) ile iliskili.")
        elif sectors:
            lines.append(f"- [KANIT:{rid}] '{title}' haberi portfoy sektorleri ({', '.join(sectors[:2])}) icin sinyal uretiyor.")
        else:
            lines.append(f"- [KANIT:{rid}] '{title}' temasi portfoy risk istahi ve sektor dagilimi icin izlenmeli.")
        if len(lines) >= limit:
            break
    return lines or ["- [KANIT:T?] Haber akisinda anlamli sinyal yok."]


def _build_sector_lines(payload: dict[str, Any]) -> list[str]:
    weights: dict[str, float] = {}
    for row in payload.get("holdings_full") or payload.get("holdings") or []:
        sector = (row.get("sector") or row.get("asset_class") or "UNKNOWN").upper()
        weights[sector] = weights.get(sector, 0.0) + _safe_float(row.get("weight"))

    rel = _related_rows(payload)
    sector_imp: dict[str, float] = {}
    sym_sector = {
        (row.get("symbol") or "").upper(): (row.get("sector") or row.get("asset_class") or "UNKNOWN").upper()
        for row in payload.get("holdings_full") or payload.get("holdings") or []
    }
    for row in rel:
        sector = sym_sector.get((row.get("symbol") or "").upper(), "UNKNOWN")
        sector_imp[sector] = sector_imp.get(sector, 0.0) + _safe_float(row.get("impact"))
    if not rel:
        for row in payload.get("localHeadlines") or []:
            sectors = [str(s).upper() for s in (row.get("portfolioSectors") or []) if s]
            if not sectors:
                continue
            title = _clean_title(row.get("title"))
            sent = _headline_sentiment(title)
            base = max(0.05, min(0.25, _headline_priority(row) / 20.0))
            signed = base if sent == "positive" else (-base if sent == "negative" else 0.0)
            for sector in sectors:
                sector_imp[sector] = sector_imp.get(sector, 0.0) + signed

    out: list[str] = []
    for sector, weight in sorted(weights.items(), key=lambda x: x[1], reverse=True)[:3]:
        imp = sector_imp.get(sector, 0.0)
        out.append(
            f"- {sector}: portfoy agirligi %{weight * 100:.1f}; haber net etkisi {imp:+.2f}."
        )
    return out or ["- Sektor dagiliminda anlamli agirlik verisi yok."]


def _build_portfolio_lines(payload: dict[str, Any]) -> list[str]:
    totals: dict[str, float] = {}
    for row in _related_rows(payload):
        symbol = (row.get("symbol") or "").upper()
        totals[symbol] = totals.get(symbol, 0.0) + _safe_float(row.get("impact"))
    if not totals:
        for row in payload.get("localHeadlines") or []:
            symbols = [str(s).upper() for s in (row.get("portfolioSymbols") or []) if s]
            if not symbols:
                continue
            title = _clean_title(row.get("title"))
            sent = _headline_sentiment(title)
            base = max(0.05, min(0.25, _headline_priority(row) / 20.0))
            signed = base if sent == "positive" else (-base if sent == "negative" else 0.0)
            for sym in symbols:
                totals[sym] = totals.get(sym, 0.0) + signed
    out: list[str] = []
    for sym, score in sorted(totals.items(), key=lambda x: abs(x[1]), reverse=True)[:4]:
        out.append(
            f"- {sym}: kisa vade beklenti {_direction_text(None, score)} "
            f"(olasilik {_probability_text(abs(score))}, net etki {score:+.2f})."
        )
    return out or ["- Portfoy hisseleri icin haber-temelli net sinyal zayif."]


def _build_external_lines(payload: dict[str, Any], positive: bool) -> list[str]:
    out: list[str] = []
    target = "positive" if positive else "negative"
    for row in (payload.get("topNews") or [])[:8]:
        title = _clean_title(row.get("title"))
        if not title:
            continue
        if _headline_sentiment(title) != target:
            continue
        rid = row.get("id") or "T?"
        out.append(f"- [KANIT:{rid}] {title}")
        if len(out) >= 2:
            break
    if out:
        return out
    if positive:
        return ["- Portfoy disinda guclu pozitif tema sinyali sinirli."]
    return ["- Portfoy disinda belirgin negatif tema sinyali sinirli."]


def _build_model_idea_lines(payload: dict[str, Any]) -> list[str]:
    ideas: list[str] = []
    risk_flags = [str(x) for x in (payload.get("riskFlags") or [])]
    coverage_ratio = _safe_float((payload.get("stats") or {}).get("coverage_ratio"))
    top = payload.get("topNews") or []
    theme = _clean_title(top[0].get("title")) if top else "makro akim"

    if any("FX_RISK" in f for f in risk_flags):
        ideas.append("- Model gorusu (ORTA): USD kaynakli oynaklik devam ederse TRY bazli portfoyde dalga boyu artabilir. (varsayim)")
    if coverage_ratio < 0.40:
        ideas.append("- Model gorusu (DUSUK): Haber kapsami dusuk kaldigi icin sinyal guvenirligi zayif kalabilir. (varsayim)")
    ideas.append(
        f"- Model gorusu (ORTA): '{theme}' temasinin surmesi durumunda benzer sektorlerde momentum korunabilir. (varsayim)"
    )
    if len(ideas) < 3:
        ideas.append("- Model gorusu (DUSUK): Volatilite azalirsa fiyatlama hizi yavaslayabilir. (varsayim)")
    return ideas[:3]


def _build_rule_based_summary(payload: dict[str, Any]) -> str:
    sections = [
        ("Haber Temelli Icgoruler", _build_evidence_lines(payload, limit=4)),
        ("Sektor Etkisi", _build_sector_lines(payload)),
        ("Portfoy Hisse Etkisi", _build_portfolio_lines(payload)),
        ("Portfoy Disi Pozitif Etkiler", _build_external_lines(payload, positive=True)),
        ("Portfoy Disi Negatif Etkiler", _build_external_lines(payload, positive=False)),
        ("Model Fikirleri (Varsayim)", _build_model_idea_lines(payload)),
    ]
    blocks = [f"{title}\n" + "\n".join(lines) for title, lines in sections]
    return "\n\n".join(blocks).strip()


def _is_low_quality_summary(text: str, payload: dict[str, Any]) -> bool:
    out = (text or "").strip()
    if not out or len(out) < 220:
        return True
    lower = out.lower()
    if "basligina dayali etki notu" in lower:
        return True
    if lower.count("yetersiz veri") >= 2:
        return True

    evidence_count = len(re.findall(r"\[kanit:[tl]\d+\]", lower))
    if evidence_count < 2:
        return True

    symbols = _portfolio_symbols(payload)
    symbol_hits = 0
    for sym in symbols[:12]:
        if re.search(rf"\b{re.escape(sym)}\b", out, flags=re.IGNORECASE):
            symbol_hits += 1
    if symbol_hits < 1:
        return True

    bullet_count = len([ln for ln in out.splitlines() if ln.strip().startswith(("-", "*", "•"))])
    if bullet_count < 4:
        return True
    return False


def _build_prompt(payload: dict[str, Any], strict: bool = False) -> str:
    serialized = json.dumps(payload, ensure_ascii=True)
    strict_rules = ""
    if strict:
        strict_rules = (
            "\nEK SIKILIK:\n"
            "- Her maddede varlik/tema + etki + neden yaz.\n"
            "- Bos/sablon ifade kullanma.\n"
            "- Portfoy Hisse Etkisi bolumunde en az 2 sembol gecsin.\n"
            "- Cikti yalnizca 6 baslik ve madde listeleri olsun.\n"
        )
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
        "- localHeadlines altindaki tags/portfolioSymbols/portfolioSectors alanlarini onceliklendir.\n"
        "- Model Fikirleri maddelerinde 'Model gorusu' ve olasilik (DUSUK/ORTA/YUKSEK) zorunlu.\n"
        "- Model Fikirleri bolumu kanitsiz olabilir ama varsayim oldugunu acik yaz.\n"
        "- Yatirim tavsiyesi verme, emir cagrisi yapma.\n"
        "- Turkce yaz, TSI kullan.\n"
        f"{strict_rules}\n"
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
        out = _build_rule_based_summary(payload)
    lowered = out.lower()
    if "haber temelli icgoruler" not in lowered:
        fallback = _build_evidence_lines(payload, limit=3)
        out = "Haber Temelli Icgoruler\n" + "\n".join(fallback) + "\n\n" + out
        lowered = out.lower()
    if "sektor etkisi" not in lowered:
        out += "\n\nSektor Etkisi\n" + "\n".join(_build_sector_lines(payload))
        lowered = out.lower()
    if "portfoy hisse etkisi" not in lowered:
        out += "\n\nPortfoy Hisse Etkisi\n" + "\n".join(_build_portfolio_lines(payload))
        lowered = out.lower()
    if "model fikirleri" not in lowered:
        out = out + "\n\nModel Fikirleri (Varsayim)\n" + "\n".join(_build_model_idea_lines(payload))
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

    def _run_request(prompt: str) -> tuple[str | None, str | None]:
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

    last_err: str | None = None
    for strict in (False, True):
        prompt = _build_prompt(prepared_payload, strict=strict)
        text, err = _run_request(prompt)
        if text:
            if _is_low_quality_summary(text, prepared_payload):
                last_err = f"low_quality_{'strict' if strict else 'base'}"
                continue
            return text, err
        if err:
            last_err = err

    return _build_rule_based_summary(prepared_payload), f"fallback_rule_based:{last_err or 'no_quality'}"
