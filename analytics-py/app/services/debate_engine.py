from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any

from app.infra.cache import cache_key, now_iso
from app.models import NewsItem
from app.services.portfolio_engine import build_portfolio, PortfolioSettings
from app.llm.debate_providers import call_openrouter, call_openrouter_openai, call_openrouter_referee, get_openrouter_debug
from app.engine.news_engine import SECTOR_ROLLING_EVENTS
from app.providers.yahoo import _fetch_chart


_INFLIGHT_LOCK = threading.Lock()
_INFLIGHT: dict[str, dict[str, Any]] = {}


def _format_tsi(dt: datetime | None = None) -> str:
    months = {
        1: "Ocak",
        2: "Şubat",
        3: "Mart",
        4: "Nisan",
        5: "Mayıs",
        6: "Haziran",
        7: "Temmuz",
        8: "Ağustos",
        9: "Eylül",
        10: "Ekim",
        11: "Kasım",
        12: "Aralık",
    }
    dt = dt or datetime.now(timezone.utc)
    ts = dt.astimezone(timezone(timedelta(hours=3)))
    return f"{ts.day} {months.get(ts.month, ts.month)} {ts.year}, {ts.strftime('%H:%M')} TSİ"


def _normalize_title(title: str | None) -> str:
    return (title or "").strip().lower()


def make_evidence_id(prefix: str, url: str | None, published: str | None, title: str | None) -> str:
    key = url or _normalize_title(title)
    stamp = published or ""
    digest = hashlib.sha256(f"{key}|{stamp}".encode("utf-8")).hexdigest()[:16]
    return f"{prefix}:{digest}"


def _build_global_news_summary(cache, window: str) -> dict:
    items = []
    cached = None
    for key in (cache_key("news", window, "all"), cache_key("news", window, "")):
        if not key:
            continue
        try:
            cached = cache.get(key) if cache else None
        except Exception:
            cached = None
        if cached:
            break
    if cached:
        items = [NewsItem(**n) for n in cached]
    category_counts: dict[str, int] = {}
    event_type_counts: dict[str, int] = {}
    channel_counts: dict[str, int] = {}
    evidence_ids: list[str] = []
    for it in items[:60]:
        if it.category:
            category_counts[it.category] = category_counts.get(it.category, 0) + 1
        if it.event_type:
            event_type_counts[it.event_type] = event_type_counts.get(it.event_type, 0) + 1
        for ch in it.impact_channel or []:
            channel_counts[ch] = channel_counts.get(ch, 0) + 1
        evidence_ids.append(make_evidence_id("gn", it.url, it.publishedAtISO, it.title))
    top_event_types = sorted(event_type_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    top_channels = sorted(channel_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    return {
        "category_counts": category_counts,
        "top_event_types": top_event_types,
        "top_impact_channels": top_channels,
        "top_evidence_ids": evidence_ids[:10],
        "items": items,
    }


def _sector_rotation_snapshot() -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    scores: dict[str, dict[str, Any]] = {}
    for ts, sector, score in list(SECTOR_ROLLING_EVENTS):
        if ts < cutoff:
            continue
        row = scores.setdefault(sector, {"rotationScore": 0, "unique_clusters": 0})
        row["rotationScore"] += score
        row["unique_clusters"] += 1
    ranked = sorted(scores.items(), key=lambda x: x[1]["rotationScore"], reverse=True)[:6]
    out = []
    for sector, vals in ranked:
        out.append(
            {
                "sector": sector,
                "rotationScore": vals["rotationScore"],
                "unique_clusters": vals["unique_clusters"],
                "top_evidence_ids": [],
                "rule_breakdown": {"required_hits_sum": 0, "boost_hits_sum": 0, "exclude_hits_sum": 0},
            }
        )
    return {"sectors": out, "portfolioExposure": {}}


def _price_change_from_chart(symbol: str, window: str) -> dict | None:
    try:
        range_map = {"24h": "7d", "7d": "7d", "30d": "30d"}
        chart = _fetch_chart(symbol, range_map.get(window, "7d"), "1d", 6.0)
        if not chart:
            return None
        df = chart.get("df")
        if df is None or df.empty or len(df) < 2:
            return None
        closes = df["Close"].dropna().tolist()
        if len(closes) < 2:
            return None
        last = float(closes[-1])
        prev = float(closes[-2])
        ret_1d = (last - prev) / prev if prev else 0.0
        first = float(closes[0])
        if len(closes) >= 8:
            base_7d = float(closes[-8])
            ret_7d = (last - base_7d) / base_7d if base_7d else 0.0
        else:
            ret_7d = (last - first) / first if first else 0.0
        if len(closes) >= 31:
            base_30d = float(closes[-31])
            ret_30d = (last - base_30d) / base_30d if base_30d else 0.0
        else:
            ret_30d = (last - first) / first if first else 0.0
        ret_window = ret_1d if window == "24h" else ret_7d if window == "7d" else ret_30d
        return {"ret_1d": ret_1d, "ret_7d": ret_7d, "ret_30d": ret_30d, "ret_window": ret_window}
    except Exception:
        return None


def _build_watchlist_changes(holdings: list[dict], window: str) -> dict:
    movers = []
    for h in holdings[:15]:
        sym = h.get("yahoo_symbol") or h.get("symbol")
        if not sym:
            continue
        changes = _price_change_from_chart(sym, window)
        if not changes:
            continue
        movers.append(
            {
                "symbol": h.get("symbol"),
                "ret_1d": changes["ret_1d"],
                "ret_7d": changes["ret_7d"],
                "ret_30d": changes["ret_30d"],
                "ret_window": changes["ret_window"],
                "vol_30d": h.get("vol_30d"),
            }
        )
    movers.sort(key=lambda x: abs(x.get("ret_1d") or 0.0), reverse=True)
    return {"window": window, "top_movers": movers[:10]}


def _build_evidence_index(portfolio_items: list[dict], global_items: list[NewsItem]) -> tuple[dict, dict]:
    evidence: dict[str, dict] = {}
    pointers_by_symbol: dict[str, dict] = {}
    pointers_by_sector: dict[str, dict] = {}

    def _add_entry(eid: str, item: dict, prefix: str):
        if eid in evidence:
            return
        evidence[eid] = {
            "publishedAtISO": item.get("publishedAtISO"),
            "title": (item.get("title") or "")[:120],
            "matchedSymbols": item.get("matchedSymbols") or [],
            "matchedSectors": item.get("matchedSectors") or [],
            "event_type": item.get("event_type") or "UNKNOWN",
            "impact_channel": item.get("impact_channel") or [],
            "impactScore": item.get("impactScore") or 0.0,
            "directness": "direct" if prefix == "pn" else "indirect",
        }

    for item in portfolio_items[:60]:
        eid = make_evidence_id("pn", item.get("url"), item.get("publishedAtISO"), item.get("title"))
        _add_entry(eid, item, "pn")

    for item in global_items[:60]:
        data = item.model_dump()
        data["matchedSymbols"] = item.entities or []
        data["matchedSectors"] = []
        data["impactScore"] = item.impact_potential or 0
        eid = make_evidence_id("gn", item.url, item.publishedAtISO, item.title)
        _add_entry(eid, data, "gn")

    for eid, entry in evidence.items():
        impact = entry.get("impactScore") or 0.0
        for sym in entry.get("matchedSymbols") or []:
            bucket = pointers_by_symbol.setdefault(sym, {"pos": [], "neg": [], "channels": {}, "types": {}})
            (bucket["pos"] if impact >= 0 else bucket["neg"]).append((eid, impact))
            for ch in entry.get("impact_channel") or []:
                bucket["channels"][ch] = bucket["channels"].get(ch, 0) + 1
            et = entry.get("event_type")
            bucket["types"][et] = bucket["types"].get(et, 0) + 1
        for sec in entry.get("matchedSectors") or []:
            bucket = pointers_by_sector.setdefault(sec, {"pos": [], "neg": [], "channels": {}, "types": {}})
            (bucket["pos"] if impact >= 0 else bucket["neg"]).append((eid, impact))
            for ch in entry.get("impact_channel") or []:
                bucket["channels"][ch] = bucket["channels"].get(ch, 0) + 1
            et = entry.get("event_type")
            bucket["types"][et] = bucket["types"].get(et, 0) + 1

    def _trim_pointer(bucket: dict) -> dict:
        pos = sorted(bucket.get("pos", []), key=lambda x: x[1], reverse=True)[:3]
        neg = sorted(bucket.get("neg", []), key=lambda x: x[1])[:3]
        channels = sorted(bucket.get("channels", {}).items(), key=lambda x: x[1], reverse=True)[:3]
        types = sorted(bucket.get("types", {}).items(), key=lambda x: x[1], reverse=True)[:3]
        return {
            "top_positive_ids": [p[0] for p in pos],
            "top_negative_ids": [n[0] for n in neg],
            "top_channels": [c[0] for c in channels],
            "top_event_types": [t[0] for t in types],
        }

    pointers = {
        "bySymbol": {k: _trim_pointer(v) for k, v in pointers_by_symbol.items()},
        "bySector": {k: _trim_pointer(v) for k, v in pointers_by_sector.items()},
    }
    return evidence, pointers


def build_context(pipeline, base: str, window: str, horizon: str) -> tuple[dict, str, str]:
    portfolio = build_portfolio(pipeline, base_currency=base, news_horizon=window)
    global_summary = _build_global_news_summary(pipeline.cache if pipeline else None, window)
    max_holdings = int(os.getenv("PORTFOLIO_DEBATE_MAX_HOLDINGS", "15") or 15)
    max_evidence = int(os.getenv("PORTFOLIO_DEBATE_MAX_EVIDENCE", "60") or 60)

    settings = PortfolioSettings()
    constraints = {
        "turnover_cap": getattr(settings, f"turnover_{horizon}", settings.turnover_daily),
        "max_weight": settings.max_weight,
        "crypto_max": settings.max_crypto_weight,
        "version": "v1",
        "asOfTSI": _format_tsi(),
    }

    holdings = portfolio.get("holdings") or []
    top_holdings = sorted(holdings, key=lambda h: h.get("weight", 0), reverse=True)[: max_holdings or 15]

    portfolio_news = portfolio.get("newsImpact", {}).get("items", []) or []
    evidence_index, evidence_pointers = _build_evidence_index(portfolio_news, global_summary.get("items") or [])

    impact_by_symbol = []
    summary = portfolio.get("newsImpact", {}).get("summary", {}) or {}
    impact_by_symbol_map = summary.get("impact_by_symbol") or {}
    impact_by_symbol_direct = summary.get("impact_by_symbol_direct") or {}
    impact_by_symbol_indirect = summary.get("impact_by_symbol_indirect") or {}
    for sym, impact in impact_by_symbol_map.items():
        pointers = evidence_pointers.get("bySymbol", {}).get(sym, {})
        impact_by_symbol.append(
            {
                "symbol": sym,
                "impact": impact,
                "evidence_ids": (pointers.get("top_positive_ids") or [])[:2]
                + (pointers.get("top_negative_ids") or [])[:1],
            }
        )
    low_signal_ratio = float(summary.get("low_signal_ratio") or 0.0)
    coverage_ratio = float(summary.get("coverage_ratio") or 0.0)

    sector_rotation = _sector_rotation_snapshot()
    portfolio_exposure = {}
    for h in holdings:
        sector = h.get("sector")
        if sector:
            portfolio_exposure.setdefault(sector, []).append(h.get("symbol"))
    sector_rotation["portfolioExposure"] = portfolio_exposure

    engine_signals = {"trim": [], "sectorFocus": []}
    recs = portfolio.get("recommendations", [])
    selected = next((r for r in recs if r.get("period") == horizon), None)
    optimizer_hold = {"mode": "ACTIVE", "reason": None}
    if selected:
        optimizer_hold["mode"] = selected.get("mode") or "ACTIVE"
        optimizer_hold["reason"] = selected.get("hold_reason")
        for act in (selected.get("actions") or [])[:5]:
            sym = act.get("symbol")
            pointers = evidence_pointers.get("bySymbol", {}).get(sym, {})
            ids = (pointers.get("top_negative_ids") or [])[:2] or (pointers.get("top_positive_ids") or [])[:1]
            engine_signals["trim"].append(
                {
                    "symbol": sym,
                    "deltaWeight": act.get("deltaWeight"),
                    "why": act.get("reason") or [],
                    "confidence_engine": act.get("confidence"),
                    "evidence_ids": ids,
                    "score_breakdown": act.get("score_breakdown") or {},
                }
            )

    for sector in (sector_rotation.get("sectors") or [])[:3]:
        sec = sector.get("sector")
        pointers = evidence_pointers.get("bySector", {}).get(sec, {})
        ids = (pointers.get("top_positive_ids") or [])[:2] or (pointers.get("top_negative_ids") or [])[:1]
        engine_signals["sectorFocus"].append(
            {
                "sector": sec,
                "why": ["rotation_score"],
                "confidence_engine": 60,
                "evidence_ids": ids,
            }
        )

    context = {
        "meta": {
            "base": base,
            "window": window,
            "horizon": horizon,
            "generatedAtTSI": _format_tsi(),
        },
        "constraintsSnapshot": constraints,
        "portfolioSnapshot": {
            "topHoldings": [
                {
                    "symbol": h.get("symbol"),
                    "asset_class": h.get("asset_class"),
                    "sector": h.get("sector"),
                    "weight": h.get("weight"),
                    "mkt_value_base": h.get("mkt_value_base"),
                    "vol_30d": h.get("vol_30d"),
                }
                for h in top_holdings
            ],
            "allocation": portfolio.get("allocation") or {},
            "risk": portfolio.get("risk") or {},
        },
        "globalNewsSummary": {
            "category_counts": global_summary.get("category_counts") or {},
            "top_event_types": global_summary.get("top_event_types") or [],
            "top_impact_channels": global_summary.get("top_impact_channels") or [],
            "top_evidence_ids": global_summary.get("top_evidence_ids") or [],
        },
        "portfolioNewsSummary": {
            "coverage": dict((portfolio.get("newsImpact") or {}).get("coverage") or {}),
            "impact_by_symbol": impact_by_symbol,
            "impact_by_symbol_direct": sorted(
                [
                    {"symbol": k, "impact": v}
                    for k, v in impact_by_symbol_direct.items()
                ],
                key=lambda x: abs(x.get("impact") or 0.0),
                reverse=True,
            )[:10],
            "impact_by_symbol_indirect": sorted(
                [
                    {"symbol": k, "impact": v}
                    for k, v in impact_by_symbol_indirect.items()
                ],
                key=lambda x: abs(x.get("impact") or 0.0),
                reverse=True,
            )[:10],
            "low_signal_ratio": low_signal_ratio,
            "coverage_ratio": coverage_ratio,
        },
        "sectorRotation": sector_rotation,
        "watchlistPriceChanges": _build_watchlist_changes(holdings, window),
        "engineSignals": engine_signals,
        "optimizerHold": optimizer_hold,
        "evidenceIndex": dict(list(evidence_index.items())[: max_evidence or 60]),
        "evidencePointers": evidence_pointers,
        "missingData": [],
    }

    context_str = json.dumps(context, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    context_hash = hashlib.sha256(context_str.encode("utf-8")).hexdigest()[:16]
    context["context_hash"] = context_hash
    return context, context_str, context_hash


def _build_prompt(context_str: str) -> str:
    return (
        "Turkce yaz. TSİ kullan. Yatırım tavsiyesi verme.\n"
        "SADECE verilen evidenceIndex'e dayan.\n"
        "Her trimSignals ve sectorFocus maddesi en az 1 evidence_id içermeli.\n"
        "evidenceIndex dışında kanıt kullanma.\n"
        "ÇIKTI SADECE strict JSON (markdown yok).\n"
        "Görev: Aşağıdaki context JSON'a dayanarak portföy için kısa vadeli öngörü ve optimizasyon sinyali üret.\n"
        "Kısıtlar: constraintsSnapshot’a uy.\n"
        "Çıktı şeması:\n"
        "{\n"
        '  "generatedAtTSI":"...",\n'
        '  "dataStatus":"ok|partial",\n'
        '  "executiveSummary":[max 5],\n'
        '  "portfolioMode":"risk_on|risk_off|mixed",\n'
        '  "trimSignals":[max 3, each includes evidence_ids 1-3],\n'
        '  "sectorFocus":[max 3, each includes evidence_ids 1-3],\n'
        '  "watchMetrics":[max 5],\n'
        '  "scenarios":{"base":[max 3 bullets],"risk":[max 3 bullets]},\n'
        '  "note":"Sinyal; yatırım tavsiyesi değildir",\n'
        '  "missingData":[...]\n'
        "}\n"
        "CONTEXT_JSON:\n"
        f"{context_str}"
    )


def _score_output(output: dict, context: dict) -> int:
    if not output:
        return 0
    constraints = context.get("constraintsSnapshot") or {}
    cap = constraints.get("turnover_cap", 0.0) or 0.0
    evidence_index = context.get("evidenceIndex") or {}
    optimizer_hold = context.get("optimizerHold") or {}
    hold_mode = optimizer_hold.get("mode") == "HOLD"
    score = 0

    # Constraint compliance (40)
    turnover = 0.0
    valid = True
    for item in output.get("trimSignals", []) or []:
        delta = abs(float(item.get("deltaWeight") or 0.0))
        turnover += delta
        ids = item.get("evidence_ids") or []
        if not ids or any(i not in evidence_index for i in ids):
            valid = False
    if turnover > cap + 1e-6:
        valid = False
    score += 40 if valid else 5
    if hold_mode and (output.get("trimSignals") or []):
        score -= 20

    # Evidence consistency (30)
    correct = 0
    total = 0
    for item in output.get("trimSignals", []) or []:
        ids = item.get("evidence_ids") or []
        impacts = [float(evidence_index.get(i, {}).get("impactScore") or 0.0) for i in ids if i in evidence_index]
        if not impacts:
            continue
        total += 1
        if sum(impacts) <= 0:
            correct += 1
    for item in output.get("sectorFocus", []) or []:
        ids = item.get("evidence_ids") or []
        impacts = [float(evidence_index.get(i, {}).get("impactScore") or 0.0) for i in ids if i in evidence_index]
        if not impacts:
            continue
        total += 1
        if sum(impacts) >= 0:
            correct += 1
    score += int(30 * (correct / total)) if total else 10

    # Coverage of pointers (20)
    pointers = context.get("evidencePointers") or {}
    top_ids = set()
    for bucket in (pointers.get("bySymbol") or {}).values():
        top_ids.update(bucket.get("top_positive_ids") or [])
        top_ids.update(bucket.get("top_negative_ids") or [])
    used = set()
    for item in output.get("trimSignals", []) or []:
        used.update(item.get("evidence_ids") or [])
    overlap = len(used & top_ids) / max(1, len(used))
    score += int(20 * overlap)

    # Turnover preference (10)
    if cap > 0:
        score += int(10 * max(0.0, 1 - (turnover / cap)))
    else:
        score += 5
    return score


def _map_error_code(err: str | None) -> str | None:
    if not err:
        return None
    if "missing" in err:
        return "missing_key"
    if "policy" in err:
        return "policy"
    if "rate" in err or "quota" in err:
        return "quota"
    if "schema" in err or "parse" in err:
        return "parse"
    if "timeout" in err or "ReadTimeout" in err:
        return "timeout"
    return "unknown"


def _consensus(openrouter: dict | None, openai: dict | None) -> dict:
    if not openrouter or not openai:
        return {}
    g_trim = {(t.get("symbol"), t.get("action")) for t in openrouter.get("trimSignals", []) or []}
    c_trim = {(t.get("symbol"), t.get("action")) for t in openai.get("trimSignals", []) or []}
    common = g_trim & c_trim
    return {"trimSignals": list(common)}


def _referee_context(context: dict, openrouter_data: dict, openai_data: dict, base: str, window: str, horizon: str) -> dict:
    constraints = context.get("constraintsSnapshot") or {}
    global_summary = context.get("globalNewsSummary") or {}
    portfolio_summary = context.get("portfolioNewsSummary") or {}
    risk = (context.get("portfolioSnapshot") or {}).get("risk") or {}
    return {
        "context_hash": context.get("context_hash") or "",
        "request": {
            "base": base,
            "window": window,
            "horizon": horizon,
            "generatedAtTSI": _format_tsi(),
        },
        "constraintsSnapshot": constraints,
        "newsContentProfile": {
            "top_event_types": global_summary.get("top_event_types") or [],
            "top_impact_channels": global_summary.get("top_impact_channels") or [],
            "top_evidence_ids": global_summary.get("top_evidence_ids") or [],
            "coverage": portfolio_summary.get("coverage") or {},
            "low_signal_ratio": portfolio_summary.get("low_signal_ratio") or 0.0,
            "evidencePointers": context.get("evidencePointers") or {},
        },
        "portfolioRisk": {
            "hhi": risk.get("hhi"),
            "vol_30d": risk.get("vol_30d"),
            "fx_risk": "fx_risk_up" in (risk.get("flags") or []),
        },
        "provider_a": {"name": "openrouter", "plan": openrouter_data},
        "provider_b": {"name": "openai", "plan": openai_data},
    }


def run_debate(pipeline, base: str, window: str, horizon: str, force: bool = False) -> dict:
    ttl = int(os.getenv("PORTFOLIO_DEBATE_TTL_SECONDS", "21600"))
    cooldown = int(os.getenv("PORTFOLIO_DEBATE_COOLDOWN_SECONDS", "600"))
    provider_timeout_ms = int(os.getenv("PORTFOLIO_DEBATE_PROVIDER_TIMEOUT_MS", "8000"))
    total_timeout_ms = int(os.getenv("PORTFOLIO_DEBATE_TOTAL_TIMEOUT_MS", "10000"))
    timeout = float(os.getenv("PORTFOLIO_DEBATE_TIMEOUT_SECONDS", str(total_timeout_ms / 1000)))  # legacy fallback
    provider_timeout = max(1.0, provider_timeout_ms / 1000.0)
    total_timeout = max(timeout, total_timeout_ms / 1000.0)
    singleflight_enabled = (os.getenv("PORTFOLIO_DEBATE_SINGLEFLIGHT_ENABLED", "true").lower() in ("1", "true", "yes", "on"))
    fast_fail = os.getenv("PORTFOLIO_DEBATE_FAST_FAIL_ON_MISS", "true").lower() in ("1", "true", "yes", "on")

    last_key = cache_key("debate_last", base, window, horizon)

    cache = pipeline.cache if pipeline else None
    last_cached = cache.get(last_key) if cache else None
    if last_cached and not force:
        last_cached["cache"] = {"hit": True, "ttl_seconds": ttl, "cooldown_remaining_seconds": 0}
        return last_cached
    if fast_fail and not force:
        return {
            "generatedAtTSI": _format_tsi(),
            "dataStatus": "partial",
            "cache": {"hit": False, "ttl_seconds": ttl, "cooldown_remaining_seconds": 0},
            "providers": {"openrouter": "skipped", "openai": "skipped"},
            "winner": "single",
            "consensus": {},
            "disagreements": {},
            "raw": {},
            "debug_notes": ["cache_miss_fast_fail"],
        }

    context, context_str, context_hash = build_context(pipeline, base, window, horizon)
    key = cache_key("debate", base, window, horizon, context_hash)
    ts_key = cache_key("debate_ts", base, window, horizon, context_hash)
    cached = cache.get(key) if cache else None
    if cached and not force:
        cached["cache"] = {"hit": True, "ttl_seconds": ttl, "cooldown_remaining_seconds": 0}
        return cached

    now = time.time()
    last_ts = cache.get(ts_key) if cache else None
    if last_ts and not force:
        elapsed = now - float(last_ts)
        if elapsed < cooldown:
            remaining = int(cooldown - elapsed)
            if cached:
                cached["cache"] = {"hit": True, "ttl_seconds": ttl, "cooldown_remaining_seconds": remaining}
                return cached
            return {
                "generatedAtTSI": _format_tsi(),
                "dataStatus": "partial",
                "cache": {"hit": False, "ttl_seconds": ttl, "cooldown_remaining_seconds": remaining},
                "providers": {"openrouter": "skipped", "openai": "skipped"},
                "winner": "single",
                "consensus": {},
                "disagreements": {},
                "raw": {},
                "debug_notes": ["cooldown_active_no_cache"],
            }

    inflight_key = key
    if singleflight_enabled:
        with _INFLIGHT_LOCK:
            if inflight_key in _INFLIGHT:
                evt = _INFLIGHT[inflight_key]["event"]
            else:
                evt = threading.Event()
                _INFLIGHT[inflight_key] = {"event": evt}
        if evt.is_set() and cached:
            return cached
        if evt.is_set() and not cached:
            return {
                "generatedAtTSI": _format_tsi(),
                "dataStatus": "partial",
                "cache": {"hit": False, "ttl_seconds": ttl, "cooldown_remaining_seconds": 0},
                "providers": {"openrouter": "skipped", "openai": "skipped"},
                "winner": "single",
                "consensus": {},
                "disagreements": {},
                "raw": {},
                "debug_notes": ["inflight_done_no_cache"],
            }
        if inflight_key in _INFLIGHT and _INFLIGHT[inflight_key].get("owner"):
            evt.wait(timeout=total_timeout + 2)
            cached = cache.get(key) if cache else None
            if cached:
                cached["cache"] = {"hit": True, "ttl_seconds": ttl, "cooldown_remaining_seconds": 0}
                return cached
            return {
                "generatedAtTSI": _format_tsi(),
                "dataStatus": "partial",
                "cache": {"hit": False, "ttl_seconds": ttl, "cooldown_remaining_seconds": 0},
                "providers": {"openrouter": "skipped", "openai": "skipped"},
                "winner": "single",
                "consensus": {},
                "disagreements": {},
                "raw": {},
                "debug_notes": ["inflight_timeout"],
            }
        _INFLIGHT[inflight_key]["owner"] = True
    prompt = _build_prompt(context_str)
    providers = {"openrouter": "skipped", "openai": "skipped"}
    raw = {}
    notes = [f"context_hash={context_hash}"]

    openrouter_status, openrouter_data, openrouter_err = call_openrouter(prompt, provider_timeout, force=force)
    providers["openrouter"] = openrouter_status
    if openrouter_err:
        notes.append(f"openrouter_error={openrouter_err}")
    notes.append("openrouter_provider=openrouter")
    notes.append(f"openrouter_model={os.getenv('OPENROUTER_MODEL_PRIMARY', '')}")
    dbg = get_openrouter_debug()
    if dbg:
        notes.append(f"openrouter_free_daily_count={dbg.get('free_daily_count', 0)}")
        notes.append(f"openrouter_free_minute_count={dbg.get('free_minute_count', 0)}")
        keyinfo = dbg.get("keyinfo_cached")
        if isinstance(keyinfo, dict):
            is_free = None
            if "data" in keyinfo and isinstance(keyinfo.get("data"), dict):
                is_free = keyinfo["data"].get("is_free_tier")
            if is_free is None:
                is_free = keyinfo.get("is_free_tier")
            if is_free is not None:
                notes.append(f"openrouter_key_is_free_tier={is_free}")
    if openrouter_data:
        raw["openrouter"] = openrouter_data

    openai_status, openai_data, openai_err = call_openrouter_openai(prompt, provider_timeout, force=force)
    providers["openai"] = openai_status
    if openai_err:
        notes.append(f"openai_error={openai_err}")
    notes.append(f"openai_model={os.getenv('OPENROUTER_MODEL_SECONDARY', '')}")
    if openai_data:
        if isinstance(openai_data, dict) and openai_data.get("_schema_repaired"):
            notes.append("openai_schema_repaired=true")
            openai_data.pop("_schema_repaired", None)
        raw["openai"] = openai_data

    openrouter_score = _score_output(openrouter_data or {}, context) if openrouter_data else 0
    openai_score = _score_output(openai_data or {}, context) if openai_data else 0
    disagreement_score = abs(openrouter_score - openai_score) / max(1, max(openrouter_score, openai_score, 1))
    notes.append(f"disagreement_score={disagreement_score:.3f}")
    disagreement_score = abs(openrouter_score - openai_score) / max(1, max(openrouter_score, openai_score, 1))
    notes.append(f"disagreement_score={disagreement_score:.3f}")
    if openrouter_data and openai_data:
        if abs(openrouter_score - openai_score) < 5:
            winner = "tie"
        else:
            winner = "openrouter" if openrouter_score > openai_score else "openai"
    elif openrouter_data or openai_data:
        winner = "single"
    else:
        winner = "single"

    provider_meta = {
        "openrouter": {
            "status": openrouter_status,
            "model_used": os.getenv("OPENROUTER_MODEL_PRIMARY", ""),
            "error_code": _map_error_code(openrouter_err),
            "error_message": (openrouter_err or "")[:160],
        },
        "openai": {
            "status": openai_status,
            "model_used": os.getenv("OPENROUTER_MODEL_SECONDARY", ""),
            "error_code": _map_error_code(openai_err),
            "error_message": (openai_err or "")[:160],
        },
    }

    result = {
        "generatedAtTSI": _format_tsi(),
        "dataStatus": "ok" if (openrouter_data or openai_data) else "partial",
        "cache": {"hit": False, "ttl_seconds": ttl, "cooldown_remaining_seconds": 0},
        "providers": providers,
        "provider_meta": provider_meta,
        "winner": winner,
        "consensus": _consensus(openrouter_data, openai_data),
        "disagreements": {},
        "raw": raw,
        "debug_notes": notes,
    }

    referee_enabled = os.getenv("PORTFOLIO_DEBATE_REFEREE_ENABLED", "true").lower() in ("1", "true", "yes", "on")
    referee_only_on_disagreement = os.getenv("PORTFOLIO_DEBATE_REFEREE_ONLY_ON_DISAGREEMENT", "false").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    referee_threshold = float(os.getenv("PORTFOLIO_DEBATE_REFEREE_DISAGREEMENT_THRESHOLD", "0.25") or 0.25)
    referee_timeout_ms = int(os.getenv("PORTFOLIO_DEBATE_REFEREE_TIMEOUT_MS", "20000") or 20000)
    run_on_single = os.getenv("PORTFOLIO_DEBATE_REFEREE_RUN_ON_SINGLE_PROVIDER", "true").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    run_on_both_fail = os.getenv("PORTFOLIO_DEBATE_REFEREE_RUN_ON_BOTH_FAIL", "false").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    referee = {
        "status": "skipped",
        "mode": "skipped",
        "reason": None,
        "model_used": os.getenv("PORTFOLIO_DEBATE_REFEREE_MODEL", ""),
        "error": None,
        "result": None,
    }
    if referee_enabled:
        ok_providers = [p for p, status in providers.items() if status == "ok"]
        fail_providers = [p for p, status in providers.items() if status != "ok"]
        notes.append(f"ok_providers={','.join(ok_providers) if ok_providers else 'none'}")
        notes.append(f"fail_providers={','.join(fail_providers) if fail_providers else 'none'}")

        referee_mode = "skipped_no_provider"
        if len(ok_providers) == 2:
            if disagreement_score >= referee_threshold:
                referee_mode = "judge"
            elif referee_only_on_disagreement:
                referee_mode = "skipped_low_disagreement"
            else:
                referee_mode = "analyst_low_disagreement"
        elif len(ok_providers) == 1:
            referee_mode = "analyst_single_provider" if run_on_single else "skipped_single_provider"
        else:
            referee_mode = "analyst_no_provider" if run_on_both_fail else "skipped_no_provider"

        referee["mode"] = referee_mode
        notes.append(f"referee_mode={referee_mode}")

        should_run = referee_mode.startswith("analyst") or referee_mode == "judge"
        if should_run:
            ref_ctx = _referee_context(context, openrouter_data or {}, openai_data or {}, base, window, horizon)
            ref_ctx["provider_meta"] = provider_meta
            ref_ctx["mode"] = referee_mode
            if referee_mode == "analyst_single_provider":
                primary = ok_providers[0]
                ref_ctx["primary_plan"] = raw.get(primary) or {}
                ref_ctx["missing_provider"] = {
                    "name": fail_providers[0] if fail_providers else "",
                    "error_code": provider_meta.get(fail_providers[0] if fail_providers else "", {}).get("error_code"),
                }
            status, judge, err, meta = call_openrouter_referee(
                ref_ctx, referee_timeout_ms, mode="judge" if referee_mode == "judge" else "analyst"
            )
            referee["status"] = status
            referee["error"] = err
            referee["result"] = judge
            referee["model_used"] = meta.get("referee_model_used", referee["model_used"])
            referee["reason"] = referee_mode
            notes.append(f"referee_status={status}")
            notes.append(f"referee_model={referee['model_used']}")
            if meta.get("policy_blocked"):
                notes.append("referee_policy_blocked=true")
            if status == "ok" and isinstance(judge, dict):
                if referee_mode == "judge" and judge.get("winner") in ("provider_a", "provider_b"):
                    result["winner"] = "referee"
                    result["winner_reason"] = "referee_selected"
                    selected = openrouter_data if judge.get("winner") == "provider_a" else openai_data
                    if isinstance(selected, dict):
                        result["consensus"] = dict(selected)
                        result["consensus"]["referee"] = judge
                else:
                    if isinstance(result.get("consensus"), dict):
                        result["consensus"]["referee"] = judge
        else:
            referee["status"] = "skipped"
            referee["reason"] = referee_mode
            notes.append(f"referee_status={referee_mode}")

    result["referee"] = referee

    if cache:
        cache.set(key, result, ttl)
        cache.set(ts_key, now, cooldown + 5)
        cache.set(last_key, result, ttl)

    if singleflight_enabled:
        evt.set()
        with _INFLIGHT_LOCK:
            _INFLIGHT.pop(inflight_key, None)
    return result


def get_cached_debate(pipeline, base: str, window: str, horizon: str) -> dict | None:
    cache = pipeline.cache if pipeline else None
    if not cache:
        return None
    last_key = cache_key("debate_last", base, window, horizon)
    cached = cache.get(last_key)
    if cached:
        cached["cache"] = {
            "hit": True,
            "ttl_seconds": int(os.getenv("PORTFOLIO_DEBATE_TTL_SECONDS", "21600")),
            "cooldown_remaining_seconds": 0,
        }
    return cached
