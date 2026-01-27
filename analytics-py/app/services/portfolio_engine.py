from __future__ import annotations

import json
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError

import numpy as np

from app.models import IntelRequest, NewsItem
from app.engine.news_engine import (
    annotate_items,
    dedup_clusters,
    dedup_global,
    fetch_news,
    final_rank_score,
    normalize_finnhub,
    normalize_rss,
)
from app.providers.finnhub_news import fetch_finnhub_company_news
from app.providers.rss import fetch_rss
from app.infra.cache import cache_key
from app.services.quote_router import get_quote_router
from app.providers.yahoo import _fetch_chart


BASE_DIR = Path(__file__).resolve().parents[3]
ALIASES_PATH = BASE_DIR / "frontend" / "src" / "lib" / "portfolio_aliases.json"

TR_MAP = str.maketrans(
    {
        "ı": "i",
        "İ": "i",
        "ş": "s",
        "Ş": "s",
        "ğ": "g",
        "Ğ": "g",
        "ü": "u",
        "Ü": "u",
        "ö": "o",
        "Ö": "o",
        "ç": "c",
        "Ç": "c",
    }
)

SHORT_TICKERS = {"HL", "SIL"}
SHORT_TICKER_CONTEXT = {"stock", "shares", "nyse", "nasdaq", "etf", "inc", "corp", "company"}


DEFAULT_HOLDINGS = [
    {"symbol": "ASTOR", "qty": 390.0},
    {"symbol": "SOKM", "qty": 725.0},
    {"symbol": "TUPRS", "qty": 98.0},
    {"symbol": "ENJSA", "qty": 250.0},
    {"symbol": "SIL", "qty": 12.0346},
    {"symbol": "AMD", "qty": 3.1127},
    {"symbol": "PLTR", "qty": 1.3093},
    {"symbol": "HL", "qty": 2.9487},
    {"symbol": "BTC", "qty": 0.0254814},
    {"symbol": "NEAR", "qty": 116.946},
]


@dataclass
class PortfolioSettings:
    max_weight: float = 0.30
    max_crypto_weight: float = 0.20
    turnover_daily: float = 0.05
    turnover_weekly: float = 0.15
    turnover_monthly: float = 0.30


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _to_tsi(iso: str | None) -> str | None:
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except Exception:
        return iso
    return dt.astimezone(timezone(timedelta(hours=3))).isoformat()


def normalize(text: str) -> str:
    text = text.translate(TR_MAP).lower()
    text = re.sub(r"[^a-z0-9\s$]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def recency_weight(ts: str | None) -> float:
    if not ts:
        return 0.2
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return 0.2
    hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
    if hours <= 1:
        return 1.0
    if hours <= 6:
        return 0.7
    if hours <= 24:
        return 0.4
    return 0.2


def load_aliases() -> dict:
    if ALIASES_PATH.exists():
        try:
            return json.loads(ALIASES_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"symbols": {}, "fx": {"USDTRY": "USDTRY=X"}}


def _build_portfolio_watchlist(alias_map: dict) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for holding in DEFAULT_HOLDINGS:
        symbol = holding.get("symbol")
        if not symbol:
            continue
        if symbol not in seen:
            terms.append(symbol)
            seen.add(symbol)
        aliases = alias_map.get("symbols", {}).get(symbol, {}).get("aliases", [])
        added = 0
        for alias in aliases:
            if not alias:
                continue
            alias_clean = alias.strip()
            if not alias_clean:
                continue
            lower = alias_clean.lower()
            if lower == symbol.lower():
                continue
            if lower in {"near", "hl", "sil"}:
                continue
            if len(alias_clean) < 4:
                continue
            if alias_clean in seen:
                continue
            terms.append(alias_clean)
            seen.add(alias_clean)
            added += 1
            if added >= 3:
                break
    return terms


def _token_set_ratio(a: str, b: str) -> float:
    tokens_a = set(re.findall(r"\w+", a))
    tokens_b = set(re.findall(r"\w+", b))
    if not tokens_a or not tokens_b:
        return 0.0
    intersect = " ".join(sorted(tokens_a & tokens_b))
    combined_a = " ".join(sorted(tokens_a))
    combined_b = " ".join(sorted(tokens_b))
    ra = difflib_ratio(intersect, combined_a)
    rb = difflib_ratio(intersect, combined_b)
    return max(ra, rb)


def difflib_ratio(a: str, b: str) -> float:
    import difflib

    return difflib.SequenceMatcher(None, a, b).ratio()


def _direct_ticker_match(text: str, symbol: str) -> tuple[bool, str | None]:
    if symbol in SHORT_TICKERS:
        text_lower = text.lower()
        if re.search(rf"\${symbol}\b", text, flags=re.IGNORECASE):
            return True, f"${symbol}"
        if re.search(rf"\b{symbol}\b", text, flags=re.IGNORECASE):
            if any(ctx in text_lower for ctx in SHORT_TICKER_CONTEXT):
                return True, symbol
        return False, None
    if symbol == "NEAR":
        if re.search(r"\bnear protocol\b", text):
            return True, "near protocol"
        if re.search(r"\bnearprotocol\b", text):
            return True, "nearprotocol"
        if re.search(r"\bNEAR\b", text):
            return True, "NEAR"
        return False, None
    if re.search(rf"\b{re.escape(symbol)}\b", text, flags=re.IGNORECASE):
        return True, symbol
    return False, None


def match_news_item(item: NewsItem, alias_map: dict) -> tuple[list[dict], dict]:
    title = item.title or ""
    url = item.url or ""
    entities = item.entities or []
    norm_title = normalize(title)
    norm_entities = [normalize(e) for e in entities]

    matches: list[dict] = []
    debug_counts = {"direct": 0, "entity": 0, "title": 0, "fuzzy": 0, "sector": 0, "guarded": 0}

    for symbol, data in alias_map.get("symbols", {}).items():
        aliases = data.get("aliases", [])
        norm_aliases = [normalize(a) for a in aliases]
        matched = False
        # Direct ticker match (title/url)
        direct, phrase = _direct_ticker_match(f"{title} {url}", symbol)
        if direct:
            matches.append({"symbol": symbol, "method": "direct", "score": 1.0, "matched_phrase": phrase})
            debug_counts["direct"] += 1
            matched = True

        # Entities exact/alias
        if not matched and norm_entities:
            for ent in norm_entities:
                if ent in norm_aliases:
                    if symbol == "NEAR" and ent == "near":
                        if "near protocol" not in norm_title and "nearprotocol" not in norm_title:
                            continue
                    matches.append({"symbol": symbol, "method": "entity", "score": 0.9, "matched_phrase": ent})
                    debug_counts["entity"] += 1
                    matched = True
                    break

        # Title alias match
        if not matched:
            for alias in norm_aliases:
                if alias and alias in norm_title:
                    # Guard short tickers
                    if symbol in SHORT_TICKERS and len(alias) <= 2:
                        continue
                    if symbol == "NEAR" and alias == "near" and "near protocol" not in norm_title:
                        continue
                    matches.append({"symbol": symbol, "method": "title", "score": 0.7, "matched_phrase": alias})
                    debug_counts["title"] += 1
                    matched = True
                    break

        # Fuzzy match
        if not matched:
            for alias in norm_aliases:
                if len(alias.split()) < 2:
                    continue
                ratio = _token_set_ratio(alias, norm_title)
                if ratio >= 0.88:
                    matches.append({"symbol": symbol, "method": "fuzzy", "score": 0.6, "matched_phrase": alias})
                    debug_counts["fuzzy"] += 1
                    matched = True
                    break

    # Fuzzy guard: if too many symbols, drop fuzzy
    if len(matches) > 4:
        pruned = [m for m in matches if m["method"] != "fuzzy"]
        debug_counts["guarded"] += len(matches) - len(pruned)
        matches = pruned

    return matches, debug_counts


def sector_match(item: NewsItem, alias_map: dict) -> list[dict]:
    matches = []
    impacts = item.sector_impacts or []
    if not impacts:
        return matches
    for symbol, data in alias_map.get("symbols", {}).items():
        sector = data.get("sector")
        if not sector:
            continue
        for imp in impacts:
            imp_sector = imp.get("sector") if isinstance(imp, dict) else getattr(imp, "sector", None)
            if imp_sector == sector:
                matches.append({
                    "symbol": symbol,
                    "method": "sector",
                    "score": 0.4,
                    "matched_phrase": sector,
                })
    return matches


def news_direction(item: NewsItem, flow_score: float | None, risk_flags: list[str]) -> float:
    weights = {
        "regülasyon_baskısı": -1.0,
        "regülasyon/hukuk": -1.0,
        "regulasyon_baskisi": -1.0,
        "regulasyon/hukuk": -1.0,
        "risk_primi": -0.8,
        "büyüme": 0.8,
        "arz_zinciri": -0.4,
    }
    dir_score = 0.0
    channels = item.impact_channel or []
    for ch in channels:
        dir_score += weights.get(ch, 0.0)
    if "likidite" in channels:
        if flow_score is not None:
            if flow_score >= 60:
                dir_score += 0.5
            elif flow_score <= 40:
                dir_score -= 0.5
    if any("RISK_OFF" in f for f in risk_flags or []):
        dir_score -= 0.2
    if any("RISK_ON" in f for f in risk_flags or []):
        dir_score += 0.2
    return max(-1.0, min(1.0, dir_score))


def compute_news_impact(
    items: list[NewsItem],
    alias_map: dict,
    flow_score: float | None,
    risk_flags: list[str],
    event_points: list[dict] | None,
) -> tuple[list[dict], dict]:
    matches_summary = {"direct": 0, "entity": 0, "title": 0, "fuzzy": 0, "sector": 0, "guarded": 0}
    direct_methods = {"direct", "entity", "title", "fuzzy"}
    output = []
    for item in items:
        matches, counts = match_news_item(item, alias_map)
        for k in matches_summary:
            matches_summary[k] += counts.get(k, 0)
        # indirect sector matches
        sector_matches = sector_match(item, alias_map)
        if sector_matches:
            matches.extend(sector_matches)
            matches_summary["sector"] += len(sector_matches)
        if not matches:
            continue

        w_base = (item.relevance_score or item.score or 0) / 100.0
        w_base *= (item.quality_score or 0) / 100.0
        w = w_base * recency_weight(item.publishedAtISO)
        low_signal = item.event_type == "OTHER" and not (item.impact_channel or [])
        if low_signal:
            w *= 0.25

        dir_score = news_direction(item, flow_score, risk_flags)
        direction = "neutral"
        if dir_score > 0.05:
            direction = "positive"
        elif dir_score < -0.05:
            direction = "negative"

        impact_per_symbol = {}
        impact_per_symbol_direct = {}
        impact_per_symbol_indirect = {}
        for m in matches:
            impact_per_symbol.setdefault(m["symbol"], 0.0)
            impact = w * dir_score * m["score"]
            impact_per_symbol[m["symbol"]] += impact
            if m["method"] in direct_methods:
                impact_per_symbol_direct.setdefault(m["symbol"], 0.0)
                impact_per_symbol_direct[m["symbol"]] += impact
            elif m["method"] == "sector":
                impact_per_symbol_indirect.setdefault(m["symbol"], 0.0)
                impact_per_symbol_indirect[m["symbol"]] += impact

        evidence = None
        if event_points:
            for ev in event_points:
                if ev.get("headline") == item.title or ev.get("title") == item.title:
                    evidence = {
                        "event_id": ev.get("event_id"),
                        "published_at_utc": ev.get("published_at_utc"),
                        "reaction": ev.get("reactions"),
                    }
                    break

        output.append(
            {
                "title": item.title,
                "url": item.url,
                "publishedAtISO": item.publishedAtISO,
                "event_type": item.event_type,
                "impact_channel": item.impact_channel or [],
                "matchedSymbols": sorted(impact_per_symbol.keys()),
                "match_debug": matches,
                "impactScore": sum(impact_per_symbol.values()),
                "direction": direction,
                "low_signal": low_signal,
                "impact_by_symbol": impact_per_symbol,
                "impact_by_symbol_direct": impact_per_symbol_direct,
                "impact_by_symbol_indirect": impact_per_symbol_indirect,
                "evidence": evidence,
            }
        )

    return output, matches_summary


def fetch_price(symbol: str) -> tuple[float | None, str, str | None]:
    router = get_quote_router()
    result = router.get_quote(symbol)
    if result.ok and result.data and result.data.price:
        return float(result.data.price), "quote_router", result.data.currency
    # Fallback to Yahoo chart last close (useful for symbols not resolved by quote router)
    try:
        chart = _fetch_chart(symbol, "5d", "1d", 4.0)
        if chart:
            df = chart.get("df")
            if df is not None and not df.empty:
                last = df["Close"].dropna().iloc[-1]
                currency = (chart.get("meta") or {}).get("currency")
                return float(last), "yahoo_chart", currency
    except Exception:
        pass
    return None, "missing", None


def fetch_daily_history(symbol: str, timeout: float = 6.0) -> list[float]:
    try:
        result = _fetch_chart(symbol, "6mo", "1d", timeout)
        if not result:
            return []
        df = result.get("df")
        if df is None or df.empty:
            return []
        return [float(v) for v in df["Close"].dropna().tolist()]
    except Exception:
        return []


def compute_risk_metrics(holdings: list[dict], prices: dict, settings: PortfolioSettings) -> tuple[dict, list[str]]:
    missing = []
    weights = [h.get("weight", 0.0) for h in holdings]
    hhi = sum(w * w for w in weights)
    max_weight = max(weights) if weights else 0.0
    eps = 1e-9

    vols = []
    def _ret_from_hist(hist: list[float], days: int) -> float | None:
        if len(hist) <= days:
            return None
        last = hist[-1]
        prev = hist[-1 - days]
        if prev == 0:
            return None
        return (last - prev) / prev

    for h in holdings:
        sym = h["symbol"]
        hist = fetch_daily_history(h["yahoo_symbol"])
        if len(hist) < 10:
            missing.append(sym)
            continue
        rets = []
        for i in range(1, len(hist)):
            if hist[i - 1] == 0:
                continue
            rets.append((hist[i] - hist[i - 1]) / hist[i - 1])
        if len(rets) < 5:
            missing.append(sym)
            continue
        vol = float(np.std(rets[-30:])) if len(rets) >= 30 else float(np.std(rets))
        vols.append(vol)
        h["vol_30d"] = vol
        h["ret_1d"] = _ret_from_hist(hist, 1)
        h["ret_7d"] = _ret_from_hist(hist, 7)
        h["ret_30d"] = _ret_from_hist(hist, 30)
        if h.get("ret_7d") is not None:
            mom_z_7d = h["ret_7d"] / (max(vol, eps) * math.sqrt(7))
            h["mom_z_7d"] = max(-3.0, min(3.0, mom_z_7d))
        else:
            h["mom_z_7d"] = None
        if h.get("ret_30d") is not None:
            mom_z_30d = h["ret_30d"] / (max(vol, eps) * math.sqrt(30))
            h["mom_z_30d"] = max(-3.0, min(3.0, mom_z_30d))
        else:
            h["mom_z_30d"] = None

    weight_sum = sum(weights)
    if weight_sum > 0:
        port_vol = float(np.average([h.get("vol_30d", 0.0) for h in holdings], weights=weights))
    else:
        vols_only = [h.get("vol_30d", 0.0) for h in holdings if h.get("vol_30d") is not None]
        port_vol = float(np.mean(vols_only)) if vols_only else 0.0
    var_95_1d = 1.65 * port_vol

    risk = {
        "vol_30d": port_vol,
        "var_95_1d": var_95_1d,
        "max_weight": max_weight,
        "hhi": hhi,
        "data_status": "partial" if missing else ("ok" if weight_sum > 0 else "missing"),
    }
    return risk, missing


def compute_fx_risk(allocation: dict, threshold: float = 0.50) -> tuple[float, float, bool]:
    by_currency = allocation.get("by_currency") or {}
    usd_exposure = float(by_currency.get("USD", 0.0) or 0.0)
    fx_risk_proxy = usd_exposure
    return usd_exposure, fx_risk_proxy, usd_exposure >= threshold


def build_optimizer(
    holdings: list[dict],
    news_impact: dict,
    news_direct: dict,
    news_indirect: dict,
    risk_flags: list[str],
    settings: PortfolioSettings,
    coverage_ratio: float = 1.0,
    coverage_total: int = 0,
    low_signal_ratio: float = 0.0,
    fx_risk_proxy: float = 0.0,
) -> list[dict]:
    clamp_ratio = min(1.0, max(0.3, coverage_ratio))
    hold_gate = False
    hold_reason = ""
    if coverage_total == 0:
        hold_gate = True
        hold_reason = "NO_NEWS_ITEMS"
    elif coverage_ratio < 0.20 or low_signal_ratio > 0.50:
        hold_gate = True
        hold_reason = "LOW_COVERAGE_OR_LOW_SIGNAL"

    coeffs = {
        "daily": {"a": 0.20, "b": 0.30, "b2": 0.12, "c": 0.15, "s": 0.10, "d": 0.20, "e": 0.15, "f": 0.10, "g": 0.08},
        "weekly": {"a": 0.25, "b": 0.25, "b2": 0.15, "c": 0.15, "s": 0.15, "d": 0.18, "e": 0.12, "f": 0.10, "g": 0.06},
        "monthly": {"a": 0.30, "b": 0.15, "b2": 0.18, "c": 0.12, "s": 0.20, "d": 0.15, "e": 0.15, "f": 0.10, "g": 0.05},
    }
    periods = [
        ("daily", settings.turnover_daily * clamp_ratio),
        ("weekly", settings.turnover_weekly * clamp_ratio),
        ("monthly", settings.turnover_monthly * clamp_ratio),
    ]
    tcost_by_class = {"BIST": 0.0015, "NASDAQ": 0.0005, "CRYPTO": 0.0010}

    def _clamp(val: float, lo: float = -1.0, hi: float = 1.0) -> float:
        return max(lo, min(hi, val))

    results = []
    risk_off = any("RISK_OFF" in f for f in risk_flags or [])
    for period, turnover_cap in periods:
        if hold_gate:
            results.append(
                {
                    "period": period,
                    "actions": [],
                    "status": "hold",
                    "notes": [hold_reason],
                    "turnover_cap": turnover_cap,
                    "mode": "HOLD",
                    "hold_reason": hold_reason,
                }
            )
            continue
        cset = coeffs.get(period, coeffs["daily"])
        scored = []
        for h in holdings:
            mom_raw = h.get("mom_z_7d")
            mom = _clamp((mom_raw or 0.0) / 3.0)
            news_dir = _clamp(news_direct.get(h["symbol"], 0.0))
            news_ind = _clamp(news_indirect.get(h["symbol"], 0.0))
            regime = -0.3 if risk_off and h["asset_class"] == "CRYPTO" else 0.1
            regime = _clamp(regime)
            sector_rotation = 0.0
            vol = h.get("vol_30d", 0.0) or 0.0
            vol_norm = _clamp(vol / 0.10, 0.0, 1.0)
            concentration = h.get("weight", 0.0) ** 2
            max_w = settings.max_weight or 0.30
            conc_norm = _clamp(concentration / (max_w * max_w), 0.0, 1.0)
            fx_penalty = fx_risk_proxy if h.get("currency") == "USD" else 0.0
            fx_penalty = _clamp(fx_penalty, 0.0, 1.0)
            tcost = tcost_by_class.get(h.get("asset_class"), 0.0010)
            tcost_norm = _clamp(tcost / 0.002, 0.0, 1.0)

            score = (
                cset["a"] * mom
                + cset["b"] * news_dir
                + cset["b2"] * news_ind
                + cset["c"] * regime
                + cset["s"] * sector_rotation
                - cset["d"] * vol_norm
                - cset["e"] * conc_norm
                - cset["f"] * fx_penalty
                - cset["g"] * tcost_norm
            )
            breakdown = {
                "mom": mom,
                "news_direct": news_dir,
                "news_indirect": news_ind,
                "regime": regime,
                "sector_rotation": sector_rotation,
                "vol": vol_norm,
                "concentration": conc_norm,
                "fx_risk": fx_penalty,
                "tcost": tcost_norm,
                "total": score,
            }
            scored.append((score, h, breakdown))
        scored.sort(key=lambda x: x[0], reverse=True)
        increases = [s for s in scored if s[0] > 0][:3]
        decreases = [s for s in scored if s[0] < 0][-3:]
        actions = []
        delta = min(turnover_cap / 2.0, 0.03)
        for score, h, breakdown in increases:
            if h["weight"] + delta > settings.max_weight:
                continue
            if h["asset_class"] == "CRYPTO":
                crypto_weight = sum(x["weight"] for x in holdings if x["asset_class"] == "CRYPTO")
                if crypto_weight + delta > settings.max_crypto_weight:
                    continue
            actions.append(
                {
                    "symbol": h["symbol"],
                    "action": "increase",
                    "deltaWeight": round(delta, 4),
                    "reason": [
                        f"score={round(score,3)}",
                        f"newsImpact={round(news_impact.get(h['symbol'],0.0),3)}",
                    ],
                    "confidence": int(60 + min(30, abs(score) * 100)),
                    "score_breakdown": breakdown,
                }
            )
        for score, h, breakdown in decreases:
            actions.append(
                {
                    "symbol": h["symbol"],
                    "action": "decrease",
                    "deltaWeight": round(delta, 4),
                    "reason": [
                        f"score={round(score,3)}",
                        f"newsImpact={round(news_impact.get(h['symbol'],0.0),3)}",
                    ],
                    "confidence": int(60 + min(30, abs(score) * 100)),
                    "score_breakdown": breakdown,
                }
            )
        results.append(
            {
                "period": period,
                "actions": actions,
                "status": "ok" if actions else "partial",
                "notes": [] if actions else ["insufficient_signal"],
                "turnover_cap": turnover_cap,
                "mode": "ACTIVE",
            }
        )
    return results


def build_portfolio(pipeline, base_currency: str = "TRY", news_horizon: str = "24h") -> dict:
    alias_map = load_aliases()
    settings = PortfolioSettings()
    start = time.time()
    portfolio_terms = _build_portfolio_watchlist(alias_map)
    watchlist_terms = sorted(set(portfolio_terms))
    wl_key = ",".join(watchlist_terms)

    # News/flow/risk: prefer cached news to avoid slow pipeline on portfolio requests
    top_news: list[NewsItem] = []
    flow_score = None
    risk_flags: list[str] = []
    event_points: list[dict] = []
    used_pipeline = False
    used_cache = False
    cache_hit = "miss"
    news_debug_notes: list[str] = []

    if getattr(pipeline, "cache", None) is not None:
        cached = None
        keys = [
            ("primary", cache_key("news", news_horizon, wl_key)),
            ("all", cache_key("news", news_horizon, "all")),
            ("empty", cache_key("news", news_horizon, "")),
        ]
        for name, key in keys:
            if not key:
                continue
            try:
                cached = pipeline.cache.get(key)
            except Exception:
                cached = None
            if cached:
                cache_hit = name
                break
        if cached:
            top_news = [NewsItem(**n) for n in cached]
            used_cache = True

    if not used_cache and os.getenv("PORTFOLIO_PIPELINE_ENABLED", "false").lower() in ("1", "true", "yes", "on"):
        pipeline_error = None
        use_fast_mode = os.getenv("PORTFOLIO_NEWS_FAST_MODE", "true").lower() in ("1", "true", "yes", "on")
        fast_long_only = os.getenv("PORTFOLIO_NEWS_FAST_LONG_ONLY", "true").lower() in ("1", "true", "yes", "on")
        use_fast = use_fast_mode and (news_horizon.endswith("d") or not fast_long_only)
        if use_fast:
            try:
                maxrecords = 80 if news_horizon.endswith("d") else 48
                base_timeout = getattr(getattr(pipeline, "settings", None), "request_timeout", 15)
                fast_timeout = float(os.getenv("PORTFOLIO_NEWS_TIMEOUT", "12"))
                timeout = min(base_timeout, fast_timeout) if base_timeout else fast_timeout
                watchlist_cap = int(os.getenv("PORTFOLIO_NEWS_WATCHLIST_CAP", "12") or 12)
                news_watchlist = watchlist_terms[:watchlist_cap] if watchlist_cap > 0 else watchlist_terms
                rank_weights = getattr(getattr(pipeline, "settings", None), "news_rank_weights", None)
                total_timeout = float(os.getenv("PORTFOLIO_NEWS_TOTAL_TIMEOUT", "20"))
                news_ex = ThreadPoolExecutor(max_workers=1)
                future = news_ex.submit(
                    fetch_news,
                    "",
                    news_horizon,
                    maxrecords,
                    timeout,
                    watchlist=news_watchlist,
                    rank_weights=rank_weights,
                    skip_gdelt=True,
                )
                try:
                    top_news, notes, used_span = future.result(timeout=total_timeout)
                except FuturesTimeoutError:
                    future.cancel()
                    notes = []
                    used_span = news_horizon
                    fallback_notes = ["portfolio_news_fallback=timeout"]
                    fh = fetch_finnhub_company_news(news_watchlist, news_horizon, min(timeout, 6))
                    fallback_items: list[NewsItem] = []
                    if fh.ok and fh.data:
                        fallback_items.extend(normalize_finnhub(fh.data))
                        fallback_notes.append("portfolio_news_fallback=finnhub")
                    else:
                        fallback_notes.append("portfolio_news_fallback=finnhub_empty")
                    if not fallback_items:
                        rss = fetch_rss(" ".join(news_watchlist[:6]) or "markets", maxrecords, min(timeout, 6), news_horizon)
                        if rss.ok and rss.data:
                            fallback_items.extend(normalize_rss(rss.data))
                            fallback_notes.append("portfolio_news_fallback=rss")
                        elif rss.error_msg:
                            fallback_notes.append(f"portfolio_news_fallback_rss_error={str(rss.error_msg)[:80]}")
                    if fallback_items:
                        annotated, meta = annotate_items(fallback_items, 0.2)
                        deduped = dedup_clusters(annotated, meta)
                        deduped = dedup_global(deduped)
                        deduped.sort(key=lambda x: (final_rank_score(x, rank_weights), x.quality_score), reverse=True)
                        top_news = deduped[:maxrecords]
                        notes.extend(fallback_notes)
                    else:
                        notes.extend(fallback_notes)
                        raise TimeoutError("portfolio_news_timeout")
                finally:
                    news_ex.shutdown(wait=False, cancel_futures=True)
                if getattr(pipeline, "cache", None) is not None:
                    try:
                        pipeline.cache.set(cache_key("news", news_horizon, wl_key), [n.model_dump() for n in top_news], 90)
                        pipeline.cache.set(cache_key("news", news_horizon, "all"), [n.model_dump() for n in top_news], 90)
                    except Exception:
                        pass
                used_pipeline = True
                news_debug_notes = [
                    "portfolio_news_fast_mode=true",
                    f"portfolio_news_fast_span={used_span}",
                    f"portfolio_news_fast_timeout={timeout}",
                    f"portfolio_news_fast_total_timeout={total_timeout}",
                    f"portfolio_news_fast_watchlist_cap={watchlist_cap}",
                ]
            except Exception as exc:
                cache_hit = "miss"
                used_pipeline = False
                top_news = []
                flow_score = None
                risk_flags = []
                event_points = []
                pipeline_error = f"{type(exc).__name__}:{str(exc)[:120]}"
        else:
            try:
                intel = pipeline.run(IntelRequest(timeframe="1h", newsTimespan=news_horizon, watchlist=watchlist_terms))
                top_news = intel.top_news or []
                flow_score = getattr(intel.flow, "flow_score", None)
                risk_flags = getattr(intel.risk, "flags", []) or []
                try:
                    event_points = [p.model_dump() for p in (intel.flow.event_study or [])]
                except Exception:
                    event_points = []
                if getattr(pipeline, "cache", None) is not None:
                    try:
                        pipeline.cache.set(cache_key("news", news_horizon, wl_key), [n.model_dump() for n in top_news], 90)
                        pipeline.cache.set(cache_key("news", news_horizon, "all"), [n.model_dump() for n in top_news], 90)
                    except Exception:
                        pass
                used_pipeline = True
            except Exception as exc:
                cache_hit = "miss"
                used_pipeline = False
                top_news = []
                flow_score = None
                risk_flags = []
                event_points = []
                pipeline_error = f"{type(exc).__name__}:{str(exc)[:120]}"
    else:
        pipeline_error = None

    fx_symbol = alias_map.get("fx", {}).get("USDTRY", "USDTRY=X")
    fx_price, fx_source, _ = fetch_price(fx_symbol)
    fx_rate = fx_price or 0.0
    fx_status = "ok" if fx_rate else "missing"

    holdings = []
    missing_prices = []
    total_value = 0.0
    price_results: dict[str, tuple[float | None, str, str | None]] = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {}
        for h in DEFAULT_HOLDINGS:
            symbol = h["symbol"]
            data = alias_map.get("symbols", {}).get(symbol, {})
            yahoo_symbol = data.get("yahoo", symbol)
            futures[ex.submit(fetch_price, yahoo_symbol)] = yahoo_symbol
        for fut in as_completed(futures):
            yahoo_symbol = futures[fut]
            try:
                price_results[yahoo_symbol] = fut.result()
            except Exception:
                price_results[yahoo_symbol] = (None, "missing", None)

    for h in DEFAULT_HOLDINGS:
        symbol = h["symbol"]
        data = alias_map.get("symbols", {}).get(symbol, {})
        yahoo_symbol = data.get("yahoo", symbol)
        currency = data.get("currency", "USD")
        price, price_source, quote_ccy = price_results.get(yahoo_symbol, (None, "missing", None))
        if price is None:
            missing_prices.append(symbol)
            price = 0.0
        if base_currency == "TRY" and currency == "USD":
            mkt_value = price * h["qty"] * (fx_rate or 0.0)
        elif base_currency == "USD" and currency == "TRY":
            mkt_value = price * h["qty"] / (fx_rate or 0.0) if fx_rate else 0.0
        else:
            mkt_value = price * h["qty"]
        total_value += mkt_value
        holdings.append(
            {
                "symbol": symbol,
                "qty": h["qty"],
                "price": price,
                "currency": currency,
                "yahoo_symbol": yahoo_symbol,
                "asset_class": data.get("asset_class", "UNKNOWN"),
                "sector": data.get("sector"),
                "mkt_value_base": mkt_value,
                "data_status": "missing" if price_source == "missing" else "ok",
            }
        )

    for h in holdings:
        h["weight"] = (h["mkt_value_base"] / total_value) if total_value else 0.0

    allocation = {
        "by_asset_class": {},
        "by_currency": {},
    }
    for h in holdings:
        allocation["by_asset_class"][h["asset_class"]] = allocation["by_asset_class"].get(h["asset_class"], 0.0) + h["weight"]
        allocation["by_currency"][h["currency"]] = allocation["by_currency"].get(h["currency"], 0.0) + h["weight"]

    risk, missing_vol = compute_risk_metrics(holdings, {}, settings)

    usd_exposure, fx_risk_proxy, fx_flag = compute_fx_risk(allocation)
    risk["fx_usd_exposure"] = usd_exposure
    risk["fx_risk_proxy"] = fx_risk_proxy
    if fx_flag:
        risk_flags = list(risk_flags or [])
        risk_flags.append("FX_RISK_UP")

    news_items, match_summary = compute_news_impact(top_news, alias_map, flow_score, risk_flags, event_points)
    asset_news = {}
    asset_news_direct = {}
    asset_news_indirect = {}
    low_signal_count = 0
    for n in news_items:
        if n.get("low_signal"):
            low_signal_count += 1
        for sym, score in n.get("impact_by_symbol", {}).items():
            asset_news[sym] = asset_news.get(sym, 0.0) + score
        for sym, score in n.get("impact_by_symbol_direct", {}).items():
            asset_news_direct[sym] = asset_news_direct.get(sym, 0.0) + score
        for sym, score in n.get("impact_by_symbol_indirect", {}).items():
            asset_news_indirect[sym] = asset_news_indirect.get(sym, 0.0) + score

    coverage_ratio = (len(news_items) / len(top_news)) if top_news else 0.0
    low_signal_ratio = (low_signal_count / max(1, len(news_items))) if news_items else 0.0
    recs = build_optimizer(
        holdings,
        asset_news,
        asset_news_direct,
        asset_news_indirect,
        risk_flags,
        settings,
        coverage_ratio=coverage_ratio,
        coverage_total=len(top_news),
        low_signal_ratio=low_signal_ratio,
        fx_risk_proxy=fx_risk_proxy,
    )

    mom_z_weighted = 0.0
    for h in holdings:
        mom = h.get("mom_z_7d")
        if mom is None:
            continue
        mom_z_weighted += (h.get("weight") or 0.0) * mom

    debug_notes = [
        f"portfolio_price_source=yahoo",
        f"portfolio_news_cache={'hit' if used_cache else 'miss'}",
        f"portfolio_news_cache_hit={cache_hit if used_cache else 'miss'}",
        f"portfolio_news_watchlist_size={len(watchlist_terms)}",
        f"portfolio_news_cache_key_primary_len={len(wl_key)}",
        f"portfolio_pipeline_enabled_env={os.getenv('PORTFOLIO_PIPELINE_ENABLED', '')}",
        f"portfolio_pipeline_used={used_pipeline}",
        f"portfolio_news_fetched_total={len(top_news)}",
        f"portfolio_missing_prices={len(missing_prices)}",
        f"portfolio_fx_status={fx_status}",
        f"portfolio_news_matched={len(news_items)}",
        f"portfolio_news_match_methods={match_summary}",
        f"portfolio_news_false_positive_guard_hits={match_summary.get('guarded', 0)}",
        f"portfolio_opt_status=ok" if recs else "portfolio_opt_status=partial",
        f"coverage_ratio={coverage_ratio:.3f}",
        f"low_signal_ratio={low_signal_ratio:.3f}",
        f"fx_usd_exposure={usd_exposure:.3f}",
        f"portfolio_time_ms={int((time.time()-start)*1000)}",
    ]
    if news_debug_notes:
        debug_notes.extend(news_debug_notes)
    daily_rec = next((r for r in recs if r.get("period") == "daily"), None)
    if daily_rec and daily_rec.get("mode") == "HOLD":
        debug_notes.append("optimizer_hold_gate=true")
        if daily_rec.get("hold_reason"):
            debug_notes.append(f"optimizer_hold_reason={daily_rec.get('hold_reason')}")
    else:
        debug_notes.append("optimizer_hold_gate=false")
    effective_turnover_cap = settings.turnover_daily * min(1.0, max(0.3, coverage_ratio))
    debug_notes.append(f"effective_turnover_cap={effective_turnover_cap:.3f}")
    if pipeline_error:
        debug_notes.append(f"portfolio_pipeline_error={pipeline_error}")

    return {
        "asOfISO": _to_tsi(_now_iso()),
        "baseCurrency": base_currency,
        "fx": {"USDTRY": fx_rate, "status": fx_status, "source": fx_symbol},
        "holdings": holdings,
        "allocation": allocation,
        "risk": risk,
        "newsImpact": {
            "horizon": news_horizon,
            "items": news_items,
            "summary": {
                "impact_by_symbol": asset_news,
                "impact_by_symbol_direct": asset_news_direct,
                "impact_by_symbol_indirect": asset_news_indirect,
                "low_signal_ratio": low_signal_ratio,
                "coverage_ratio": coverage_ratio,
            },
            "coverage": {"total": len(top_news), "matched": len(news_items)},
        },
        "recommendations": recs,
        "optimizer_inputs": {"mom_z_weighted": mom_z_weighted},
        "debug_notes": debug_notes,
    }
