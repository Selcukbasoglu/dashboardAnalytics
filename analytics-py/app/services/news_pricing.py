from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from app.models import NewsItem

_TR_MAP = str.maketrans(
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


FLAGSHIP_CEO_REGISTRY: dict[str, list[dict[str, str]]] = {
    "SEMICONDUCTORS": [
        {"company": "NVIDIA", "ceo": "Jensen Huang"},
        {"company": "AMD", "ceo": "Lisa Su"},
        {"company": "TSMC", "ceo": "C. C. Wei"},
    ],
    "SOFTWARE": [
        {"company": "Microsoft", "ceo": "Satya Nadella"},
        {"company": "Palantir", "ceo": "Alex Karp"},
        {"company": "Salesforce", "ceo": "Marc Benioff"},
    ],
    "POWER_UTILITIES": [
        {"company": "NextEra Energy", "ceo": "John Ketchum"},
        {"company": "Enel", "ceo": "Flavio Cattaneo"},
        {"company": "Iberdrola", "ceo": "Ignacio Galan"},
    ],
    "ENERGY": [
        {"company": "Exxon Mobil", "ceo": "Darren Woods"},
        {"company": "Shell", "ceo": "Wael Sawan"},
        {"company": "Saudi Aramco", "ceo": "Amin Nasser"},
    ],
    "INDUSTRIALS": [
        {"company": "Siemens", "ceo": "Roland Busch"},
        {"company": "GE Aerospace", "ceo": "Larry Culp"},
        {"company": "Honeywell", "ceo": "Vimal Kapur"},
    ],
    "CONSUMER_RETAIL": [
        {"company": "Walmart", "ceo": "Doug McMillon"},
        {"company": "Costco", "ceo": "Ron Vachris"},
        {"company": "Carrefour", "ceo": "Alexandre Bompard"},
    ],
    "METALS_MINERS": [
        {"company": "Rio Tinto", "ceo": "Jakob Stausholm"},
        {"company": "BHP", "ceo": "Mike Henry"},
        {"company": "Barrick", "ceo": "Mark Bristow"},
    ],
    "CRYPTO": [
        {"company": "Coinbase", "ceo": "Brian Armstrong"},
        {"company": "Binance", "ceo": "Richard Teng"},
        {"company": "Tether", "ceo": "Paolo Ardoino"},
    ],
}

# Deterministic constituent list for portfolio ETFs/funds that need look-through tracking.
ETF_CONSTITUENTS_REGISTRY: dict[str, list[dict[str, Any]]] = {
    "SIL": [
        {"symbol": "PAAS", "company": "Pan American Silver", "aliases": ["paas", "pan american silver"]},
        {"symbol": "WPM", "company": "Wheaton Precious Metals", "aliases": ["wpm", "wheaton precious metals"]},
        {"symbol": "AG", "company": "First Majestic Silver", "aliases": ["ag", "first majestic silver"]},
        {"symbol": "CDE", "company": "Coeur Mining", "aliases": ["cde", "coeur mining"]},
        {"symbol": "MAG", "company": "MAG Silver", "aliases": ["mag", "mag silver"]},
        {"symbol": "EXK", "company": "Endeavour Silver", "aliases": ["exk", "endeavour silver"]},
        {"symbol": "SSRM", "company": "SSR Mining", "aliases": ["ssrm", "ssr mining"]},
        {"symbol": "HL", "company": "Hecla Mining", "aliases": ["hl", "hecla", "hecla mining"]},
    ]
}

_ANNOUNCEMENT_KEYWORDS: tuple[tuple[str, str], ...] = (
    ("EARNINGS", "earnings"),
    ("EARNINGS", "results"),
    ("EARNINGS", "financial results"),
    ("EARNINGS", "bilanco"),
    ("EARNINGS", "bilanço"),
    ("GUIDANCE", "guidance"),
    ("GUIDANCE", "outlook"),
    ("GUIDANCE", "beklenti"),
    ("GUIDANCE", "projeksiyon"),
    ("INVESTOR_DAY", "investor day"),
    ("INVESTOR_DAY", "analyst day"),
    ("INVESTOR_DAY", "yatirimci gunu"),
    ("CONFERENCE_CALL", "conference call"),
    ("CONFERENCE_CALL", "earnings call"),
    ("CONFERENCE_CALL", "teleconference"),
    ("CONFERENCE_CALL", "webcast"),
    ("CONFERENCE_CALL", "toplanti"),
    ("DISCLOSURE", "kap"),
    ("DISCLOSURE", "disclosure"),
    ("DISCLOSURE", "filing"),
    ("PRODUCT", "launch"),
    ("PRODUCT", "tanitim"),
    ("PRODUCT", "release"),
    ("PRODUCT", "unveil"),
    ("PRODUCT", "introduce"),
    ("PRODUCT", "roadmap"),
    ("PRODUCT", "product event"),
    ("PRODUCT", "launch event"),
    ("PRODUCT", "beta"),
    ("PRODUCT", "on siparis"),
    ("PRODUCT", "ön sipariş"),
    ("PRODUCT", "urun tanitimi"),
    ("PRODUCT", "ürün tanıtımı"),
    ("REGULATORY", "approval"),
    ("REGULATORY", "onay"),
)

_FUTURE_HINTS = (
    "will",
    "to announce",
    "to report",
    "scheduled",
    "next week",
    "tomorrow",
    "yarin",
    "yarın",
    "gelecek hafta",
    "aciklayacak",
    "açıklayacak",
    "duzenleyecek",
    "düzenleyecek",
    "takvim",
)

_CEO_STATEMENT_HINTS = (
    "ceo",
    "chief executive",
    "said",
    "says",
    "commented",
    "interview",
    "remarks",
    "warned",
    "expects",
    "expects",
    "acikladi",
    "açıkladı",
    "dedi",
    "degerlendirdi",
    "değerlendirdi",
)

_MONTH_MAP = {
    "jan": 1,
    "january": 1,
    "ocak": 1,
    "feb": 2,
    "february": 2,
    "subat": 2,
    "şubat": 2,
    "mar": 3,
    "march": 3,
    "mart": 3,
    "apr": 4,
    "april": 4,
    "nisan": 4,
    "may": 5,
    "mayis": 5,
    "mayıs": 5,
    "jun": 6,
    "june": 6,
    "haziran": 6,
    "jul": 7,
    "july": 7,
    "temmuz": 7,
    "aug": 8,
    "august": 8,
    "agustos": 8,
    "ağustos": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "eylul": 9,
    "eylül": 9,
    "oct": 10,
    "october": 10,
    "ekim": 10,
    "nov": 11,
    "november": 11,
    "kasim": 11,
    "kasım": 11,
    "dec": 12,
    "december": 12,
    "aralik": 12,
    "aralık": 12,
}

_WEEKDAY_MAP = {
    "monday": 0,
    "pazartesi": 0,
    "tuesday": 1,
    "sali": 1,
    "salı": 1,
    "wednesday": 2,
    "carsamba": 2,
    "çarşamba": 2,
    "thursday": 3,
    "persembe": 3,
    "perşembe": 3,
    "friday": 4,
    "cuma": 4,
    "saturday": 5,
    "cumartesi": 5,
    "sunday": 6,
    "pazar": 6,
}


def _normalize(text: str | None) -> str:
    raw = (text or "").translate(_TR_MAP).lower()
    raw = re.sub(r"[^a-z0-9\s./:-]", " ", raw)
    return re.sub(r"\s+", " ", raw).strip()


def _to_dt(iso_value: str | None) -> datetime:
    if not iso_value:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(iso_value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def _extract_eta_iso(title: str, published_iso: str | None) -> tuple[str | None, int | None]:
    norm = _normalize(title)
    if not norm:
        return None, None
    ref = _to_dt(published_iso)

    if "tomorrow" in norm or "yarin" in norm:
        eta = ref + timedelta(days=1)
        return eta.isoformat().replace("+00:00", "Z"), 1
    if "next week" in norm or "gelecek hafta" in norm:
        eta = ref + timedelta(days=7)
        return eta.isoformat().replace("+00:00", "Z"), 7
    if "this week" in norm or "bu hafta" in norm:
        eta = ref + timedelta(days=3)
        return eta.isoformat().replace("+00:00", "Z"), 3

    weekday_hits = [wd for wd in _WEEKDAY_MAP.keys() if re.search(rf"\b{re.escape(wd)}\b", norm)]
    if weekday_hits:
        target = _WEEKDAY_MAP[weekday_hits[0]]
        delta = (target - ref.weekday()) % 7
        delta = 7 if delta == 0 else delta
        eta = ref + timedelta(days=delta)
        return eta.isoformat().replace("+00:00", "Z"), int(delta)

    for m in re.finditer(
        r"\b(?P<day>[0-3]?\d)[\s./-]+(?P<month>[a-zA-ZçğıöşüÇĞİÖŞÜ]+)(?:[\s./-]+(?P<year>20\d{2}))?\b",
        title,
        flags=re.IGNORECASE,
    ):
        try:
            day = int(m.group("day"))
            month_key = (m.group("month") or "").strip().lower()
            month = _MONTH_MAP.get(month_key)
            if not month:
                continue
            year = int(m.group("year")) if m.group("year") else ref.year
            eta = datetime(year=year, month=month, day=day, tzinfo=timezone.utc)
            if eta < ref - timedelta(days=1):
                eta = datetime(year=year + 1, month=month, day=day, tzinfo=timezone.utc)
            delta_days = int((eta - ref).total_seconds() // 86400)
            return eta.isoformat().replace("+00:00", "Z"), max(0, delta_days)
        except Exception:
            continue

    return None, None


def _infer_eta_days_without_date(title: str, published_iso: str | None) -> tuple[str | None, int | None]:
    norm = _normalize(title)
    if not norm:
        return None, None
    ref = _to_dt(published_iso)
    if "this month" in norm or "bu ay" in norm:
        eta_days = 14
    elif "next month" in norm or "gelecek ay" in norm:
        eta_days = 28
    elif any(k in norm for k in ("q1", "q2", "q3", "q4", "ceyrek", "çeyrek")):
        eta_days = 21
    elif any(k in norm for k in _FUTURE_HINTS):
        eta_days = 10
    else:
        return None, None
    eta = ref + timedelta(days=eta_days)
    return eta.isoformat().replace("+00:00", "Z"), eta_days


def _week_bucket(eta_days: int) -> tuple[str, int, int]:
    if eta_days <= 6:
        return "W1", 0, 6
    if eta_days <= 13:
        return "W2", 7, 13
    if eta_days <= 20:
        return "W3", 14, 20
    if eta_days <= 27:
        return "W4", 21, 27
    return "W5", 28, 30


def _build_monthly_plan(upcoming: list[dict[str, Any]], monthly_window_days: int) -> dict[str, Any]:
    items = sorted(
        [x for x in upcoming if isinstance(x.get("eta_days"), int) and int(x.get("eta_days")) <= monthly_window_days],
        key=lambda x: (int(x.get("eta_days", 9999)), -int(x.get("confidence", 0))),
    )

    week_slots: dict[str, dict[str, Any]] = {}
    by_event_type: dict[str, int] = {}
    for item in items:
        event_type = str(item.get("event_type") or "CORPORATE_UPDATE")
        by_event_type[event_type] = by_event_type.get(event_type, 0) + 1
        bucket, start_day, end_day = _week_bucket(int(item.get("eta_days", 30)))
        slot = week_slots.setdefault(
            bucket,
            {
                "week": bucket,
                "start_day": start_day,
                "end_day": end_day,
                "count": 0,
                "event_types": {},
                "symbols": [],
            },
        )
        slot["count"] += 1
        slot["event_types"][event_type] = int(slot["event_types"].get(event_type, 0)) + 1
        sym = str(item.get("symbol") or "").upper()
        if sym and sym not in slot["symbols"]:
            slot["symbols"].append(sym)

    by_week = sorted(week_slots.values(), key=lambda x: int(x.get("start_day", 999)))
    by_type_rows = sorted(
        [{"event_type": k, "count": v} for k, v in by_event_type.items()],
        key=lambda x: int(x["count"]),
        reverse=True,
    )
    return {
        "window_days": monthly_window_days,
        "items": items[:30],
        "by_week": by_week,
        "by_event_type": by_type_rows,
    }


def _announcement_type(title: str) -> str:
    norm = _normalize(title)
    for kind, key in _ANNOUNCEMENT_KEYWORDS:
        if key in norm:
            return kind
    return "CORPORATE_UPDATE"


def _is_announcement(title: str) -> bool:
    norm = _normalize(title)
    if not norm:
        return False
    for _, key in _ANNOUNCEMENT_KEYWORDS:
        if key in norm:
            return True
    return any(hint in norm for hint in _FUTURE_HINTS)


def _headline_sentiment(title: str) -> float:
    norm = _normalize(title)
    pos = ("beat", "raise", "surge", "strong", "record", "onay", "buyume", "guidance raised")
    neg = ("miss", "cut", "lawsuit", "drop", "risk", "yasak", "zayif", "baski")
    if any(k in norm for k in pos):
        return 1.0
    if any(k in norm for k in neg):
        return -1.0
    return 0.0


def _symbol_alias_pairs(alias_map: dict, holdings: list[dict]) -> list[tuple[str, list[str]]]:
    out: list[tuple[str, list[str]]] = []
    symbols_meta = (alias_map or {}).get("symbols", {}) or {}
    for h in holdings:
        symbol = (h.get("symbol") or "").upper()
        if not symbol:
            continue
        aliases = symbols_meta.get(symbol, {}).get("aliases", []) or []
        norm_aliases = sorted(
            {
                a
                for a in [_normalize(symbol)] + [_normalize(x) for x in aliases]
                if a and len(a) >= 2
            }
        )
        out.append((symbol, norm_aliases))
    return out


def _fund_constituent_catalog(holdings: list[dict]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()
    held_symbols = {(h.get("symbol") or "").upper() for h in holdings if h.get("symbol")}
    for fund_symbol in sorted(held_symbols):
        members = ETF_CONSTITUENTS_REGISTRY.get(fund_symbol) or []
        for member in members:
            constituent_symbol = str(member.get("symbol") or "").upper()
            company = str(member.get("company") or "").strip()
            if not constituent_symbol:
                continue
            key = (fund_symbol, constituent_symbol)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            aliases = member.get("aliases") or []
            norm_aliases = sorted(
                {
                    a
                    for a in [_normalize(constituent_symbol), _normalize(company)] + [_normalize(x) for x in aliases]
                    if a and len(a) >= 2
                }
            )
            out.append(
                {
                    "fund_symbol": fund_symbol,
                    "constituent_symbol": constituent_symbol,
                    "company": company,
                    "aliases": norm_aliases,
                }
            )
    return out


def _match_fund_constituents(title: str, entities: list[str], catalog: list[dict[str, Any]]) -> dict[str, list[dict[str, str]]]:
    if not catalog:
        return {}
    norm_text = _normalize(f"{title} {' '.join([str(x) for x in (entities or [])])}")
    if not norm_text:
        return {}
    out: dict[str, list[dict[str, str]]] = {}
    seen: set[tuple[str, str]] = set()
    for row in catalog:
        fund_symbol = str(row.get("fund_symbol") or "").upper()
        constituent_symbol = str(row.get("constituent_symbol") or "").upper()
        company = str(row.get("company") or "").strip()
        aliases = row.get("aliases") or []
        if not fund_symbol or not constituent_symbol:
            continue
        matched = False
        for alias in aliases:
            if len(alias) <= 3:
                if re.search(rf"\b{re.escape(alias)}\b", norm_text):
                    matched = True
                    break
            elif alias in norm_text:
                matched = True
                break
        if not matched:
            continue
        key = (fund_symbol, constituent_symbol)
        if key in seen:
            continue
        seen.add(key)
        out.setdefault(fund_symbol, []).append({"symbol": constituent_symbol, "company": company})
    return out


def _match_symbols(title: str, alias_pairs: list[tuple[str, list[str]]]) -> list[str]:
    norm = _normalize(title)
    matched: list[str] = []
    for symbol, aliases in alias_pairs:
        for alias in aliases:
            if len(alias) <= 3:
                if re.search(rf"\b{re.escape(alias)}\b", norm):
                    matched.append(symbol)
                    break
            elif alias in norm:
                matched.append(symbol)
                break
    return sorted(set(matched))


def _horizon_days(timespan: str | None) -> int:
    span = (timespan or "").strip().lower()
    if span.endswith("h") and span[:-1].isdigit():
        hours = int(span[:-1])
        return max(1, int(math.ceil(hours / 24.0)))
    if span.endswith("d") and span[:-1].isdigit():
        return max(1, int(span[:-1]))
    return 7


def _company_in_text(company: str, ceo: str, title: str, entities: list[str]) -> bool:
    norm_title = _normalize(title)
    norm_company = _normalize(company)
    norm_ceo = _normalize(ceo)
    entity_text = " ".join([_normalize(x) for x in entities or []])
    if norm_company and norm_company in norm_title:
        return True
    if norm_ceo and norm_ceo in norm_title:
        return True
    if norm_company and norm_company in entity_text:
        return True
    if norm_ceo and norm_ceo in entity_text:
        return True
    return False


def build_announcement_tracker(
    top_news: list[NewsItem],
    local_news: list[NewsItem],
    holdings: list[dict],
    alias_map: dict,
    news_horizon: str,
) -> dict[str, Any]:
    monthly_window_days = 30
    horizon_days = max(monthly_window_days, _horizon_days(news_horizon))
    alias_pairs = _symbol_alias_pairs(alias_map, holdings)
    fund_catalog = _fund_constituent_catalog(holdings)
    symbols_meta = (alias_map or {}).get("symbols", {}) or {}
    symbol_to_sector = {
        (h.get("symbol") or "").upper(): (
            (symbols_meta.get((h.get("symbol") or "").upper(), {}) or {}).get("sector")
            or h.get("sector")
            or "UNKNOWN"
        )
        for h in holdings
        if h.get("symbol")
    }

    upcoming: list[dict[str, Any]] = []
    seen_upcoming: set[tuple[str, str]] = set()
    merged: list[NewsItem] = list(top_news or []) + list(local_news or [])
    for item in merged:
        title = item.title or ""
        if not title or not _is_announcement(title):
            continue
        direct_symbols = _match_symbols(title, alias_pairs)
        fund_constituent_hits = _match_fund_constituents(title, item.entities or [], fund_catalog)
        matched_symbols = sorted(set(direct_symbols).union(set(fund_constituent_hits.keys())))
        if not matched_symbols:
            continue
        eta_iso, eta_days = _extract_eta_iso(title, item.publishedAtISO)
        eta_estimated = False
        if eta_days is None:
            eta_iso, eta_days = _infer_eta_days_without_date(title, item.publishedAtISO)
            eta_estimated = eta_days is not None
        if eta_days is None:
            continue
        if eta_days < 0 or eta_days > monthly_window_days:
            continue
        for sym in matched_symbols:
            key = (sym, title.strip().lower())
            if key in seen_upcoming:
                continue
            seen_upcoming.add(key)
            related_constituents = fund_constituent_hits.get(sym) or []
            match_scope = "direct"
            if related_constituents and sym not in direct_symbols:
                match_scope = "fund_constituent"
            elif related_constituents:
                match_scope = "direct_and_fund"
            upcoming.append(
                {
                    "symbol": sym,
                    "sector": symbol_to_sector.get(sym, "UNKNOWN"),
                    "event_type": _announcement_type(title),
                    "eta_days": eta_days,
                    "eta_iso": eta_iso,
                    "eta_estimated": eta_estimated,
                    "match_scope": match_scope,
                    "related_constituents": related_constituents[:5],
                    "confidence": min(
                        95,
                        52
                        + (15 if eta_days <= 7 else 0)
                        + (8 if item.source in {"kap.org.tr", "reuters.com", "bloomberg.com"} else 0),
                    ),
                    "headline": title,
                    "source": item.source,
                    "url": item.url,
                }
            )

    upcoming.sort(key=lambda x: (x.get("eta_days", 9999), -int(x.get("confidence", 0))))
    upcoming = upcoming[:20]

    statement_candidates = [n for n in merged if n.title]
    ceo_statements: list[dict[str, Any]] = []
    seen_ceo: set[tuple[str, str, str]] = set()
    sector_symbols: dict[str, list[str]] = {}
    for sym, sector in symbol_to_sector.items():
        sector_symbols.setdefault(str(sector).upper(), []).append(sym)

    for item in statement_candidates:
        title = item.title or ""
        norm_title = _normalize(title)
        has_statement_hint = any(k in norm_title for k in _CEO_STATEMENT_HINTS) or bool(item.person_event)
        if not has_statement_hint:
            continue
        entities = item.entities or []
        for sector, leaders in FLAGSHIP_CEO_REGISTRY.items():
            for leader in leaders:
                company = leader.get("company") or ""
                ceo = leader.get("ceo") or ""
                if not _company_in_text(company, ceo, title, entities):
                    continue
                key = (sector, company, title.strip().lower())
                if key in seen_ceo:
                    continue
                seen_ceo.add(key)
                tone_raw = _headline_sentiment(title)
                stance = "neutral"
                if tone_raw > 0.05:
                    stance = "positive"
                elif tone_raw < -0.05:
                    stance = "negative"
                linked_symbols = sorted(sector_symbols.get(sector.upper(), []))
                ceo_statements.append(
                    {
                        "sector": sector,
                        "company": company,
                        "ceo": ceo,
                        "stance": stance,
                        "signal_score": round(0.35 + abs(tone_raw) * 0.35 + (0.15 if linked_symbols else 0.0), 3),
                        "linked_symbols": linked_symbols[:8],
                        "headline": title,
                        "source": item.source,
                        "publishedAtISO": item.publishedAtISO,
                        "url": item.url,
                    }
                )

    ceo_statements.sort(key=lambda x: (len(x.get("linked_symbols") or []), x.get("signal_score", 0.0)), reverse=True)
    ceo_statements = ceo_statements[:20]
    monthly_plan = _build_monthly_plan(upcoming, monthly_window_days)
    monthly_items = monthly_plan.get("items") or []
    by_event_type = monthly_plan.get("by_event_type") or []
    fund_constituent_events = [x for x in upcoming if str(x.get("match_scope") or "").startswith("fund_") or str(x.get("match_scope") or "") == "direct_and_fund"]

    return {
        "horizon_days": horizon_days,
        "portfolio_upcoming": upcoming,
        "sector_ceo_statements": ceo_statements,
        "monthly_plan": monthly_plan,
        "summary": {
            "upcoming_count": len(upcoming),
            "ceo_statement_count": len(ceo_statements),
            "monthly_plan_count": len(monthly_items),
            "monthly_event_types": [row.get("event_type") for row in by_event_type[:4] if row.get("event_type")],
            "fund_constituent_event_count": len(fund_constituent_events),
            "funds_with_constituent_signal": len({x.get("symbol") for x in fund_constituent_events if x.get("symbol")}),
            "symbols_with_upcoming": len({x.get("symbol") for x in upcoming if x.get("symbol")}),
            "sectors_with_ceo_signal": len({x.get("sector") for x in ceo_statements if x.get("sector")}),
        },
    }


def _pricing_state(pressure: float, prepricing_ratio: float) -> str:
    mag = abs(pressure)
    if mag < 0.08:
        return "NO_EDGE"
    if prepricing_ratio >= 1.5:
        return "PRICED_IN"
    if prepricing_ratio <= 0.65:
        return "UNDERPRICED"
    if prepricing_ratio >= 2.1:
        return "OVEREXTENDED"
    return "PARTIAL_PRICED"


def _extract_prepricing_ratio(evidence: Any) -> float:
    if not isinstance(evidence, dict):
        return 1.0
    reaction = evidence.get("reaction")
    if not isinstance(reaction, dict):
        return 1.0
    ratios: list[float] = []
    for value in reaction.values():
        if not isinstance(value, dict):
            continue
        pre = value.get("pre_30m_ret")
        post = value.get("post_30m_ret")
        try:
            pre_v = abs(float(pre or 0.0))
            post_v = abs(float(post or 0.0))
            if pre_v <= 0 and post_v <= 0:
                continue
            ratios.append(pre_v / max(0.0001, post_v))
        except Exception:
            continue
    if not ratios:
        return 1.0
    return sum(ratios) / len(ratios)


def compute_news_pricing_model(
    news_items: list[dict[str, Any]],
    holdings: list[dict[str, Any]],
    tracker: dict[str, Any],
) -> dict[str, Any]:
    if not news_items:
        return {
            "market_pressure_score": 0.0,
            "market_regime": "NEUTRAL",
            "coverage_quality": 0.0,
            "symbol_pricing": [],
            "top_positive": [],
            "top_negative": [],
        }

    weights = {str(h.get("symbol") or "").upper(): float(h.get("weight") or 0.0) for h in holdings if h.get("symbol")}
    upcoming_syms = {str(x.get("symbol") or "").upper() for x in (tracker.get("portfolio_upcoming") or []) if x.get("symbol")}
    ceo_linked_syms: set[str] = set()
    for row in tracker.get("sector_ceo_statements") or []:
        for sym in row.get("linked_symbols") or []:
            ceo_linked_syms.add(str(sym).upper())

    per_sym: dict[str, dict[str, Any]] = {}
    low_signal_count = 0
    for item in news_items:
        if item.get("low_signal"):
            low_signal_count += 1
        by_symbol = item.get("impact_by_symbol") or {}
        evidence = item.get("evidence")
        prepricing_ratio = _extract_prepricing_ratio(evidence)
        for symbol, score in by_symbol.items():
            sym = str(symbol).upper()
            if not sym:
                continue
            try:
                impact = float(score or 0.0)
            except Exception:
                impact = 0.0
            bucket = per_sym.setdefault(
                sym,
                {
                    "raw": 0.0,
                    "count": 0,
                    "prepricing_ratios": [],
                    "direct_sum": 0.0,
                    "indirect_sum": 0.0,
                },
            )
            direct = float((item.get("impact_by_symbol_direct") or {}).get(sym, 0.0) or 0.0)
            indirect = float((item.get("impact_by_symbol_indirect") or {}).get(sym, 0.0) or 0.0)
            schedule_boost = 1.15 if sym in upcoming_syms else 1.0
            ceo_boost = 1.10 if sym in ceo_linked_syms else 1.0
            crowding_penalty = 0.80 if prepricing_ratio >= 1.5 else 1.0
            residual = impact * schedule_boost * ceo_boost * crowding_penalty
            bucket["raw"] += residual
            bucket["count"] += 1
            bucket["direct_sum"] += direct
            bucket["indirect_sum"] += indirect
            bucket["prepricing_ratios"].append(prepricing_ratio)

    symbol_pricing: list[dict[str, Any]] = []
    for sym, row in per_sym.items():
        raw = float(row.get("raw", 0.0))
        count = int(row.get("count", 0))
        pressure = math.tanh(raw * 1.85)
        ratio_list = row.get("prepricing_ratios") or []
        avg_ratio = sum(ratio_list) / max(1, len(ratio_list))
        state = _pricing_state(pressure, avg_ratio)
        symbol_pricing.append(
            {
                "symbol": sym,
                "pressure_score": round(pressure, 4),
                "state": state,
                "expected_move_pct_24h": round(pressure * (0.7 + min(0.3, count * 0.07)), 4),
                "confidence": min(95, 45 + int(abs(pressure) * 35) + min(15, count * 4)),
                "evidence_count": count,
                "prepricing_ratio": round(avg_ratio, 3),
                "direct_component": round(float(row.get("direct_sum") or 0.0), 4),
                "indirect_component": round(float(row.get("indirect_sum") or 0.0), 4),
            }
        )

    symbol_pricing.sort(key=lambda x: abs(float(x.get("pressure_score") or 0.0)), reverse=True)
    top_positive = [x for x in symbol_pricing if float(x.get("pressure_score") or 0.0) > 0][:4]
    top_negative = [x for x in symbol_pricing if float(x.get("pressure_score") or 0.0) < 0][:4]

    if symbol_pricing:
        weighted = 0.0
        used_weight = 0.0
        for row in symbol_pricing:
            sym = str(row.get("symbol") or "").upper()
            w = float(weights.get(sym, 0.0))
            if w > 0:
                weighted += w * float(row.get("pressure_score") or 0.0)
                used_weight += w
        if used_weight > 0:
            market_score = weighted / used_weight
        else:
            market_score = sum(float(x.get("pressure_score") or 0.0) for x in symbol_pricing) / len(symbol_pricing)
    else:
        market_score = 0.0

    regime = "NEUTRAL"
    if market_score >= 0.12:
        regime = "BULLISH_PRICING"
    elif market_score <= -0.12:
        regime = "BEARISH_PRICING"

    coverage_quality = max(0.0, 1.0 - (low_signal_count / max(1, len(news_items))))
    return {
        "market_pressure_score": round(market_score, 4),
        "market_regime": regime,
        "coverage_quality": round(coverage_quality, 4),
        "symbol_pricing": symbol_pricing[:12],
        "top_positive": top_positive,
        "top_negative": top_negative,
    }
