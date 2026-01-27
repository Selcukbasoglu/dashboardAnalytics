from __future__ import annotations

import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List

try:
    from app.engine.sector_config import SECTOR_GIANTS_REGISTRY as _SECTOR_GIANTS_REGISTRY
    from app.engine.sector_config import SECTOR_RULES as _SECTOR_RULES
except Exception:
    _SECTOR_GIANTS_REGISTRY = {}
    _SECTOR_RULES = {}


FALLBACK_SECTOR_RULES = {
    "SEMICONDUCTORS": {"required": ["chip", "semiconductor"], "boost": ["ai", "datacenter"], "exclude": []},
    "OIL_GAS_UPSTREAM": {"required": ["oil", "gas"], "boost": ["opec", "brent", "wti"], "exclude": []},
    "LNG_NATGAS": {"required": ["lng", "natural gas"], "boost": ["pipeline", "liquefaction"], "exclude": []},
    "POWER_UTILITIES": {"required": ["utility", "power grid"], "boost": ["renewable", "nuclear"], "exclude": []},
    "BANKS_RATES": {"required": ["bank", "lender"], "boost": ["rates", "interest"], "exclude": []},
    "SHIPPING_LOGISTICS": {"required": ["shipping", "logistics"], "boost": ["supply chain"], "exclude": []},
    "DEFENSE_AEROSPACE": {"required": ["defense", "aerospace"], "boost": ["military"], "exclude": []},
}


MACRO_KEYWORDS = [
    "cpi",
    "pce",
    "gdp",
    "pmi",
    "payroll",
    "inflation",
    "rate hike",
    "rate cut",
    "yields",
    "jobless",
    "unemployment",
]

GEOPOLITICS_KEYWORDS = [
    "sanctions",
    "tariff",
    "export controls",
    "conflict",
    "ceasefire",
    "shipping lane",
    "attack",
    "missile",
]

SYSTEMIC_KEYWORDS = [
    "global",
    "risk-off",
    "systemic",
    "contagion",
    "crisis",
]

COMPANY_EVENT_TYPES = {
    "EARNINGS_GUIDANCE",
    "MNA",
    "CAPEX_INVESTMENT",
    "PRODUCT_PLATFORM",
}

SECTOR_EVENT_TYPES = {
    "REGULATION_LEGAL",
    "CRYPTO_MARKET_STRUCTURE",
    "SECURITY_INCIDENT",
}

AMBIGUITY_WORDS = ["may", "could", "might", "reportedly", "sources", "rumor", "rumour", "likely", "possible"]

NUMERIC_SHOCK_RE = re.compile(
    r"\b\d+(?:\.\d+)?\s?(?:%|bp|bps|bpd|mbpd|million|billion|trillion)\b",
    flags=re.IGNORECASE,
)

POS_TRIGGERS = [
    "approval",
    "framework",
    "deal",
    "ceasefire",
    "rate cut",
    "easing",
    "supply increase",
    "increase output",
    "restore supply",
]

NEG_TRIGGERS = [
    "ban",
    "lawsuit",
    "enforcement",
    "sanctions",
    "tariff",
    "export controls",
    "attack",
    "disruption",
    "rate hike",
    "outage",
]

REGION_HINTS = [
    "united states",
    "u.s.",
    "usa",
    "europe",
    "eu",
    "china",
    "russia",
    "iran",
    "israel",
    "saudi",
    "germany",
    "france",
    "japan",
]


def _get(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def _set(item: Any, key: str, value: Any) -> None:
    if isinstance(item, dict):
        item[key] = value
    else:
        setattr(item, key, value)


def _combined_text(item: Any) -> str:
    title = str(_get(item, "title") or "")
    desc = str(_get(item, "description") or _get(item, "snippet") or _get(item, "summary") or "")
    return f"{title} {desc}".strip()


def _parse_any_date(value: Any) -> datetime | None:
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    try:
        if value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return datetime.fromisoformat(value)
    except Exception:
        try:
            dt = parsedate_to_datetime(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None


def _numeric_shock(text: str) -> bool:
    return bool(NUMERIC_SHOCK_RE.search(text))


def _ambiguity_penalty(text: str) -> int:
    hits = sum(1 for w in AMBIGUITY_WORDS if re.search(rf"\b{re.escape(w)}\b", text))
    return min(15, hits * 5)


def _entity_count(item: Any) -> int:
    entities = _get(item, "entities") or []
    return len(entities)


def _region_count(text: str, tags: list[str]) -> int:
    count = 0
    found = set()
    for hint in REGION_HINTS:
        if hint in text:
            found.add(hint)
    for t in tags:
        if t in {"AB", "ABD", "Asya", "Ortadogu"}:
            found.add(t)
    count = len(found)
    return count


def _effective_sector_rules() -> Dict[str, dict]:
    rules = dict(_SECTOR_RULES) if _SECTOR_RULES else dict(FALLBACK_SECTOR_RULES)
    if "CRYPTO" not in rules:
        rules["CRYPTO"] = {
            "required": ["crypto", "bitcoin", "stablecoin", "exchange", "etf", "defi"],
            "boost": ["approval", "inflow", "outflow", "custody"],
            "exclude": [],
        }
    return rules


def _effective_giants() -> Dict[str, List[str]]:
    giants = dict(_SECTOR_GIANTS_REGISTRY) if _SECTOR_GIANTS_REGISTRY else {}
    if "CRYPTO" not in giants:
        try:
            from app.engine.news_engine import CRYPTO_WATCHLIST
        except Exception:
            CRYPTO_WATCHLIST = []
        giants["CRYPTO"] = [name for name, _ in CRYPTO_WATCHLIST][:8]
    return giants


def infer_news_scope(item: Any) -> dict:
    title = str(_get(item, "title") or "")
    text = _combined_text(item).lower()
    event_type = str(_get(item, "event_type") or "")
    tags = list(_get(item, "tags") or [])
    entity_count = _entity_count(item)
    region_count = _region_count(text, tags)
    numeric_shock = _numeric_shock(text)
    ambiguity_penalty = _ambiguity_penalty(text)

    signals: List[str] = []
    is_macro = event_type == "MACRO_RATES_INFLATION" or any(k in text for k in MACRO_KEYWORDS)
    if is_macro:
        signals.append("macro_keywords")
    is_geo = event_type in {"SANCTIONS_GEOPOLITICS", "ENERGY_SUPPLY_OPEC"} or "War" in tags or any(
        k in text for k in GEOPOLITICS_KEYWORDS
    )
    if is_geo:
        signals.append("geopolitics_keywords")
    is_company = event_type in COMPANY_EVENT_TYPES or (entity_count == 1 and any(k in text for k in ["earnings", "guidance", "merger", "acquisition", "capex", "launch"]))
    if is_company:
        signals.append("company_focus")
    is_sector = event_type in SECTOR_EVENT_TYPES or (entity_count >= 2 and any(k in text for k in ["industry", "sector", "market structure", "regulation"]))
    if is_sector:
        signals.append("sector_focus")
    systemic_hint = any(k in text for k in SYSTEMIC_KEYWORDS)
    if systemic_hint:
        signals.append("systemic_keyword")

    news_scope = "UNKNOWN"
    if is_macro:
        news_scope = "MACRO"
    elif is_geo:
        news_scope = "GEOPOLITICS"
    elif is_company:
        news_scope = "COMPANY"
    elif is_sector:
        news_scope = "SECTOR"

    scope_score = 10
    if news_scope in {"MACRO", "GEOPOLITICS"}:
        scope_score += 20
    scope_score += min(20, 5 * region_count)
    scope_score += min(20, 4 * min(5, entity_count))
    if numeric_shock:
        scope_score += 10
        signals.append("numeric_shock")
    scope_score -= ambiguity_penalty

    if systemic_hint or (region_count >= 2 and news_scope in {"MACRO", "GEOPOLITICS"}) or scope_score >= 75:
        news_scope = "SYSTEMIC"
        scope_score += 25
        signals.append("systemic_signal")

    if news_scope == "SYSTEMIC" and scope_score < 50:
        scope_score = 50

    scope_score = max(0, min(100, int(round(scope_score))))
    return {"news_scope": news_scope, "scope_score": scope_score, "scope_signals": signals}


def infer_sector_impacts(item: Any, max_sectors: int = 5) -> List[dict]:
    title = str(_get(item, "title") or "")
    text = _combined_text(item).lower()
    entities = [str(e) for e in (_get(item, "entities") or [])]
    rules = _effective_sector_rules()
    giants = _effective_giants()
    numeric_shock = _numeric_shock(text)
    ambiguity_penalty = _ambiguity_penalty(text)

    impacts: List[dict] = []
    for sector, cfg in rules.items():
        required = [k.lower() for k in cfg.get("required", [])]
        boost = [k.lower() for k in cfg.get("boost", [])]
        exclude = [k.lower() for k in cfg.get("exclude", [])]

        required_hits = sum(1 for k in required if k in text)
        boost_hits = sum(1 for k in boost if k in text)
        exclude_hits = sum(1 for k in exclude if k in text)
        giant_hits = 0
        for name in giants.get(sector, []):
            if name.lower() in text:
                giant_hits += 1
            elif name in entities:
                giant_hits += 1

        if sector == "BANKS_RATES" and any(k in text for k in MACRO_KEYWORDS):
            required_hits = max(required_hits, 1)

        if required_hits == 0 and giant_hits == 0:
            continue
        if exclude_hits and required_hits == 0:
            continue

        impact_score = 20
        if required_hits:
            impact_score += 20
        impact_score += min(15, 7 * required_hits)
        impact_score += min(15, 5 * giant_hits)
        impact_score += min(12, 4 * boost_hits)
        if numeric_shock:
            impact_score += 10
        impact_score -= ambiguity_penalty

        confidence = 30
        confidence += min(30, 6 * required_hits + 4 * boost_hits)
        confidence += min(20, 6 * giant_hits)
        confidence -= ambiguity_penalty

        pos_hits = sum(1 for k in POS_TRIGGERS if k in text)
        neg_hits = sum(1 for k in NEG_TRIGGERS if k in text)

        direction = "NEUTRAL"
        if pos_hits and neg_hits and max(pos_hits, neg_hits) >= 2:
            direction = "MIXED"
        elif pos_hits > neg_hits:
            direction = "UP"
        elif neg_hits > pos_hits:
            direction = "DOWN"

        if sector in {"OIL_GAS_UPSTREAM", "LNG_NATGAS"}:
            if any(k in text for k in ["output cut", "cut output", "disruption", "attack", "sanctions"]):
                direction = "UP"
            if any(k in text for k in ["increase output", "restore supply"]):
                direction = "DOWN"
        if sector == "SEMICONDUCTORS":
            if any(k in text for k in ["export controls", "tariff", "ban"]):
                direction = "DOWN"
            if any(k in text for k in ["datacenter demand", "capex", "ai demand", "hbm"]):
                direction = "UP"
        if sector == "DEFENSE_AEROSPACE":
            if "ceasefire" in text:
                direction = "DOWN"

        impact_score = max(0, min(100, int(round(impact_score))))
        confidence = max(0, min(100, int(round(confidence))))
        rationale = f"required:{required_hits} boost:{boost_hits} giants:{giant_hits}"
        impacts.append(
            {
                "sector": sector,
                "direction": direction,
                "confidence": confidence,
                "rationale": rationale,
                "impact_score": impact_score,
            }
        )

    impacts.sort(key=lambda x: x["impact_score"], reverse=True)
    return impacts[:max_sectors]


def attach_scope_and_sectors(item: Any, max_sectors: int = 5) -> None:
    scope = infer_news_scope(item)
    impacts = infer_sector_impacts(item, max_sectors=max_sectors)
    max_impact = max([i["impact_score"] for i in impacts], default=0)
    summary = ", ".join([f"{i['sector']}:{i['direction']}" for i in impacts[:2]])

    _set(item, "news_scope", scope["news_scope"])
    _set(item, "scope_score", scope["scope_score"])
    _set(item, "scope_signals", scope["scope_signals"])
    _set(item, "sector_impacts", impacts)
    _set(item, "max_sector_impact", max_impact)
    _set(item, "sector_summary", summary)
