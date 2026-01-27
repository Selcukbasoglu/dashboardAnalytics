from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Iterable

from app.config import Settings
from app.engine.news_engine import canonical_title, parse_any_date
from app.engine.sector_impact import attach_scope_and_sectors
from app.infra.db import DB
from app.models import NewsItem


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _source_tier(domain: str, tiers: dict) -> str:
    domain = (domain or "").lower()
    for key in ("primary", "tier1", "tier2", "social"):
        if domain in set(tiers.get(key, [])):
            return key
    return "tier2"


def _tier_score(tier: str) -> float:
    return {
        "primary": 1.0,
        "tier1": 0.85,
        "tier2": 0.65,
        "social": 0.4,
    }.get(tier, 0.6)


def _severity_score(event_type: str | None, tags: list[str]) -> float:
    et = (event_type or "").upper()
    if et in {"WAR", "SANCTIONS", "GEO_RISK", "CEASEFIRE"}:
        return 0.9
    if et in {"REGULATION", "SEC", "CFTC"}:
        return 0.75
    if et in {"EARNINGS_GUIDANCE", "EARNINGS"}:
        return 0.6
    if "War" in tags or "Energy" in tags:
        return 0.8
    if "Reg" in tags:
        return 0.7
    if "ETF" in tags:
        return 0.55
    return 0.45


def _direction_from_text(text: str | None) -> int:
    if not text:
        return 0
    t = text.lower()
    if "yukari" in t or "pozitif" in t or "artis" in t:
        return 1
    if "asagi" in t or "negatif" in t or "dusus" in t:
        return -1
    return 0


def _relevance_targets(item: NewsItem) -> list[tuple[str, float]]:
    title = (item.title or "").lower()
    tags = item.tags or []
    event_type = (item.event_type or "").upper()
    entities = item.entities or []
    targets: dict[str, float] = {}
    news_scope = getattr(item, "news_scope", "") or ""
    scope_score = float(getattr(item, "scope_score", 0) or 0)
    max_sector = float(getattr(item, "max_sector_impact", 0) or 0)
    scale = 0.85 + 0.15 * max(scope_score / 100.0, max_sector / 100.0)

    def _add(asset: str, score: float):
        if asset in targets:
            targets[asset] = max(targets[asset], score)
        else:
            targets[asset] = score

    if "btc" in title or "bitcoin" in title or "BTC" in entities:
        _add("BTC", 0.9)
    if "eth" in title or "ethereum" in title or "ETH" in entities:
        _add("ETH", 0.9)
        _add("ALTS", 0.7)

    if "stablecoin" in title or "Stablecoin" in tags or "usdt" in title or "usdc" in title or "tether" in title or "circle" in title:
        _add("STABLES", 0.85)
        _add("ALTS", 0.7)

    if "etf" in title or "ETF" in tags:
        if "ethereum" in title or "eth" in title:
            _add("ETH", 0.9)
            _add("ALTS", 0.65)
        else:
            _add("BTC", 0.9)
            _add("ALTS", 0.55)

    if any(k in title for k in ["layer 2", "layer-2", "l2", "l1", "defi", "staking", "rollup", "bridge", "solana", "avalanche", "polygon", "arbitrum", "optimism"]):
        _add("ALTS", 0.8)
        _add("ETH", 0.65)

    if event_type in ("CRYPTO_MARKET_STRUCTURE", "REGULATION_LEGAL") or "Reg" in tags or any(
        k in title for k in ["exchange", "custody", "license", "regulation", "sec", "cftc", "lawsuit", "enforcement", "ban", "framework", "mica", "market structure"]
    ):
        _add("STABLES", 0.8)
        _add("ALTS", 0.7)
        _add("ETH", 0.6)
        _add("BTC", 0.5)

    if item.category in ("kripto", "crypto", "teknoloji", "tech"):
        _add("ALTS", 0.6)

    if news_scope in ("MACRO", "GEOPOLITICS", "SYSTEMIC"):
        _add("BTC", 0.55)
        _add("ALTS", 0.45)
        _add("ETH", 0.4)
        if news_scope in ("GEOPOLITICS", "SYSTEMIC"):
            _add("STABLES", 0.5)

    sector_impacts = getattr(item, "sector_impacts", []) or []
    top_sectors = [s.get("sector") for s in sector_impacts if isinstance(s, dict)]
    if "BANKS_RATES" in top_sectors:
        _add("BTC", 0.6)
        _add("ALTS", 0.45)
        _add("STABLES", 0.5)
    if any(s in top_sectors for s in ("OIL_GAS_UPSTREAM", "LNG_NATGAS", "DEFENSE_AEROSPACE", "SHIPPING_LOGISTICS")):
        _add("BTC", 0.45)
        _add("ALTS", 0.35)
        _add("STABLES", 0.55)

    defaults = {"BTC": 0.3, "ETH": 0.25, "ALTS": 0.3, "STABLES": 0.3}
    if news_scope in ("MACRO", "GEOPOLITICS", "SYSTEMIC"):
        defaults = {"BTC": 0.45, "ETH": 0.35, "ALTS": 0.35, "STABLES": 0.5}
    elif news_scope == "COMPANY":
        defaults = {"BTC": 0.2, "ETH": 0.2, "ALTS": 0.2, "STABLES": 0.2}
    for asset, score in defaults.items():
        if asset not in targets:
            targets[asset] = score

    out: list[tuple[str, float]] = []
    for asset, score in targets.items():
        out.append((asset, round(min(1.0, score * scale), 4)))
    return out


def _dedup_hash(item: NewsItem, domain: str) -> str:
    base = f"{canonical_title(item.title)}::{domain}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _event_id(item: NewsItem, domain: str) -> str:
    base = item.url or f"{canonical_title(item.title)}::{domain}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def store_events(db: DB, items: Iterable[NewsItem], settings: Settings) -> int:
    rows = []
    asset_rows = []
    for item in items:
        try:
            attach_scope_and_sectors(item)
        except Exception:
            pass
        domain = item.source or ""
        tier = _source_tier(domain, settings.source_tiers)
        other_sources = item.other_sources or []
        credibility = min(1.0, _tier_score(tier) + 0.05 * min(4, len(other_sources)))
        severity = _severity_score(item.event_type, item.tags or [])
        direction = _direction_from_text(item.expected_direction_short_term)
        tags = list(item.tags or [])
        news_scope = getattr(item, "news_scope", None)
        if news_scope:
            tags.append(f"Scope:{news_scope}")
        sector_impacts = getattr(item, "sector_impacts", []) or []
        for sec in sector_impacts[:2]:
            if isinstance(sec, dict) and sec.get("sector"):
                tags.append(f"Sector:{sec['sector']}")
        targets = _relevance_targets(item)
        impact = round(min(100.0, severity * credibility * 100.0), 2)

        dt = parse_any_date(item.publishedAtISO) or datetime.now(timezone.utc)
        ts_utc = dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        eid = _event_id(item, domain)
        rows.append(
            (
                eid,
                ts_utc,
                domain,
                tier,
                item.title,
                item.description or item.short_summary or "",
                item.url,
                json.dumps(tags),
                _dedup_hash(item, domain),
                item.dedup_cluster_id,
                credibility,
                severity,
                impact,
                item.event_type,
                item.category,
                direction,
            )
        )
        for asset, score in targets:
            asset_rows.append((eid, asset, score))
        if news_scope:
            asset_rows.append((eid, f"SCOPE:{news_scope}", round(float(getattr(item, "scope_score", 0) or 0) / 100.0, 4)))
        for sec in sector_impacts[:3]:
            if isinstance(sec, dict) and sec.get("sector"):
                asset_rows.append((eid, f"SECTOR:{sec['sector']}", round(float(sec.get("impact_score", 0)) / 100.0, 4)))

    db.insert_ignore(
        "events",
        [
            "event_id",
            "ts_utc",
            "source",
            "source_tier",
            "headline",
            "body",
            "url",
            "tags_json",
            "dedup_hash",
            "cluster_id",
            "credibility_score",
            "severity_score",
            "impact_score",
            "event_type",
            "category",
            "direction",
        ],
        rows,
    )
    db.insert_ignore(
        "event_asset_map",
        ["event_id", "asset_or_sector", "relevance_score"],
        asset_rows,
    )
    return len(rows)


def get_kv(db: DB, key: str) -> str | None:
    row = db.fetchone("SELECT value FROM kv_store WHERE key = ?", (key,))
    if row:
        return row["value"]
    return None


def set_kv(db: DB, key: str, value: str) -> None:
    db.upsert("kv_store", ["key", "value"], ["key"], [(key, value)])


def should_ingest(last_ts: str | None, interval_minutes: int) -> bool:
    if not last_ts:
        return True
    try:
        dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
    except Exception:
        return True
    return datetime.now(timezone.utc) - dt >= timedelta(minutes=interval_minutes)


def backfill_event_targets(db: DB, lookback_days: int = 7) -> int:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat().replace("+00:00", "Z")
    rows = db.fetchall(
        """
        SELECT event_id, ts_utc, headline, tags_json, event_type, category
        FROM events WHERE ts_utc >= ?
        """,
        (cutoff,),
    )
    inserts: list[tuple[str, str, float]] = []
    for row in rows:
        event_id = row["event_id"]
        existing_rows = db.fetchall(
            "SELECT asset_or_sector FROM event_asset_map WHERE event_id = ?",
            (event_id,),
        )
        existing = {r["asset_or_sector"] for r in existing_rows}
        missing_targets = [t for t in ("BTC", "ETH", "ALTS", "STABLES") if t not in existing]
        if not missing_targets:
            continue
        try:
            tags = json.loads(row["tags_json"] or "[]")
        except Exception:
            tags = []
        item = NewsItem(
            title=row["headline"] or "",
            url="",
            source="",
            tags=tags,
            event_type=row["event_type"] or "",
            category=row["category"] or "",
        )
        targets = _relevance_targets(item)
        for asset, score in targets:
            if asset in missing_targets:
                inserts.append((event_id, asset, score))
    if inserts:
        db.insert_ignore("event_asset_map", ["event_id", "asset_or_sector", "relevance_score"], inserts)
    return len(inserts)


def record_ingest(db: DB) -> None:
    set_kv(db, "news_ingest_at", _iso_now())
    set_kv(db, "news_last_scan_ts", _iso_now())


def last_scan_ts(db: DB) -> str | None:
    return get_kv(db, "news_last_scan_ts")
