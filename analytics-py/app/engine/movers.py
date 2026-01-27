from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

from app.engine.news_engine import WATCHLIST_BY_CATEGORY, detect_ticker_alias, format_ts_tsi
from app.engine.sector_config import SECTOR_GIANTS_REGISTRY
from app.models import (
    CryptoOutlook,
    DailyEquityMoverEvidence,
    DailyEquityMoverItem,
    DailyEquityMovers,
    DailyEquityMoversDebug,
    EventFeed,
    ForecastPanel,
    MarketSnapshot,
)


MONTHS_TR = {
    "Ocak": 1,
    "Subat": 2,
    "Şubat": 2,
    "Şubat": 2,
    "Mart": 3,
    "Nisan": 4,
    "Mayis": 5,
    "Mayıs": 5,
    "Haziran": 6,
    "Temmuz": 7,
    "Agustos": 8,
    "Ağustos": 8,
    "Eylul": 9,
    "Eylül": 9,
    "Ekim": 10,
    "Kasim": 11,
    "Kasım": 11,
    "Aralik": 12,
    "Aralık": 12,
}


CATALYST_RULES: List[Tuple[str, int, List[str]]] = [
    ("REGULATORY", 12, ["regulation", "regulatory", "sanction", "export control", "tariff", "ban"]),
    ("EARNINGS", 10, ["earnings", "guidance", "results", "forecast", "outlook"]),
    ("M&A", 8, ["merger", "acquire", "acquisition", "deal", "buyout"]),
    ("ETF_FLOW", 6, ["flow", "inflow", "outflow", "etf"]),
    ("CYBER", 9, ["hack", "breach", "exploit", "ransomware"]),
]

CATA_TEXT = {
    "REGULATORY": "regulasyon",
    "EARNINGS": "bilanco",
    "M&A": "m&a",
    "ETF_FLOW": "fon akisi",
    "CYBER": "siber risk",
}

DIRECTION_POS_WORDS = ["approval", "approved", "beat", "beats", "contract", "order", "inflow", "upgrade"]
DIRECTION_NEG_WORDS = ["ban", "sanction", "cut", "breach", "hack", "exploit", "outflow", "lawsuit", "fine"]


def _parse_ts_tsi(ts: str | None) -> datetime | None:
    if not ts:
        return None
    cleaned = ts.replace(" TSİ", "").replace(" TSI", "").strip()
    if not cleaned:
        return None
    try:
        date_part, time_part = cleaned.split(",")
        day_str, month_name, year_str = date_part.strip().split(" ")
        hour_str, minute_str = time_part.strip().split(":")
        month = MONTHS_TR.get(month_name)
        if not month:
            return None
        dt = datetime(
            int(year_str),
            month,
            int(day_str),
            int(hour_str),
            int(minute_str),
            tzinfo=ZoneInfo("Europe/Istanbul"),
        )
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _recency_weight(dt: datetime | None, now: datetime) -> float:
    if not dt:
        return 0.6
    hours = (now - dt).total_seconds() / 3600.0
    if hours <= 1:
        return 1.0
    if hours <= 6:
        return 0.8
    if hours <= 24:
        return 0.6
    return 0.4


def _is_us_ticker(ticker: str | None) -> bool:
    if not ticker:
        return False
    if not ticker.isalpha():
        return False
    return ticker.isupper() and 1 <= len(ticker) <= 5


def _build_company_registry() -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    name_to_ticker: Dict[str, str] = {}
    alias_to_ticker: Dict[str, str] = {}
    ticker_to_name: Dict[str, str] = {}
    for entries in WATCHLIST_BY_CATEGORY.values():
        for name, aliases in entries:
            ticker = detect_ticker_alias(aliases)
            if not ticker:
                continue
            name_to_ticker[name] = ticker
            for alias in aliases:
                alias_to_ticker[alias.lower()] = ticker
            if _is_us_ticker(ticker):
                ticker_to_name[ticker] = name
    return name_to_ticker, alias_to_ticker, ticker_to_name


NAME_TO_TICKER, ALIAS_TO_TICKER, TICKER_TO_NAME = _build_company_registry()


def _build_sector_seed_tickers() -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for sector_name, names in SECTOR_GIANTS_REGISTRY.items():
        tickers: List[str] = []
        for name in names:
            ticker = NAME_TO_TICKER.get(name) or ALIAS_TO_TICKER.get(name.lower())
            if _is_us_ticker(ticker):
                tickers.append(ticker)
            if len(tickers) >= 2:
                break
        if tickers:
            out[sector_name] = tickers
    return out


SECTOR_SEEDS_US = _build_sector_seed_tickers()


def _extract_catalysts(text: str) -> List[Tuple[str, int]]:
    hits: List[Tuple[str, int]] = []
    for label, weight, keywords in CATALYST_RULES:
        if any(k in text for k in keywords):
            hits.append((label, weight))
    return hits


def _direction_from_text(text: str) -> int:
    if any(k in text for k in DIRECTION_NEG_WORDS):
        return -1
    if any(k in text for k in DIRECTION_POS_WORDS):
        return 1
    return 0


def _choose_prepricing(tags: List[str]) -> str:
    order = {"PREPRICED_HIGH": 3, "PREPRICED_PARTIAL": 2, "UNPRICED": 1, "UNKNOWN": 0}
    best = "UNKNOWN"
    best_score = 0
    for tag in tags:
        score = order.get(tag or "UNKNOWN", 0)
        if score > best_score:
            best_score = score
            best = tag
    return best or "UNKNOWN"


def _format_why(direction: str, catalysts: List[str]) -> str:
    cause = "haber akisi"
    if catalysts:
        labels = [CATA_TEXT.get(c, c.lower()) for c in catalysts[:2]]
        cause = ", ".join(labels)
    if direction == "UP":
        return f"{cause} kaynakli pozitif sinyaller, kisa vadede yukari oynaklik ihtimalini artiriyor."
    if direction == "DOWN":
        return f"{cause} kaynakli negatif sinyaller, kisa vadede asagi oynaklik ihtimalini artiriyor."
    return f"{cause} etkisi karisik; kisa vadede yon belirsiz kalabilir."


def build_daily_equity_movers(event_feed: EventFeed) -> DailyEquityMovers:
    now = datetime.now(timezone.utc)
    candidates_seen = 0
    dropped_non_us = 0

    groups: Dict[str, dict] = {}
    all_events = [
        *(event_feed.regional or []),
        *(event_feed.company or []),
        *(event_feed.sector or []),
        *(event_feed.personal or []),
    ]

    for event in all_events:
        tickers: List[str] = []
        for asset in event.impacted_assets or []:
            if asset.asset_type != "equity":
                continue
            symbol = (asset.symbol_or_id or "").strip()
            ticker = None
            if _is_us_ticker(symbol):
                ticker = symbol
            else:
                ticker = ALIAS_TO_TICKER.get(symbol.lower()) or NAME_TO_TICKER.get(symbol)
            if _is_us_ticker(ticker):
                tickers.append(ticker)
            else:
                dropped_non_us += 1

        if not tickers and event.event_category == "SECTOR":
            seeds = SECTOR_SEEDS_US.get(event.sector_name or "")
            if seeds:
                tickers.extend(seeds)

        if not tickers:
            continue

        for ticker in dict.fromkeys(tickers):
            candidates_seen += 1
            bucket = groups.setdefault(
                ticker,
                {
                    "events": [],
                    "catalysts": {},
                    "direction_score": 0,
                    "sector": None,
                    "prepricing": [],
                },
            )
            bucket["events"].append(event)
            if not bucket["sector"] and event.sector_name and event.sector_name != "UNKNOWN":
                bucket["sector"] = event.sector_name
            bucket["prepricing"].append(event.prepricing_tag or "UNKNOWN")

            text = f"{event.title or ''} {event.short_summary or ''}".lower()
            for label, weight in _extract_catalysts(text):
                bucket["catalysts"][label] = max(weight, bucket["catalysts"].get(label, 0))
            if event.impact_label == "POS":
                bucket["direction_score"] += 1
            elif event.impact_label == "NEG":
                bucket["direction_score"] -= 1
            else:
                bucket["direction_score"] += _direction_from_text(text)

    items: List[DailyEquityMoverItem] = []
    for ticker, info in groups.items():
        events = info["events"]
        if not events:
            continue

        score_sum = 0.0
        max_quality = 0
        max_relevance = 0
        max_overall = 0
        pricing_ok = False
        for event in events:
            dt = _parse_ts_tsi(event.ts)
            weight = _recency_weight(dt, now)
            strength = 0.55 * event.relevance_score + 0.45 * event.quality_score
            score_sum += strength * weight
            max_quality = max(max_quality, event.quality_score)
            max_relevance = max(max_relevance, event.relevance_score)
            max_overall = max(max_overall, event.overall_confidence)
            if event.market_reaction:
                pricing_ok = True

        catalyst_items = sorted(info["catalysts"].items(), key=lambda x: -x[1])
        catalyst_boost = sum(weight for _, weight in catalyst_items[:2])
        prepricing_tag = _choose_prepricing(info["prepricing"])
        prepricing_penalty = -10 if prepricing_tag == "PREPRICED_HIGH" else -5 if prepricing_tag == "PREPRICED_PARTIAL" else 0

        raw_score = score_sum + catalyst_boost + prepricing_penalty
        move_score = int(max(0, min(100, round(raw_score))))
        confidence = int(max(0, min(100, max_overall or (max_quality + max_relevance) // 2)))
        expected_move_band_pct = round(1.0 + (move_score / 100.0) * 3.0, 1)

        direction = "NEUTRAL"
        if info["direction_score"] > 0:
            direction = "UP"
        elif info["direction_score"] < 0:
            direction = "DOWN"

        evidence = [
            DailyEquityMoverEvidence(
                event_id=e.dedup_cluster_id,
                category=e.event_category,
                relevance=e.relevance_score,
                quality=e.quality_score,
            )
            for e in sorted(events, key=lambda ev: (-(ev.relevance_score + ev.quality_score), ev.ts or ""))
        ][:3]

        catalysts = [label for label, _ in catalyst_items[:2]]
        pricing_status = "OK" if pricing_ok else ("MISSING" if events else "DISABLED")
        company_name = TICKER_TO_NAME.get(ticker) or ticker

        items.append(
            DailyEquityMoverItem(
                ticker=ticker,
                company_name=company_name,
                sector=info.get("sector"),
                expected_direction=direction,
                expected_move_band_pct=expected_move_band_pct,
                move_score=move_score,
                confidence=confidence,
                why=_format_why(direction, catalysts),
                catalysts=catalysts,
                evidence=evidence,
                prepricing_tag=prepricing_tag,
                pricing_status=pricing_status,
            )
        )

    items = sorted(items, key=lambda i: (-i.move_score, -i.confidence, i.ticker))[:3]
    debug = DailyEquityMoversDebug(
        candidates_seen=candidates_seen,
        dropped_non_us=dropped_non_us,
        reason_if_empty=None,
    )
    if not items:
        reason = "NO_US_TICKERS" if candidates_seen == 0 else "DATA_LOW_SIGNAL"
        debug.reason_if_empty = reason

    return DailyEquityMovers(
        asof=format_ts_tsi(now),
        items=items,
        debug=debug,
    )


def build_crypto_outlook(market: MarketSnapshot, event_feed: EventFeed | None = None) -> ForecastPanel:
    deltas = (market.coingecko.deltas or {}) if market and market.coingecko else {}
    btc_d = deltas.get("btc_d")
    usdt_d = deltas.get("usdt_d")
    usdc_d = deltas.get("usdc_d")
    total_vol = deltas.get("total_vol")
    dxy_change = market.yahoo.dxy_chg_24h if market and market.yahoo else None

    stable_dom = None
    if usdt_d is not None and usdc_d is not None:
        stable_dom = usdt_d + usdc_d

    drivers: List[str] = []
    watch_metrics: List[str] = []
    btc_bias = 0
    eth_bias = 0

    if stable_dom is not None and total_vol is not None:
        if stable_dom < 0 and total_vol > 0:
            btc_bias += 1
            eth_bias += 1
            drivers.append("Stablecoin dominansi dusuyor, toplam hacim artiyor (risk-on).")
            watch_metrics.extend(["USDT.D+USDC.D", "Total Vol"])

    if dxy_change is not None and dxy_change > 0.5:
        btc_bias -= 1
        eth_bias -= 1
        drivers.append("DXY yukseliyor, riskli varliklarda baski olasiligi artiyor.")
        watch_metrics.append("DXY")

    if btc_d is not None:
        if btc_d > 0.2:
            btc_bias += 1
            eth_bias -= 1
            drivers.append("BTC dominansi artiyor (BTC lehine denge).")
            watch_metrics.append("BTC.D")
        elif btc_d < -0.2:
            btc_bias -= 1
            eth_bias += 1
            drivers.append("BTC dominansi geriliyor (altlara kayma).")
            watch_metrics.append("BTC.D")

    btc_bias = max(-2, min(2, btc_bias))
    eth_bias = max(-2, min(2, eth_bias))

    missing = 0
    if stable_dom is None or total_vol is None:
        missing += 1
    if dxy_change is None:
        missing += 1
    if btc_d is None:
        missing += 1

    confidence = 75
    if missing == 1:
        confidence = 55
    elif missing >= 2:
        confidence = 40

    if not drivers:
        drivers.append("Net bir sinyal yok; piyasa metrikleri dengeli.")

    metrics = []
    for m in watch_metrics:
        if m not in metrics:
            metrics.append(m)
        if len(metrics) >= 3:
            break
    if len(metrics) < 3:
        for m in ["BTC.D", "USDT.D+USDC.D", "Total Vol", "DXY"]:
            if m not in metrics:
                metrics.append(m)
            if len(metrics) >= 3:
                break

    outlook = CryptoOutlook(
        asof=format_ts_tsi(datetime.now(timezone.utc)),
        btc_bias=btc_bias,
        eth_bias=eth_bias,
        confidence=confidence,
        drivers=drivers[:3],
        watch_metrics=metrics[:3],
    )
    return ForecastPanel(crypto_outlook=outlook)
