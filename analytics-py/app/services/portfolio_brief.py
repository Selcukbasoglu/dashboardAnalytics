from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any


_MONTHS_TR = {
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


def _format_tsi(dt: datetime | None = None) -> str:
    dt = dt or datetime.now(timezone.utc)
    ts = dt.astimezone(timezone(timedelta(hours=3)))
    month = _MONTHS_TR.get(ts.month, str(ts.month))
    return f"{ts.day} {month} {ts.year}, {ts.strftime('%H:%M')} TSİ"


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def build_daily_brief(portfolio: dict, window: str, period: str, base: str) -> dict:
    holdings = portfolio.get("holdings") or []
    allocation = portfolio.get("allocation") or {}
    risk = portfolio.get("risk") or {}
    news_impact = portfolio.get("newsImpact") or {}
    recommendations = portfolio.get("recommendations") or []
    debug_notes = list(portfolio.get("debug_notes") or [])

    missing_data: list[str] = []
    price_missing = [h for h in holdings if h.get("data_status") == "missing"]
    if price_missing:
        missing_data.append("Fiyat verisi eksik: bazı holdings fiyatı yok.")

    coverage = news_impact.get("coverage") or {"matched": 0, "total": 0}
    items = news_impact.get("items") or []
    if coverage.get("total", 0) == 0 and not items:
        missing_data.append("NO_NEWS_ITEMS: cache miss + pipeline disabled or ingestion failed.")
        cache_note = next((n for n in debug_notes if n.startswith("portfolio_news_cache_hit=")), None)
        pipe_note = next((n for n in debug_notes if n.startswith("portfolio_pipeline_used=")), None)
        if cache_note:
            missing_data.append(cache_note)
        if pipe_note:
            missing_data.append(pipe_note)

    symbol_to_class = {h.get("symbol"): h.get("asset_class") for h in holdings}

    # direct match count
    direct_count = 0
    for item in items:
        for md in item.get("match_debug", []):
            if md.get("method") == "direct":
                direct_count += 1
                break
    coverage = {
        "matched": int(coverage.get("matched", 0) or 0),
        "total": int(coverage.get("total", 0) or 0),
        "direct": direct_count,
    }

    # news impact by asset class
    by_class = {"BIST": 0.0, "NASDAQ": 0.0, "CRYPTO": 0.0}
    summary = news_impact.get("summary") or {}
    impact_by_symbol = summary.get("impact_by_symbol") or {}
    for sym, val in impact_by_symbol.items():
        cls = symbol_to_class.get(sym)
        if cls in by_class:
            by_class[cls] += _safe_float(val)

    # top holdings by weight
    top_holdings = sorted(holdings, key=lambda h: _safe_float(h.get("weight")), reverse=True)[:5]
    top_holdings_out = [
        {
            "symbol": h.get("symbol"),
            "asset_class": h.get("asset_class"),
            "weight": _safe_float(h.get("weight")),
            "mkt_value_base": _safe_float(h.get("mkt_value_base")),
        }
        for h in top_holdings
    ]

    # top news drivers per symbol
    per_symbol_totals: dict[str, float] = {}
    per_symbol_top_item: dict[str, dict] = {}
    for item in items:
        for sym, impact in (item.get("impact_by_symbol") or {}).items():
            impact_val = _safe_float(impact)
            per_symbol_totals[sym] = per_symbol_totals.get(sym, 0.0) + impact_val
            existing = per_symbol_top_item.get(sym)
            if existing is None or abs(impact_val) > abs(_safe_float(existing.get("_impact"))):
                per_symbol_top_item[sym] = {
                    "headline": item.get("title"),
                    "event_type": item.get("event_type") or "UNKNOWN",
                    "impact_channel": item.get("impact_channel") or [],
                    "_impact": impact_val,
                }

    positives = sorted(
        [(sym, val) for sym, val in per_symbol_totals.items() if val > 0],
        key=lambda x: x[1],
        reverse=True,
    )[:3]
    negatives = sorted(
        [(sym, val) for sym, val in per_symbol_totals.items() if val < 0],
        key=lambda x: x[1],
    )[:3]

    def _driver_entry(sym: str, val: float) -> dict:
        meta = per_symbol_top_item.get(sym, {})
        return {
            "symbol": sym,
            "impact": _safe_float(val),
            "headline": meta.get("headline"),
            "event_type": meta.get("event_type") or "UNKNOWN",
            "impact_channel": meta.get("impact_channel") or [],
        }

    top_news_drivers = {
        "positive": [_driver_entry(sym, val) for sym, val in positives],
        "negative": [_driver_entry(sym, val) for sym, val in negatives],
    }

    # optimizer hints for requested period
    period = (period or "daily").lower()
    selected = next((r for r in recommendations if r.get("period") == period), None)
    actions = []
    optimizer_mode = "ACTIVE"
    optimizer_reason = None
    if selected:
        optimizer_mode = selected.get("mode") or "ACTIVE"
        optimizer_reason = selected.get("hold_reason")
        for act in (selected.get("actions") or [])[:5]:
            actions.append(
                {
                    "symbol": act.get("symbol"),
                    "action": act.get("action"),
                    "deltaWeight": _safe_float(act.get("deltaWeight")),
                    "why": act.get("reason") or [],
                    "confidence": int(act.get("confidence") or 0),
                }
            )

    turnover_caps = {"daily": 0.05, "weekly": 0.15, "monthly": 0.30}
    constraints = {
        "max_weight": 0.30,
        "max_crypto_weight": 0.20,
        "turnover_cap": turnover_caps.get(period, 0.05),
    }

    # portfolio mode heuristic
    crypto_share = _safe_float((allocation.get("by_asset_class") or {}).get("CRYPTO"))
    if crypto_share >= 0.35:
        mode = "risk_on"
    elif crypto_share <= 0.10:
        mode = "risk_off"
    else:
        mode = "mixed"

    max_holding = top_holdings_out[0] if top_holdings_out else None
    news_line = f"Haber etkisi: {coverage.get('matched', 0)}/{coverage.get('total', 0)} eşleşme"
    if positives:
        sym, val = positives[0]
        news_line += f", pozitif: {sym} ({val:+.2f})"
    if negatives:
        sym, val = negatives[0]
        news_line += f", negatif: {sym} ({val:+.2f})"

    executive_summary = [
        f"Portföy modu: {mode}.",
        f"En büyük ağırlık: {max_holding['symbol']} (%{max_holding['weight']*100:.1f})." if max_holding else "En büyük ağırlık: —.",
        f"Risk: HHI={_safe_float(risk.get('hhi')):.3f}, max_weight={_safe_float(risk.get('max_weight')):.2f}, vol30d={_safe_float(risk.get('vol_30d')):.3f}.",
        news_line + ".",
        f"Kısıtlar: max_weight={constraints['max_weight']:.2f}, turnover_cap={constraints['turnover_cap']:.2f}.",
    ]
    fx_exposure = _safe_float(risk.get("fx_usd_exposure"))
    if fx_exposure >= 0.50:
        executive_summary.insert(3, f"FX hassasiyeti: USD ağırlığı %{fx_exposure*100:.1f}.")
    if optimizer_mode == "HOLD":
        reason = optimizer_reason or "LOW_COVERAGE_OR_LOW_SIGNAL"
        executive_summary.insert(0, f"Sinyal zayıf ({reason}) → işlem önerisi üretilmedi.")

    data_status = "ok" if not missing_data else "partial"

    debug_notes.extend(
        [
            "daily_brief_source=deterministic",
            f"daily_brief_window={window}",
            f"daily_brief_period={period}",
            f"newsImpact_items={len(items)}",
            "newsImpact_empty=true" if not items else "newsImpact_empty=false",
        ]
    )

    return {
        "generatedAtTSI": _format_tsi(),
        "dataStatus": data_status,
        "window": window,
        "period": period,
        "base": base,
        "executiveSummary": executive_summary[:5],
        "topHoldings": top_holdings_out,
        "riskSnapshot": {
            "hhi": _safe_float(risk.get("hhi")),
            "max_weight": _safe_float(risk.get("max_weight")),
            "vol_30d": _safe_float(risk.get("vol_30d")),
            "var_95_1d": _safe_float(risk.get("var_95_1d")),
            "data_status": risk.get("data_status") or "partial",
        },
        "newsImpactSummary": {
            "coverage": coverage,
            "byAssetClass": by_class,
            "low_signal_ratio": _safe_float(summary.get("low_signal_ratio")),
            "coverage_ratio": _safe_float(summary.get("coverage_ratio")),
        },
        "topNewsDrivers": top_news_drivers,
        "optimizerHints": {
            "period": period,
            "actions": actions,
            "constraints": constraints,
        },
        "missingData": missing_data,
        "debug_notes": debug_notes,
    }
