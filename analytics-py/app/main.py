from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
import time

from fastapi import FastAPI

from app.config import load_settings
from app.infra.cache import init_cache, now_iso
from app.infra.db import init_db, purge_old
from app.models import IntelRequest, IntelResponse
from app.services.event_store import last_scan_ts
from app.providers.yahoo import _fetch_chart
from app.services.forecasting import compute_metrics, load_clusters
from app.services.event_impact import load_event_impacts_all
from app.services.intel_pipeline import IntelPipelineService
from app.services.quote_router import get_quote_router, TTLCache
from app.services.portfolio_engine import build_portfolio
from app.services.portfolio_brief import build_daily_brief
from app.services.debate_engine import get_cached_debate, run_debate

app = FastAPI()
settings = load_settings()
cache = init_cache(settings.redis_url, settings.cache_ttl_seconds)
db = init_db(settings.database_url)
purge_old(db, settings.retention_days)
pipeline = IntelPipelineService(settings, cache, db)

_fallback_quote_cache = TTLCache(60, time.time)


@app.get("/health")
def health():
    def _truthy(value: str | None) -> bool:
        return (value or "").lower() in ("1", "true", "yes", "on")

    openai_enabled = _truthy(os.getenv("ENABLE_OPENAI_SUMMARY")) and bool(os.getenv("OPENAI_API_KEY"))
    router_state = get_quote_router().debug_state()
    stats = router_state.get("stats", {})
    providers = router_state.get("providers", {})

    forecast_stats = {}
    row = db.fetchone("SELECT tf, ts_utc, drivers_json FROM forecasts ORDER BY ts_utc DESC LIMIT 1")
    if row:
        try:
            drivers = json.loads(row["drivers_json"] or "{}")
        except Exception:
            drivers = {}
        forecast_stats = {
            "last_tf": row["tf"],
            "last_ts_utc": row["ts_utc"],
            "last_raw_score": drivers.get("raw_score"),
            "last_market_score": drivers.get("market_score"),
            "last_news_score": drivers.get("news_score"),
        }

    return {
        "ok": True,
        "service": "analytics-py",
        "version": os.getenv("SERVICE_VERSION") or os.getenv("GIT_SHA") or "dev",
        "tsISO": now_iso(),
        "providers_enabled": {
            "gdelt": True,
            "rss": True,
            "yahoo": providers.get("yahoo", True),
            "finnhub": providers.get("finnhub", False),
            "twelvedata": providers.get("twelvedata", False),
            "binance": True,
        },
        "quote_router_stats": {
            "provider_hits": stats.get("provider_hits", {}),
            "fallback_hits": stats.get("fallback_hits", 0),
            "fallback_rate": router_state.get("fallback_rate", 0.0),
            "negative_cache_hits": stats.get("negative_cache_hits", 0),
            "rate_limit_hits": stats.get("rate_limit_hits", 0),
        },
        "news_pipeline_stats": pipeline.get_news_stats(),
        "forecast_stats": forecast_stats,
        "env": {
            "FINNHUB_API_KEY": bool(os.getenv("FINNHUB_API_KEY")),
            "TWELVEDATA_API_KEY": bool(os.getenv("TWELVEDATA_API_KEY")),
            "OPENAI_API_KEY": bool(os.getenv("OPENAI_API_KEY")),
            "OPENAI_MODEL": bool(os.getenv("OPENAI_MODEL")),
            "ENABLE_OPENAI_SUMMARY": os.getenv("ENABLE_OPENAI_SUMMARY") is not None,
            "PY_INTEL_BASE_URL": bool(os.getenv("PY_INTEL_BASE_URL")),
            "DATABASE_URL": bool(os.getenv("DATABASE_URL")),
            "NEXT_PUBLIC_API_BASE": bool(os.getenv("NEXT_PUBLIC_API_BASE")),
        },
        "features": {
            "finnhub_fallback_enabled": bool(os.getenv("FINNHUB_API_KEY")),
            "twelvedata_fallback_enabled": bool(os.getenv("TWELVEDATA_API_KEY")),
            "openai_summaries_enabled": openai_enabled,
        },
    }


@app.post("/intel/run", response_model=IntelResponse)
def run_intel(req: IntelRequest):
    return pipeline.run(req)


@app.get("/forecasts/latest")
def forecasts_latest(tf: str, target: str):
    row = db.fetchone(
        "SELECT * FROM forecasts WHERE tf = ? AND target = ? ORDER BY ts_utc DESC LIMIT 1",
        (tf, target),
    )
    if not row:
        return {"forecast": None}
    drivers = {}
    try:
        drivers = json.loads(row["drivers_json"] or "{}")
    except Exception:
        drivers = {}
    return {
        "forecast_id": row["forecast_id"],
        "ts_utc": row["ts_utc"],
        "tf": row["tf"],
        "target": row["target"],
        "direction": row["direction"],
        "confidence": row["confidence"],
        "expires_at_utc": row["expires_at_utc"],
        "rationale_text": row["rationale_text"],
        "drivers": drivers,
    }


@app.get("/forecasts/metrics")
def forecasts_metrics():
    return {"tsISO": now_iso(), "metrics": compute_metrics(db)}


@app.get("/events/latest")
def events_latest(hours: int = 24):
    clusters = load_clusters(db, settings)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    impact_map = load_event_impacts_all(db, [c.cluster_id for c in clusters])
    output = []
    for c in clusters:
        try:
            ts = datetime.fromisoformat(c.ts_utc.replace("Z", "+00:00"))
        except Exception:
            ts = datetime.now(timezone.utc)
        if ts < cutoff:
            continue
        output.append(
            {
                "cluster_id": c.cluster_id,
                "headline": c.headline,
                "ts_utc": c.ts_utc,
                "source_tier": c.source_tier,
                "tags": c.tags,
                "impact_score": c.impact,
                "credibility_score": c.credibility,
                "severity_score": c.severity,
                "direction": c.direction,
                "targets": [{"asset": a, "relevance": r} for a, r in c.targets],
                "realized_impacts": impact_map.get(c.cluster_id, []),
            }
        )
    return {"last_scan_ts": last_scan_ts(db), "clusters": output}


@app.get("/portfolio")
def portfolio(base: str = "TRY", horizon: str = "24h"):
    base = (base or "TRY").upper()
    if base not in ("TRY", "USD"):
        base = "TRY"
    if horizon not in ("24h", "7d", "30d"):
        horizon = "24h"
    return build_portfolio(pipeline, base_currency=base, news_horizon=horizon)


@app.get("/api/v1/portfolio/daily-brief")
@app.get("/portfolio/daily-brief")
def portfolio_daily_brief(base: str = "TRY", window: str = "24h", period: str = "daily"):
    base = (base or "TRY").upper()
    if base not in ("TRY", "USD"):
        base = "TRY"
    if window not in ("24h", "7d", "30d"):
        window = "24h"
    period = (period or "daily").lower()
    if period not in ("daily", "weekly", "monthly"):
        period = "daily"
    try:
        portfolio_payload = build_portfolio(pipeline, base_currency=base, news_horizon=window)
        return build_daily_brief(portfolio_payload, window=window, period=period, base=base)
    except Exception as exc:
        return build_daily_brief(
            {
                "holdings": [],
                "allocation": {},
                "risk": {"data_status": "partial"},
                "newsImpact": {"coverage": {"matched": 0, "total": 0}, "items": [], "summary": {}},
                "recommendations": [],
                "debug_notes": [f"daily_brief_error={type(exc).__name__}"],
            },
            window=window,
            period=period,
            base=base,
        )


@app.get("/api/v1/portfolio/debate")
def portfolio_debate_get(base: str = "TRY", window: str = "24h", horizon: str = "daily"):
    base = (base or "TRY").upper()
    if base not in ("TRY", "USD"):
        base = "TRY"
    if window not in ("24h", "7d", "30d"):
        window = "24h"
    horizon = (horizon or "daily").lower()
    if horizon not in ("daily", "weekly", "monthly"):
        horizon = "daily"
    cached = get_cached_debate(pipeline, base, window, horizon)
    if cached:
        return cached
    return {
        "generatedAtTSI": datetime.now(timezone.utc).isoformat(),
        "dataStatus": "partial",
        "reason": "NO_CACHED_DEBATE",
        "cache": {"hit": False, "ttl_seconds": int(os.getenv("PORTFOLIO_DEBATE_TTL_SECONDS", "21600")), "cooldown_remaining_seconds": 0},
        "providers": {"openrouter": "skipped", "gemini": "skipped"},
        "winner": "single",
        "consensus": {},
        "disagreements": {},
        "raw": {},
        "debug_notes": ["no_cached_debate"],
    }


@app.post("/api/v1/portfolio/debate")
def portfolio_debate_post(payload: dict):
    base = (payload.get("base") or "TRY").upper()
    window = payload.get("window") or "24h"
    horizon = payload.get("horizon") or "daily"
    force = bool(payload.get("force", False))
    if base not in ("TRY", "USD"):
        base = "TRY"
    if window not in ("24h", "7d", "30d"):
        window = "24h"
    horizon = (horizon or "daily").lower()
    if horizon not in ("daily", "weekly", "monthly"):
        horizon = "daily"
    return run_debate(pipeline, base, window, horizon, force=force)


@app.get("/quotes/latest")
def quotes_latest(assets: str):
    assets_list = [a.strip().upper() for a in (assets or "").split(",") if a.strip()]
    if not assets_list:
        return {"tsISO": now_iso(), "quotes": {}}
    quotes = {}
    router = get_quote_router()
    for asset in assets_list:
        row = db.fetchone(
            "SELECT ts_utc, close FROM price_bars WHERE asset = ? ORDER BY ts_utc DESC LIMIT 1",
            (asset,),
        )
        if not row or not row["close"]:
            cached = _fallback_quote_cache.get(asset)
            if cached and cached.get("price"):
                quotes[asset] = cached
                continue
            result = router.get_quote(asset)
            if result.ok and result.data:
                quotes[asset] = {
                    "price": result.data.price,
                    "change_pct": result.data.change_pct,
                    "updated_iso": result.data.ts_utc,
                }
                _fallback_quote_cache.set(asset, quotes[asset])
                continue
            # Yahoo chart fallback (last close + prev close)
            try:
                chart = _fetch_chart(asset, "5d", "1d", 4.0)
                if chart and chart.get("df") is not None and not chart["df"].empty:
                    df = chart["df"]
                    closes = df["Close"].dropna()
                    if not closes.empty:
                        last_close = float(closes.iloc[-1])
                        prev_close = float(closes.iloc[-2]) if len(closes) > 1 else None
                        change_pct = None
                        if prev_close and prev_close != 0:
                            change_pct = (last_close - prev_close) / prev_close * 100.0
                        updated_iso = df.index[-1].to_pydatetime().isoformat().replace("+00:00", "Z")
                        quotes[asset] = {
                            "price": last_close,
                            "change_pct": change_pct,
                            "updated_iso": updated_iso,
                        }
                        _fallback_quote_cache.set(asset, quotes[asset])
            except Exception:
                pass
            continue
        last_ts = row["ts_utc"]
        last_close = row["close"] or 0.0
        try:
            last_dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
        except Exception:
            last_dt = datetime.now(timezone.utc)
        cutoff = (last_dt - timedelta(hours=24)).isoformat().replace("+00:00", "Z")
        prev = db.fetchone(
            "SELECT close FROM price_bars WHERE asset = ? AND ts_utc <= ? ORDER BY ts_utc DESC LIMIT 1",
            (asset, cutoff),
        )
        change_pct = None
        if prev and prev["close"]:
            try:
                change_pct = (last_close - prev["close"]) / prev["close"] * 100.0
            except Exception:
                change_pct = None
        if change_pct is None:
            result = router.get_quote(asset)
            if result.ok and result.data and result.data.change_pct is not None:
                change_pct = result.data.change_pct
        quotes[asset] = {"price": last_close, "change_pct": change_pct, "updated_iso": last_ts}
    return {"tsISO": now_iso(), "quotes": quotes}


@app.get("/quotes/debug")
def quotes_debug():
    router = get_quote_router()
    return router.debug_state()


@app.get("/bars/latest")
def bars_latest(assets: str, limit: int = 96):
    assets_list = [a.strip().upper() for a in (assets or "").split(",") if a.strip()]
    if not assets_list:
        return {"tsISO": now_iso(), "assets": {}}
    limit = max(8, min(int(limit or 96), 192))
    out = {}
    for asset in assets_list:
        rows = db.fetchall(
            "SELECT ts_utc, close FROM price_bars WHERE asset = ? ORDER BY ts_utc DESC LIMIT ?",
            (asset, limit),
        )
        if not rows:
            continue
        rows = list(reversed(rows))
        points = [{"ts": row["ts_utc"], "close": row["close"]} for row in rows if row["close"] is not None]
        if not points:
            continue
        out[asset] = {"updated_iso": points[-1]["ts"], "points": points}
    return {"tsISO": now_iso(), "assets": out}
