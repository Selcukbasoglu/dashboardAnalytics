from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from app.infra.cache import cache_key
from app.providers.base import ProviderResult


FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
OI_HIST_URL = "https://fapi.binance.com/futures/data/openInterestHist"
OI_LATEST_URL = "https://fapi.binance.com/fapi/v1/openInterest"


def _now_ts() -> int:
    return int(time.time())


def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _get_json_with_backoff(url: str, params: dict, timeout: float) -> tuple[dict | list | None, str | None, bool]:
    backoffs = [1, 2, 4, 8, 16, 30]
    last_err = None
    for idx, delay in enumerate([0] + backoffs):
        if delay:
            time.sleep(delay)
        try:
            with httpx.Client(timeout=timeout) as client:
                res = client.get(url, params=params)
                if res.status_code == 429:
                    last_err = "rate_limited"
                    continue
                res.raise_for_status()
                return res.json(), None, idx == 0
        except Exception as exc:
            last_err = str(exc)
    return None, last_err or "unknown_error", False


def _funding_series(data: list[dict]) -> list[dict]:
    out = []
    for row in data:
        ts = int(row.get("fundingTime") or 0) // 1000
        rate = _safe_float(row.get("fundingRate"))
        if ts:
            out.append({"t": ts, "v": rate})
    return out


def _oi_series(data: list[dict]) -> list[dict]:
    out = []
    for row in data:
        ts = int(row.get("timestamp") or 0) // 1000
        val = _safe_float(row.get("sumOpenInterest") or row.get("openInterest"))
        if ts:
            out.append({"t": ts, "v": val})
    return out


def _funding_z(series: list[dict], window_days: int = 7) -> float:
    if not series:
        return 0.0
    now_ts = _now_ts()
    cutoff = now_ts - (window_days * 24 * 3600)
    values = [p["v"] for p in series if p["t"] >= cutoff]
    if not values:
        values = [p["v"] for p in series[-200:]]
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / max(1, len(values))
    std = math.sqrt(var)
    latest = series[-1]["v"]
    eps = 1e-9
    z = (latest - mean) / max(std, eps)
    return float(max(-5.0, min(5.0, z)))


def _oi_delta_pct(series: list[dict], window_h: int = 24) -> float:
    if not series:
        return 0.0
    now_ts = _now_ts()
    target = now_ts - (window_h * 3600)
    nearest = min(series, key=lambda p: abs(p["t"] - target))
    latest = series[-1]["v"]
    base = nearest["v"]
    if base == 0:
        return 0.0
    return float(((latest - base) / base) * 100.0)


def fetch_binance_derivatives(cache, symbol: str = "BTCUSDT", timeout: float = 8.0) -> ProviderResult[dict]:
    start = time.time()
    degraded = False
    cache_hit = False
    error_code = None
    error_msg = None

    funding_key = cache_key("deriv", "binance", symbol, "funding")
    oi_hist_key = cache_key("deriv", "binance", symbol, "oi_hist")
    oi_latest_key = cache_key("deriv", "binance", symbol, "oi_latest")
    last_good_key = cache_key("deriv", "binance", symbol, "last_good")

    funding = cache.get(funding_key)
    oi_hist = cache.get(oi_hist_key)
    oi_latest = cache.get(oi_latest_key)
    if funding and oi_hist and oi_latest:
        cache_hit = True

    if not funding:
        data, err, ok_first = _get_json_with_backoff(
            FUNDING_URL,
            params={"symbol": symbol, "limit": 1000},
            timeout=timeout,
        )
        if data is not None:
            funding = data
            cache.set(funding_key, data, 120)
        else:
            degraded = True
            error_code = "binance_funding_error"
            error_msg = err

    if not oi_hist:
        data, err, ok_first = _get_json_with_backoff(
            OI_HIST_URL,
            params={"symbol": symbol, "period": "5m", "limit": 500},
            timeout=timeout,
        )
        if data is not None:
            oi_hist = data
            cache.set(oi_hist_key, data, 120)
        else:
            degraded = True
            error_code = error_code or "binance_oi_hist_error"
            error_msg = err

    if not oi_latest:
        data, err, ok_first = _get_json_with_backoff(
            OI_LATEST_URL,
            params={"symbol": symbol},
            timeout=timeout,
        )
        if data is not None:
            oi_latest = data
            cache.set(oi_latest_key, data, 120)
        else:
            degraded = True
            error_code = error_code or "binance_oi_latest_error"
            error_msg = err

    if not funding or not oi_hist or not oi_latest:
        last_good = cache.get(last_good_key)
        if last_good:
            age = _now_ts() - int(last_good.get("ts", _now_ts()))
            return ProviderResult(
                ok=True,
                source="binance",
                data=last_good.get("data"),
                latency_ms=int((time.time() - start) * 1000),
                cache_hit=cache_hit,
                error_code=error_code,
                error_msg=error_msg,
                degraded_mode=True,
                last_good_age_s=age,
            )
        return ProviderResult(
            ok=False,
            source="binance",
            data=None,
            latency_ms=int((time.time() - start) * 1000),
            cache_hit=cache_hit,
            error_code=error_code,
            error_msg=error_msg,
            degraded_mode=True,
            last_good_age_s=None,
        )

    funding_series = _funding_series(funding if isinstance(funding, list) else [])
    oi_series = _oi_series(oi_hist if isinstance(oi_hist, list) else [])

    funding_latest = _safe_float((funding[-1] or {}).get("fundingRate")) if funding_series else 0.0
    oi_latest_val = _safe_float((oi_latest or {}).get("openInterest"))

    computed = {
        "funding_z": _funding_z(funding_series),
        "oi_delta_pct": _oi_delta_pct(oi_series),
    }

    payload = {
        "funding": {"latest": funding_latest, "series": funding_series},
        "oi": {"latest": oi_latest_val, "series": oi_series},
        "computed": computed,
    }
    cache.set(last_good_key, {"ts": _now_ts(), "data": payload}, 3600)

    return ProviderResult(
        ok=True,
        source="binance",
        data=payload,
        latency_ms=int((time.time() - start) * 1000),
        cache_hit=cache_hit,
        error_code=error_code,
        error_msg=error_msg,
        degraded_mode=degraded,
        last_good_age_s=None,
    )
