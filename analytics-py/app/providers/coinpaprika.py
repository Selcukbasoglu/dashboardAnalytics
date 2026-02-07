from __future__ import annotations

import time
from typing import Any

import httpx

from app.infra.cache import cache_key, now_iso
from app.providers.base import ProviderResult


GLOBAL_URL = "https://api.coinpaprika.com/v1/global"
CACHE_TTL_S = 300
LAST_GOOD_TTL_S = 24 * 60 * 60
_BACKOFFS = [0.5, 1.5, 3.0]


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _get_json_backoff(timeout: float) -> dict:
    last_err: Exception | None = None
    for delay in [0.0] + _BACKOFFS:
        if delay:
            time.sleep(delay)
        try:
            with httpx.Client(timeout=timeout) as client:
                res = client.get(GLOBAL_URL)
                if res.status_code == 429:
                    last_err = httpx.HTTPStatusError("429", request=res.request, response=res)
                    continue
                res.raise_for_status()
                return res.json()
        except Exception as exc:
            last_err = exc
    if last_err:
        raise last_err
    raise RuntimeError("coinpaprika_empty_response")


def _build_snapshot(payload: dict) -> dict:
    return {
        "total_mcap_usd": _safe_float(payload.get("market_cap_usd")),
        "btc_dominance_pct": _safe_float(payload.get("bitcoin_dominance_percentage")),
        "total_vol_usd": _safe_float(payload.get("volume_24h_usd")),
        "tsISO": now_iso(),
        "ts_unix": int(time.time()),
        "source": "coinpaprika",
    }


def get_coinpaprika_global(cache, timeout: float = 10.0) -> ProviderResult[dict]:
    start = time.time()
    cache_hit = True
    key = cache_key("coinpaprika", "global")
    last_key = cache_key("coinpaprika", "last_good")
    data = cache.get(key)
    if data is None:
        cache_hit = False
        try:
            payload = _get_json_backoff(timeout=min(timeout, 6.0))
            data = _build_snapshot(payload)
            cache.set(key, data, CACHE_TTL_S)
            cache.set(last_key, data, LAST_GOOD_TTL_S)
        except Exception as exc:
            last_good = cache.get(last_key)
            if last_good:
                last_ts = last_good.get("ts_unix")
                age_s = int(time.time() - last_ts) if isinstance(last_ts, (int, float)) else None
                return ProviderResult(
                    ok=True,
                    source="coinpaprika",
                    data=last_good,
                    latency_ms=int((time.time() - start) * 1000),
                    cache_hit=True,
                    error_code="coinpaprika_error",
                    error_msg=str(exc),
                    degraded_mode=True,
                    last_good_age_s=age_s,
                )
            return ProviderResult(
                ok=False,
                source="coinpaprika",
                data=None,
                latency_ms=int((time.time() - start) * 1000),
                cache_hit=False,
                error_code="coinpaprika_error",
                error_msg=str(exc),
                degraded_mode=True,
                last_good_age_s=None,
            )

    return ProviderResult(
        ok=True,
        source="coinpaprika",
        data=data,
        latency_ms=int((time.time() - start) * 1000),
        cache_hit=cache_hit,
        error_code=None,
        error_msg=None,
        degraded_mode=False,
        last_good_age_s=None,
    )
