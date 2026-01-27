from __future__ import annotations

import time
from typing import Any

import httpx

from app.infra.cache import cache_key, now_iso
from app.providers.base import ProviderResult


GLOBAL_URL = "https://api.coinpaprika.com/v1/global"
CACHE_TTL_S = 300


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _fetch_global(timeout: float) -> dict:
    with httpx.Client(timeout=timeout) as client:
        res = client.get(GLOBAL_URL)
        res.raise_for_status()
        return res.json()


def get_coinpaprika_global(cache, timeout: float = 10.0) -> ProviderResult[dict]:
    start = time.time()
    cache_hit = True
    key = cache_key("coinpaprika", "global")
    data = cache.get(key)
    if data is None:
        cache_hit = False
        try:
            payload = _fetch_global(timeout=min(timeout, 3.5))
            data = {
                "total_mcap_usd": _safe_float(payload.get("market_cap_usd")),
                "btc_dominance_pct": _safe_float(payload.get("bitcoin_dominance_percentage")),
                "total_vol_usd": _safe_float(payload.get("volume_24h_usd")),
                "tsISO": now_iso(),
                "source": "coinpaprika",
            }
            cache.set(key, data, CACHE_TTL_S)
        except Exception as exc:
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
