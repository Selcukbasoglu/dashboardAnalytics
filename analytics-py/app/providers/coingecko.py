from __future__ import annotations

import time
from typing import Any

import httpx

from app.infra.cache import cache_key
from app.providers.base import ProviderResult


GLOBAL_URL = "https://api.coingecko.com/api/v3/global"
PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
_BACKOFFS = [1, 2, 4, 8, 16, 30]


def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _get_json_backoff(url: str, params: dict | None, timeout: float):
    last_err: Exception | None = None
    for delay in [0] + _BACKOFFS:
        if delay:
            time.sleep(delay)
        try:
            with httpx.Client(timeout=timeout) as client:
                res = client.get(url, params=params)
                if res.status_code == 429:
                    last_err = httpx.HTTPStatusError("429", request=res.request, response=res)
                    continue
                res.raise_for_status()
                return res.json(), None
        except Exception as exc:
            last_err = exc
    return None, last_err


def get_coingecko_snapshot(cache, timeout: float):
    note = None
    last_key = cache_key("coingecko", "last")
    last = cache.get(last_key) or {}
    cache_hit = True

    global_key = cache_key("coingecko", "global")
    data = cache.get(global_key)
    if data is None:
        cache_hit = False
        try:
            data, err = _get_json_backoff(GLOBAL_URL, None, timeout=min(timeout, 3.5))
            if err:
                raise err
            cache.set(global_key, data, 180)
        except Exception as exc:
            note = f"coingecko_global_error:{exc}"
            data = None

    price_key = cache_key("coingecko", "price")
    price_data = cache.get(price_key)
    if price_data is None:
        cache_hit = False
        try:
            params = {
                "ids": "bitcoin,ethereum",
                "vs_currencies": "usd",
                "include_24hr_change": "true",
            }
            price_data, err = _get_json_backoff(PRICE_URL, params, timeout=min(timeout, 3.5))
            if err:
                raise err
            cache.set(price_key, price_data, 120)
        except Exception as exc:
            note = f"coingecko_price_error:{exc}"
            price_data = None

    g = (data or {}).get("data") or {}
    dominance = g.get("market_cap_percentage") or {}
    total_vol = (g.get("total_volume") or {}).get("usd")
    total_mcap = (g.get("total_market_cap") or {}).get("usd")

    btc_quote = (price_data or {}).get("bitcoin", {}) or {}
    eth_quote = (price_data or {}).get("ethereum", {}) or {}
    btc_price = _safe_float(btc_quote.get("usd"))
    eth_price = _safe_float(eth_quote.get("usd"))
    btc_change = _safe_float(btc_quote.get("usd_24h_change"))
    eth_change = _safe_float(eth_quote.get("usd_24h_change"))

    if btc_price == 0 and last.get("btc_price_usd"):
        btc_price = _safe_float(last.get("btc_price_usd"))
    if eth_price == 0 and last.get("eth_price_usd"):
        eth_price = _safe_float(last.get("eth_price_usd"))

    snapshot = {
        "btc_price_usd": btc_price,
        "eth_price_usd": eth_price,
        "btc_chg_24h": btc_change,
        "eth_chg_24h": eth_change,
        "total_vol_usd": _safe_float(total_vol) or _safe_float(last.get("total_vol_usd")),
        "total_mcap_usd": _safe_float(total_mcap) or _safe_float(last.get("total_mcap_usd")),
        "dominance": {
            "btc": _safe_float(dominance.get("btc")) or _safe_float((last.get("dominance") or {}).get("btc")),
            "eth": _safe_float(dominance.get("eth")) or _safe_float((last.get("dominance") or {}).get("eth")),
            "usdt": _safe_float(dominance.get("usdt")) or _safe_float((last.get("dominance") or {}).get("usdt")),
            "usdc": _safe_float(dominance.get("usdc")) or _safe_float((last.get("dominance") or {}).get("usdc")),
        },
    }

    deltas = {
        "btc_d": snapshot["dominance"]["btc"] - _safe_float((last.get("dominance") or {}).get("btc")),
        "usdt_d": snapshot["dominance"]["usdt"] - _safe_float((last.get("dominance") or {}).get("usdt")),
        "usdc_d": snapshot["dominance"]["usdc"] - _safe_float((last.get("dominance") or {}).get("usdc")),
        "total_vol": snapshot["total_vol_usd"] - _safe_float(last.get("total_vol_usd")),
        "total_mcap": snapshot["total_mcap_usd"] - _safe_float(last.get("total_mcap_usd")),
    }
    snapshot["deltas"] = deltas
    cache.set(last_key, snapshot, 60 * 60)

    return snapshot, note, cache_hit


def fetch_coingecko(cache, timeout: float) -> ProviderResult[dict]:
    start = time.time()
    try:
        snapshot, note, cache_hit = get_coingecko_snapshot(cache, timeout)
        error_msg = note
        error_code = "coingecko_error" if note else None
        return ProviderResult(
            ok=True,
            source="coingecko",
            data=snapshot,
            latency_ms=int((time.time() - start) * 1000),
            cache_hit=cache_hit,
            error_code=error_code,
            error_msg=error_msg,
            degraded_mode=False,
            last_good_age_s=None,
        )
    except Exception as exc:
        return ProviderResult(
            ok=False,
            source="coingecko",
            data=None,
            latency_ms=int((time.time() - start) * 1000),
            cache_hit=False,
            error_code="coingecko_exception",
            error_msg=str(exc),
            degraded_mode=True,
            last_good_age_s=None,
        )
