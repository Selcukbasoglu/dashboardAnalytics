from __future__ import annotations

import time

import httpx

from app.infra.http import get_json, get_text
from app.providers.base import ProviderResult


BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
_RATE_LIMIT_UNTIL = 0.0
_RATE_LIMIT_WINDOW = 60.0


def _rate_limited() -> bool:
    return time.monotonic() < _RATE_LIMIT_UNTIL


def _mark_rate_limited() -> None:
    global _RATE_LIMIT_UNTIL
    _RATE_LIMIT_UNTIL = time.monotonic() + _RATE_LIMIT_WINDOW


def search_gdelt(query: str, timespan: str, maxrecords: int, timeout: float):
    if _rate_limited():
        return [], "gdelt_rate_limited"
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": maxrecords,
        "timespan": timespan,
    }
    try:
        data = get_json(BASE_URL, params=params, timeout=min(timeout, 8.0), retries=0)
        return (data.get("articles") or []), None
    except httpx.HTTPStatusError as exc:
        if exc.response is not None and exc.response.status_code == 429:
            _mark_rate_limited()
            return [], "gdelt_rate_limited"
        return [], f"gdelt_error:{exc}"
    except Exception as exc:
        try:
            text = get_text(BASE_URL, params=params, timeout=min(timeout, 8.0), retries=0)
            if "Your query" in text or "invalid" in text:
                return [], "gdelt_non_json"
        except Exception:
            pass
        return [], f"gdelt_error:{exc}"


def search_gdelt_context(query: str, timespan: str, maxrecords: int, timeout: float):
    if _rate_limited():
        return [], "gdelt_rate_limited"
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": maxrecords,
        "timespan": timespan,
        "context": 1,
        "isquote": 1,
    }
    try:
        data = get_json(BASE_URL, params=params, timeout=min(timeout, 8.0), retries=0)
        return (data.get("articles") or []), None
    except httpx.HTTPStatusError as exc:
        if exc.response is not None and exc.response.status_code == 429:
            _mark_rate_limited()
            return [], "gdelt_rate_limited"
        return [], f"gdelt_context_error:{exc}"
    except Exception as exc:
        try:
            text = get_text(BASE_URL, params=params, timeout=min(timeout, 8.0), retries=0)
            if "Your query" in text or "invalid" in text:
                return [], "gdelt_context_non_json"
        except Exception:
            pass
        return [], f"gdelt_context_error:{exc}"


def fetch_gdelt(query: str, timespan: str, maxrecords: int, timeout: float) -> ProviderResult[list]:
    start = time.time()
    data, err = search_gdelt(query, timespan, maxrecords, timeout)
    error_code = None
    if err:
        if "rate_limited" in err:
            error_code = "429"
        else:
            error_code = "gdelt_error"
    return ProviderResult(
        ok=err is None,
        source="gdelt",
        data=data,
        latency_ms=int((time.time() - start) * 1000),
        cache_hit=False,
        error_code=error_code,
        error_msg=err,
        degraded_mode="rate_limited" in (err or ""),
        last_good_age_s=None,
    )


def fetch_gdelt_context(query: str, timespan: str, maxrecords: int, timeout: float) -> ProviderResult[list]:
    start = time.time()
    data, err = search_gdelt_context(query, timespan, maxrecords, timeout)
    error_code = None
    if err:
        if "rate_limited" in err:
            error_code = "429"
        else:
            error_code = "gdelt_context_error"
    return ProviderResult(
        ok=err is None,
        source="gdelt_context",
        data=data,
        latency_ms=int((time.time() - start) * 1000),
        cache_hit=False,
        error_code=error_code,
        error_msg=err,
        degraded_mode="rate_limited" in (err or ""),
        last_good_age_s=None,
    )
