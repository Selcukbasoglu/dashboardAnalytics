from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx

from app.providers.base import ProviderResult


MAX_TICKERS = 8
MAX_WORKERS = 4


def _timespan_to_dates(timespan: str) -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    span = timespan.strip().lower()
    delta = timedelta(days=1)
    if span.endswith("h") and span[:-1].isdigit():
        delta = timedelta(hours=int(span[:-1]))
    elif span.endswith("d") and span[:-1].isdigit():
        delta = timedelta(days=int(span[:-1]))
    start = now - delta
    return start.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d")


def _filter_tickers(watchlist: list[str]) -> list[str]:
    out: list[str] = []
    for w in watchlist:
        if not w:
            continue
        token = w.strip().upper()
        if token.endswith(".IS"):
            continue
        if not token.replace(".", "").isalnum():
            continue
        if len(token) <= 5:
            out.append(token)
    return sorted(set(out))[:MAX_TICKERS]


def _fetch_company_news(symbol: str, api_key: str, timespan: str, timeout: float) -> list[dict]:
    base = "https://finnhub.io/api/v1/company-news"
    date_from, date_to = _timespan_to_dates(timespan)
    params = {"symbol": symbol, "from": date_from, "to": date_to, "token": api_key}
    with httpx.Client(timeout=min(timeout, 6.0)) as client:
        res = client.get(base, params=params)
        if res.status_code == 429:
            raise httpx.HTTPStatusError("rate_limited", request=res.request, response=res)
        if res.status_code >= 300:
            raise httpx.HTTPStatusError(f"status_{res.status_code}", request=res.request, response=res)
        data = res.json()
    if isinstance(data, list):
        return data
    return []


def fetch_finnhub_company_news(watchlist: list[str], timespan: str, timeout: float) -> ProviderResult[list]:
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        return ProviderResult(False, "finnhub_news", [], 0, False, "disabled", "missing_api_key", True, None)
    tickers = _filter_tickers(watchlist)
    if not tickers:
        return ProviderResult(False, "finnhub_news", [], 0, False, "no_symbols", "empty_watchlist", True, None)
    start = time.time()
    items: list[dict] = []
    error_msg = None
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(tickers))) as ex:
        futures = {ex.submit(_fetch_company_news, t, api_key, timespan, timeout): t for t in tickers}
        for fut in as_completed(futures):
            try:
                items.extend(fut.result())
            except httpx.HTTPStatusError as exc:
                error_msg = str(exc)
            except Exception as exc:
                error_msg = str(exc)
    ok = bool(items)
    return ProviderResult(
        ok=ok,
        source="finnhub_news",
        data=items,
        latency_ms=int((time.time() - start) * 1000),
        cache_hit=False,
        error_code=None if ok else "finnhub_error",
        error_msg=error_msg,
        degraded_mode=not ok,
        last_good_age_s=None,
    )
