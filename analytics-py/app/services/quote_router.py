from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

import httpx

from app.infra.cache import now_iso
from app.providers.base import ProviderResult


logger = logging.getLogger("quote_router")


STALE_AFTER_S = 6 * 60 * 60
NEGATIVE_CACHE_TTL_S = 45 * 60
LAST_GOOD_TTL_S = 120


@dataclass
class Quote:
    price: float
    change_pct: float | None
    ts_utc: str
    currency: str | None
    meta: dict[str, Any]


class TokenBucket:
    def __init__(self, capacity: int, refill_per_s: float, now_fn: Callable[[], float]):
        self.capacity = capacity
        self.refill_per_s = refill_per_s
        self.tokens = float(capacity)
        self.last_ts = now_fn()
        self.now_fn = now_fn

    def take(self, amount: float = 1.0) -> bool:
        now = self.now_fn()
        elapsed = max(0.0, now - self.last_ts)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_s)
        self.last_ts = now
        if self.tokens < amount:
            return False
        self.tokens -= amount
        return True


class TTLCache:
    def __init__(self, ttl_s: int, now_fn: Callable[[], float]):
        self.ttl_s = ttl_s
        self.now_fn = now_fn
        self.store: dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Any | None:
        entry = self.store.get(key)
        if not entry:
            return None
        expires_at, value = entry
        if self.now_fn() > expires_at:
            self.store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any, ttl_s: int | None = None) -> None:
        ttl = self.ttl_s if ttl_s is None else ttl_s
        self.store[key] = (self.now_fn() + ttl, value)

    def snapshot(self) -> dict[str, Any]:
        now = self.now_fn()
        out: dict[str, Any] = {}
        expired: list[str] = []
        for key, (expires_at, value) in self.store.items():
            if now > expires_at:
                expired.append(key)
                continue
            out[key] = value
        for key in expired:
            self.store.pop(key, None)
        return out


class SymbolResolver:
    def __init__(self, now_fn: Callable[[], float]):
        self.now_fn = now_fn
        self.cache = TTLCache(7 * 24 * 60 * 60, now_fn)
        self.maps: dict[str, dict[str, str]] = {
            "yahoo": {
                "BTC": "BTC-USD",
                "ETH": "ETH-USD",
                "NASDAQ": "^IXIC",
                "FTSE": "^FTSE",
                "EUROSTOXX": "^STOXX50E",
                "BIST": "XU100.IS",
                "DXY": "DX-Y.NYB",
                "OIL": "CL=F",
                "GOLD": "GC=F",
                "SILVER": "SI=F",
                "COPPER": "HG=F",
            },
            "finnhub": {
                "BTC": "BINANCE:BTCUSDT",
                "ETH": "BINANCE:ETHUSDT",
                "NASDAQ": "^IXIC",
                "FTSE": "^FTSE",
                "EUROSTOXX": "^STOXX50E",
                "BIST": "XU100.IS",
                "DXY": "DXY",
                "OIL": "CL=F",
                "GOLD": "GC=F",
                "SILVER": "SI=F",
                "COPPER": "HG=F",
            },
            "twelvedata": {
                "BTC": "BTC/USD",
                "ETH": "ETH/USD",
                "NASDAQ": "^IXIC",
                "FTSE": "^FTSE",
                "EUROSTOXX": "^STOXX50E",
                "BIST": "XU100.IS",
                "DXY": "DXY",
                "OIL": "CL=F",
                "GOLD": "GC=F",
                "SILVER": "SI=F",
                "COPPER": "HG=F",
            },
        }

    def resolve(self, symbol: str, provider: str, searcher: Callable[[str], str | None] | None) -> str:
        mapped = self.maps.get(provider, {}).get(symbol)
        if mapped:
            return mapped
        cache_key = f"{provider}:{symbol}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        if searcher:
            resolved = searcher(symbol)
            if resolved:
                self.cache.set(cache_key, resolved)
                return resolved
            self.cache.set(cache_key, symbol)
        return symbol


class QuoteProvider:
    name = "provider"

    def __init__(self, enabled: bool = True):
        self.enabled = enabled

    def get_quote(self, symbol: str) -> ProviderResult[Quote]:
        raise NotImplementedError

    def search(self, symbol: str) -> str | None:
        return None


class YahooQuoteProvider(QuoteProvider):
    name = "yahoo"

    def get_quote(self, symbol: str) -> ProviderResult[Quote]:
        started = time.time()
        url = "https://query1.finance.yahoo.com/v7/finance/quote"
        params = {"symbols": symbol}
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            with httpx.Client(timeout=4.0) as client:
                res = client.get(url, params=params, headers=headers)
                status = res.status_code
                if status >= 500:
                    return ProviderResult(False, self.name, None, _latency_ms(started), False, "http_5xx", res.text)
                if status == 429:
                    return ProviderResult(False, self.name, None, _latency_ms(started), False, "http_429", "rate_limited")
                if status >= 300:
                    return ProviderResult(False, self.name, None, _latency_ms(started), False, "http_error", res.text)
                payload = res.json()
        except Exception as exc:
            return ProviderResult(False, self.name, None, _latency_ms(started), False, "network_error", str(exc))

        result = (payload.get("quoteResponse") or {}).get("result") or []
        if not result:
            return ProviderResult(False, self.name, None, _latency_ms(started), False, "empty", "no_result")
        node = result[0]
        price = node.get("regularMarketPrice")
        if price is None or price == 0:
            return ProviderResult(False, self.name, None, _latency_ms(started), False, "missing_price", "no_price")
        change_pct = node.get("regularMarketChangePercent")
        ts = node.get("regularMarketTime")
        ts_iso = _ts_from_epoch(ts) if ts else now_iso()
        currency = node.get("currency")
        quote = Quote(
            price=float(price),
            change_pct=float(change_pct) if change_pct is not None else None,
            ts_utc=ts_iso,
            currency=currency,
            meta={},
        )
        return ProviderResult(True, self.name, quote, _latency_ms(started), False, None, None)


class FinnhubQuoteProvider(QuoteProvider):
    name = "finnhub"

    def __init__(self, api_key: str | None):
        super().__init__(enabled=bool(api_key))
        self.api_key = api_key

    def get_quote(self, symbol: str) -> ProviderResult[Quote]:
        if not self.api_key:
            return ProviderResult(False, self.name, None, 0, False, "disabled", "missing_api_key")
        started = time.time()
        url = "https://finnhub.io/api/v1/quote"
        params = {"symbol": symbol, "token": self.api_key}
        try:
            with httpx.Client(timeout=4.0) as client:
                res = client.get(url, params=params)
                status = res.status_code
                if status >= 500:
                    return ProviderResult(False, self.name, None, _latency_ms(started), False, "http_5xx", res.text)
                if status == 429:
                    return ProviderResult(False, self.name, None, _latency_ms(started), False, "http_429", "rate_limited")
                if status >= 300:
                    return ProviderResult(False, self.name, None, _latency_ms(started), False, "http_error", res.text)
                payload = res.json()
        except Exception as exc:
            return ProviderResult(False, self.name, None, _latency_ms(started), False, "network_error", str(exc))

        price = payload.get("c")
        if price is None or price == 0:
            return ProviderResult(False, self.name, None, _latency_ms(started), False, "missing_price", "no_price")
        change_pct = payload.get("dp")
        ts = payload.get("t")
        ts_iso = _ts_from_epoch(ts) if ts else now_iso()
        quote = Quote(
            price=float(price),
            change_pct=float(change_pct) if change_pct is not None else None,
            ts_utc=ts_iso,
            currency=None,
            meta={},
        )
        return ProviderResult(True, self.name, quote, _latency_ms(started), False, None, None)

    def search(self, symbol: str) -> str | None:
        if not self.api_key:
            return None
        url = "https://finnhub.io/api/v1/search"
        params = {"q": symbol, "token": self.api_key}
        try:
            with httpx.Client(timeout=4.0) as client:
                res = client.get(url, params=params)
                if res.status_code >= 300:
                    return None
                payload = res.json()
        except Exception:
            return None
        results = payload.get("result") or []
        for item in results:
            if item.get("symbol") == symbol or item.get("displaySymbol") == symbol:
                return item.get("symbol") or item.get("displaySymbol")
        if results:
            return results[0].get("symbol") or results[0].get("displaySymbol")
        return None


class TwelveDataQuoteProvider(QuoteProvider):
    name = "twelvedata"

    def __init__(self, api_key: str | None):
        super().__init__(enabled=bool(api_key))
        self.api_key = api_key

    def get_quote(self, symbol: str) -> ProviderResult[Quote]:
        if not self.api_key:
            return ProviderResult(False, self.name, None, 0, False, "disabled", "missing_api_key")
        started = time.time()
        url = "https://api.twelvedata.com/quote"
        params = {"symbol": symbol, "apikey": self.api_key}
        try:
            with httpx.Client(timeout=4.0) as client:
                res = client.get(url, params=params)
                status = res.status_code
                if status >= 500:
                    return ProviderResult(False, self.name, None, _latency_ms(started), False, "http_5xx", res.text)
                if status == 429:
                    return ProviderResult(False, self.name, None, _latency_ms(started), False, "http_429", "rate_limited")
                if status >= 300:
                    return ProviderResult(False, self.name, None, _latency_ms(started), False, "http_error", res.text)
                payload = res.json()
        except Exception as exc:
            return ProviderResult(False, self.name, None, _latency_ms(started), False, "network_error", str(exc))

        if payload.get("status") == "error":
            return ProviderResult(False, self.name, None, _latency_ms(started), False, "api_error", payload.get("message"))
        price = payload.get("price")
        if price is None or price == "0" or price == 0:
            return ProviderResult(False, self.name, None, _latency_ms(started), False, "missing_price", "no_price")
        change_pct = payload.get("percent_change")
        ts = payload.get("datetime") or payload.get("timestamp")
        ts_iso = _ts_from_string(ts) if ts else now_iso()
        quote = Quote(
            price=float(price),
            change_pct=float(change_pct) if change_pct is not None else None,
            ts_utc=ts_iso,
            currency=payload.get("currency"),
            meta={},
        )
        return ProviderResult(True, self.name, quote, _latency_ms(started), False, None, None)

    def search(self, symbol: str) -> str | None:
        if not self.api_key:
            return None
        url = "https://api.twelvedata.com/symbol_search"
        params = {"symbol": symbol, "apikey": self.api_key}
        try:
            with httpx.Client(timeout=4.0) as client:
                res = client.get(url, params=params)
                if res.status_code >= 300:
                    return None
                payload = res.json()
        except Exception:
            return None
        items = payload.get("data") or []
        for item in items:
            if item.get("symbol") == symbol:
                return item.get("symbol")
        if items:
            return items[0].get("symbol")
        return None


@dataclass
class ProviderState:
    bucket: TokenBucket
    backoff_until: float = 0.0
    backoff_exp: int = 0


class QuoteRouter:
    def __init__(
        self,
        providers: list[QuoteProvider],
        now_fn: Callable[[], float] | None = None,
        negative_ttl_s: int = NEGATIVE_CACHE_TTL_S,
        stale_after_s: int = STALE_AFTER_S,
        last_good_ttl_s: int = LAST_GOOD_TTL_S,
    ):
        self.now_fn = now_fn or time.time
        self.providers = providers
        self.negative_cache = TTLCache(negative_ttl_s, self.now_fn)
        self.last_good = TTLCache(last_good_ttl_s, self.now_fn)
        self.symbol_meta = TTLCache(24 * 60 * 60, self.now_fn)
        self.stale_after_s = stale_after_s
        self.resolver = SymbolResolver(self.now_fn)
        self.stats = {
            "provider_hits": {},
            "fallback_hits": 0,
            "negative_cache_hits": 0,
            "rate_limit_hits": 0,
            "backoff_hits": 0,
            "disabled_providers": {},
        }
        self.provider_state: dict[str, ProviderState] = {}
        for provider in providers:
            if provider.name == "twelvedata":
                rate = (8, 8 / 60)
            elif provider.name == "finnhub":
                rate = (60, 60 / 60)
            else:
                rate = (60, 60 / 60)
            self.provider_state[provider.name] = ProviderState(
                bucket=TokenBucket(rate[0], rate[1], self.now_fn)
            )

    def debug_state(self) -> dict[str, Any]:
        total_hits = sum(self.stats["provider_hits"].values())
        fallback_hits = self.stats["fallback_hits"]
        return {
            "stats": self.stats,
            "fallback_rate": (fallback_hits / total_hits) if total_hits else 0.0,
            "providers": {p.name: p.enabled for p in self.providers},
            "symbol_meta": self.symbol_meta.snapshot(),
        }

    def get_quote(self, symbol: str) -> ProviderResult[Quote]:
        last_good = self.last_good.get(symbol)
        for idx, provider in enumerate(self.providers):
            if not provider.enabled:
                disabled = self.stats["disabled_providers"].setdefault(provider.name, 0)
                self.stats["disabled_providers"][provider.name] = disabled + 1
                continue
            state = self.provider_state[provider.name]
            now = self.now_fn()
            if now < state.backoff_until:
                self.stats["backoff_hits"] += 1
                continue
            if not state.bucket.take(1.0):
                self.stats["rate_limit_hits"] += 1
                continue
            neg_key = f"{provider.name}:{symbol}"
            if self.negative_cache.get(neg_key):
                self.stats["negative_cache_hits"] += 1
                continue

            resolved = self.resolver.resolve(symbol, provider.name, provider.search)
            result = provider.get_quote(resolved)
            if result.ok and result.data:
                freshness = _freshness_seconds(result.data.ts_utc, self.now_fn)
                if freshness is not None and freshness > self.stale_after_s:
                    self.negative_cache.set(neg_key, True)
                    continue
                result.data.meta = _quote_meta(
                    provider.name,
                    is_fallback=idx > 0,
                    freshness_seconds=freshness,
                    degraded_mode=False,
                )
                self._record_hit(provider.name, idx > 0)
                self.last_good.set(symbol, result.data)
                self.symbol_meta.set(symbol, result.data.meta)
                return result

            if result.error_code in ("http_429", "http_5xx"):
                state.backoff_exp = min(state.backoff_exp + 1, 5)
                backoff_s = min(300, 2 ** state.backoff_exp)
                state.backoff_until = self.now_fn() + backoff_s
            self.negative_cache.set(neg_key, True)

        if last_good:
            last_good.meta = _quote_meta(
                last_good.meta.get("source", "cache"),
                is_fallback=True,
                freshness_seconds=_freshness_seconds(last_good.ts_utc, self.now_fn),
                degraded_mode=True,
            )
            self.symbol_meta.set(symbol, last_good.meta)
            return ProviderResult(
                True,
                "last_good",
                last_good,
                0,
                True,
                None,
                None,
                degraded_mode=True,
            )

        return ProviderResult(False, "router", None, 0, False, "all_failed", "no_quote")

    def patch_snapshot(self, snapshot: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        meta: dict[str, Any] = {"used_fallback": False, "providers": {}}
        patched = dict(snapshot)
        for key, cfg in SNAPSHOT_PATCH_MAP.items():
            value = snapshot.get(key, 0)
            if value not in (0, None):
                continue
            result = self.get_quote(cfg["symbol"])
            if not result.ok or not result.data:
                continue
            patched[key] = result.data.price
            chg_key = cfg.get("change_key")
            if chg_key and result.data.change_pct is not None:
                patched[chg_key] = result.data.change_pct
            meta["used_fallback"] = True
            meta["providers"][key] = result.data.meta
        return patched, meta

    def _record_hit(self, provider: str, fallback: bool) -> None:
        hits = self.stats["provider_hits"].setdefault(provider, 0)
        self.stats["provider_hits"][provider] = hits + 1
        if fallback:
            self.stats["fallback_hits"] += 1


SNAPSHOT_PATCH_MAP = {
    "btc": {"symbol": "BTC", "change_key": "btc_chg_24h"},
    "eth": {"symbol": "ETH", "change_key": "eth_chg_24h"},
    "nasdaq": {"symbol": "^IXIC", "change_key": "nasdaq_chg_24h"},
    "ftse": {"symbol": "^FTSE", "change_key": "ftse_chg_24h"},
    "eurostoxx": {"symbol": "^STOXX50E", "change_key": "eurostoxx_chg_24h"},
    "bist": {"symbol": "XU100.IS", "change_key": "bist_chg_24h"},
    "dxy": {"symbol": "DX-Y.NYB", "change_key": "dxy_chg_24h"},
    "oil": {"symbol": "CL=F", "change_key": "oil_chg_24h"},
    "gold": {"symbol": "GC=F", "change_key": "gold_chg_24h"},
    "silver": {"symbol": "SI=F", "change_key": "silver_chg_24h"},
    "copper": {"symbol": "HG=F", "change_key": "copper_chg_24h"},
}


_ROUTER: QuoteRouter | None = None


def get_quote_router() -> QuoteRouter:
    global _ROUTER
    if _ROUTER:
        return _ROUTER
    finnhub_key = os.getenv("FINNHUB_API_KEY")
    twelvedata_key = os.getenv("TWELVEDATA_API_KEY")
    providers: list[QuoteProvider] = [
        YahooQuoteProvider(),
        FinnhubQuoteProvider(finnhub_key),
        TwelveDataQuoteProvider(twelvedata_key),
    ]
    _ROUTER = QuoteRouter(providers)
    return _ROUTER


def _latency_ms(started: float) -> int:
    return int((time.time() - started) * 1000)


def _ts_from_epoch(epoch: int | float) -> str:
    try:
        dt = datetime.fromtimestamp(float(epoch), tz=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return now_iso()


def _ts_from_string(value: str) -> str:
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return now_iso()


def _freshness_seconds(ts_utc: str, now_fn: Callable[[], float]) -> int | None:
    try:
        dt = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
        return int(now_fn() - dt.timestamp())
    except Exception:
        return None


def _quote_meta(source: str, is_fallback: bool, freshness_seconds: int | None, degraded_mode: bool) -> dict[str, Any]:
    return {
        "source": source,
        "is_fallback": is_fallback,
        "freshness_seconds": freshness_seconds,
        "degraded_mode": degraded_mode,
    }
