from __future__ import annotations

from datetime import datetime, timezone

from app.providers.base import ProviderResult
from app.services.quote_router import Quote, QuoteProvider, QuoteRouter


def _iso_from_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _ok(provider: str, quote: Quote) -> ProviderResult[Quote]:
    return ProviderResult(True, provider, quote, 0, False, None, None)


def _err(provider: str, code: str = "empty") -> ProviderResult[Quote]:
    return ProviderResult(False, provider, None, 0, False, code, "err")


class StubProvider(QuoteProvider):
    def __init__(self, name: str, results: list[ProviderResult[Quote]], enabled: bool = True):
        super().__init__(enabled=enabled)
        self.name = name
        self._results = results
        self.calls = 0

    def get_quote(self, symbol: str) -> ProviderResult[Quote]:
        self.calls += 1
        if self.calls <= len(self._results):
            return self._results[self.calls - 1]
        return self._results[-1]


class DisabledProvider(QuoteProvider):
    def __init__(self, name: str):
        super().__init__(enabled=False)
        self.name = name
        self.calls = 0

    def get_quote(self, symbol: str) -> ProviderResult[Quote]:
        self.calls += 1
        raise AssertionError("disabled provider should be skipped")


def test_fallback_only_on_yahoo_miss() -> None:
    now = 1_700_000_000.0
    ts_iso = _iso_from_ts(now)
    yahoo_quote = Quote(price=101.0, change_pct=None, ts_utc=ts_iso, currency=None, meta={})
    alt_quote = Quote(price=102.0, change_pct=None, ts_utc=ts_iso, currency=None, meta={})

    yahoo = StubProvider("yahoo", [_ok("yahoo", yahoo_quote)])
    fallback = StubProvider("finnhub", [_ok("finnhub", alt_quote)])

    router = QuoteRouter([yahoo, fallback], now_fn=lambda: now)
    result = router.get_quote("QQQ")

    assert result.ok
    assert result.data is not None
    assert result.data.price == 101.0
    assert result.data.meta["is_fallback"] is False
    assert yahoo.calls == 1
    assert fallback.calls == 0


def test_provider_disabled_is_skipped() -> None:
    now = 1_700_000_000.0
    ts_iso = _iso_from_ts(now)
    alt_quote = Quote(price=202.0, change_pct=None, ts_utc=ts_iso, currency=None, meta={})

    disabled = DisabledProvider("finnhub")
    yahoo = StubProvider("yahoo", [_ok("yahoo", alt_quote)])

    router = QuoteRouter([disabled, yahoo], now_fn=lambda: now)
    result = router.get_quote("DXY")

    assert result.ok
    assert result.data is not None
    assert result.data.price == 202.0
    assert disabled.calls == 0
    assert yahoo.calls == 1


def test_negative_cache_skips_failed_provider() -> None:
    now = 1_700_000_000.0
    ts_iso = _iso_from_ts(now)
    fallback_quote = Quote(price=303.0, change_pct=None, ts_utc=ts_iso, currency=None, meta={})

    yahoo = StubProvider("yahoo", [_err("yahoo"), _err("yahoo")])
    fallback = StubProvider("finnhub", [_ok("finnhub", fallback_quote), _ok("finnhub", fallback_quote)])

    router = QuoteRouter([yahoo, fallback], now_fn=lambda: now)

    first = router.get_quote("^IXIC")
    second = router.get_quote("^IXIC")

    assert first.ok
    assert second.ok
    assert yahoo.calls == 1
    assert fallback.calls == 2


def test_last_good_served_when_all_fail() -> None:
    now = 1_700_000_000.0
    ts_iso = _iso_from_ts(now)
    yahoo_quote = Quote(price=404.0, change_pct=None, ts_utc=ts_iso, currency=None, meta={})

    yahoo = StubProvider("yahoo", [_ok("yahoo", yahoo_quote), _err("yahoo")])
    fallback = StubProvider("finnhub", [_err("finnhub")])

    router = QuoteRouter([yahoo, fallback], now_fn=lambda: now, last_good_ttl_s=300)

    first = router.get_quote("GOLD")
    second = router.get_quote("GOLD")

    assert first.ok
    assert second.ok
    assert second.degraded_mode is True
    assert second.data is not None
    assert second.data.price == 404.0
    assert second.data.meta["degraded_mode"] is True
