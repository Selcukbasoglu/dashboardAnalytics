from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
import time
from urllib.parse import quote_plus
from email.utils import parsedate_to_datetime

import feedparser

from app.infra.http import get_text
from app.providers.base import ProviderResult


FALLBACK_FEEDS = [
    "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://oilprice.com/rss/main",
    "https://www.techmeme.com/feed.xml",
]

PRESS_RELEASE_FEEDS = [
    "https://www.prnewswire.com/rss/news-releases-list.rss",
    "https://www.businesswire.com/portal/site/home/rss/",
    "https://www.globenewswire.com/RssFeed/industry/All?format=xml",
]

TR_NEWS_FEEDS = [
    "https://www.kap.org.tr/en/rss",
    "https://www.kap.org.tr/tr/rss",
    "https://www.aa.com.tr/rss/ajansguncel.xml",
    "https://www.aa.com.tr/tr/rss/default?cat=guncel",
    "https://www.bloomberght.com/rss",
    "https://www.haberturk.com/rss/ekonomi.xml",
    "https://www.ntv.com.tr/ekonomi.rss",
    "https://www.trthaber.com/sondakika.rss",
    "https://www.sozcu.com.tr/feeds-son-dakika",
    "https://www.mynet.com/haber/rss/sondakika",
    "https://www.cnnturk.com/feed/rss/all/news",
    "https://www.hurriyet.com.tr/rss/ekonomi",
    "https://www.milliyet.com.tr/rss/rssnew/ekonomi.xml",
    "https://www.dunya.com/rss/ekonomi",
    "https://www.ekonomim.com/rss",
    "https://www.finansgundem.com/rss",
    "https://www.haber7.com/rss/ekonomi",
    "https://www.ensonhaber.com/rss/ensonhaber.xml",
    "https://www.haberglobal.com.tr/rss",
    "https://www.karar.com/rss",
    "https://www.para.com.tr/feed",
    "https://www.ekonomist.com.tr/feed",
]

MAX_EXTRA_FEEDS = 5


def _parse_feed(url: str, timeout: float = 6.0):
    try:
        text = get_text(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, retries=0)
        feed = feedparser.parse(text)
    except Exception:
        # Avoid feedparser fetching the URL itself without a timeout.
        return []
    items = []
    for entry in feed.entries:
        items.append(
            {
                "title": entry.get("title", ""),
                "url": entry.get("link", ""),
                "source": (entry.get("source") or {}).get("title", ""),
                "published": entry.get("published", ""),
                "summary": entry.get("summary", "") or entry.get("description", ""),
            }
        )
    return items


def _google_when_suffix(timespan: str | None) -> str:
    if not timespan:
        return ""
    if timespan.endswith("d"):
        days = timespan[:-1]
        if days.isdigit():
            return f" when:{days}d"
    return ""


def _timespan_cutoff(timespan: str | None) -> datetime | None:
    if not timespan:
        return None
    span = (timespan or "").strip().lower()
    now = datetime.now(timezone.utc)
    if span.endswith("h") and span[:-1].isdigit():
        return now - timedelta(hours=int(span[:-1]))
    if span.endswith("d") and span[:-1].isdigit():
        return now - timedelta(days=int(span[:-1]))
    return None


def _parse_published(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        if dt is not None:
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _filter_items(items: list[dict], query: str, max_items: int, strict: bool = False, timespan: str | None = None) -> list[dict]:
    cutoff = _timespan_cutoff(timespan)
    if cutoff:
        fresh: list[dict] = []
        undated: list[dict] = []
        for it in items:
            published = _parse_published(it.get("published"))
            if published is None:
                undated.append(it)
                continue
            if published >= cutoff:
                fresh.append(it)
        if len(fresh) >= max(5, max_items // 3):
            items = fresh
        else:
            items = fresh + undated

    tokens = [t.lower() for t in re.findall(r"[A-Za-z0-9]+", query) if len(t) > 2][:10]
    if tokens:
        filtered = [it for it in items if any(tok in (it.get("title") or "").lower() for tok in tokens)]
        if strict:
            items = filtered
        elif len(filtered) >= max(5, max_items // 3):
            items = filtered
    seen = set()
    out = []
    for it in items:
        url = it.get("url") or ""
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(it)
        if len(out) >= max_items:
            break
    return out


def search_rss(
    query: str,
    max_items: int = 50,
    timespan: str | None = None,
    extra_feeds: list[str] | None = None,
):
    items = []
    rss_query = f"{query}{_google_when_suffix(timespan)}".strip()
    google_url = f"https://news.google.com/rss/search?q={quote_plus(rss_query)}&hl=en-US&gl=US&ceid=US:en"
    items.extend(_parse_feed(google_url, timeout=4.0))

    if not items:
        for url in FALLBACK_FEEDS:
            items.extend(_parse_feed(url, timeout=6.0))

    if extra_feeds:
        for url in extra_feeds[:MAX_EXTRA_FEEDS]:
            items.extend(_parse_feed(url, timeout=6.0))

    if not items:
        return []

    return _filter_items(items, query, max_items, timespan=timespan)


def search_rss_local(
    query: str,
    max_items: int = 50,
    extra_feeds: list[str] | None = None,
    strict: bool = False,
    timespan: str | None = None,
    timeout: float = 6.0,
):
    items = []
    if extra_feeds:
        for url in extra_feeds[:MAX_EXTRA_FEEDS]:
            items.extend(_parse_feed(url, timeout=timeout))
    if not items:
        return []
    return _filter_items(items, query, max_items, strict=strict, timespan=timespan)


def fetch_rss(query: str, max_items: int, timeout: float, timespan: str | None = None) -> ProviderResult[list]:
    start = time.time()
    try:
        data = search_rss(query, max_items=max_items, timespan=timespan)
        return ProviderResult(
            ok=True,
            source="rss",
            data=data,
            latency_ms=int((time.time() - start) * 1000),
            cache_hit=False,
            error_code=None,
            error_msg=None,
            degraded_mode=False,
            last_good_age_s=None,
        )
    except Exception as exc:
        return ProviderResult(
            ok=False,
            source="rss",
            data=None,
            latency_ms=int((time.time() - start) * 1000),
            cache_hit=False,
            error_code="rss_error",
            error_msg=str(exc),
            degraded_mode=True,
            last_good_age_s=None,
        )
