from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import html
import json
import re
from urllib.parse import urljoin, urlparse

from app.infra.http import get_text


TR_LOCAL_SCRAPE_PAGES = [
    "https://www.ekonomim.com/finans/borsa",
    "https://www.ekonomim.com/finans/haberler",
    "https://www.dunya.com/finans/borsa",
    "https://www.dunya.com/finans/haberler",
    "https://www.paraanaliz.com/borsa/",
    "https://www.paraanaliz.com/sirketler/",
]

TR_LOCAL_SCRAPE_DOMAINS = {
    "dunya.com",
    "ekonomim.com",
    "paraanaliz.com",
}

_BIST_TERMS = {
    "arz",
    "bilanco",
    "bist",
    "borsa",
    "borsa istanbul",
    "halka",
    "hisse",
    "hisseler",
    "kap",
    "spk",
    "temettu",
}

_NAV_TITLES = {
    "altin",
    "anasayfa",
    "borsa",
    "doviz",
    "ekonomi",
    "emtia",
    "finans",
    "hisseler",
    "son dakika",
    "yazarlar",
}

_EXCLUDE_PATH_HINTS = (
    "/hisseler/",
    "/endeksler/",
    "/doviz/",
    "/altin/",
    "/emtia/",
    "/kripto-para",
    "/video",
    "/galeri",
    "/yazar",
    "/tag/",
    "/kategori/",
    "/rss",
    "/feed",
    "/seans-istatistigi",
    "/sirket-bilgileri",
    "/en-cok-artan",
    "/en-cok-azalan",
    "/en-cok-islem-gorenler",
)

_SCRIPT_JSONLD_RE = re.compile(
    r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
    flags=re.IGNORECASE | re.DOTALL,
)

_ANCHOR_RE = re.compile(
    r"<a\b(?P<attrs>[^>]*)href=(?P<q>[\"'])(?P<href>.*?)(?P=q)(?P<rest>[^>]*)>(?P<body>.*?)</a>",
    flags=re.IGNORECASE | re.DOTALL,
)

_ATTR_RE = re.compile(r"\b{name}\s*=\s*([\"'])(.*?)\1", flags=re.IGNORECASE | re.DOTALL)
_TOKEN_RE = re.compile(r"[A-Za-z0-9ÇĞİÖŞÜçğıöşü]+")
_TAG_RE = re.compile(r"<[^>]+>")


def _canonical_domain(url: str) -> str:
    host = (urlparse(url).netloc or "").lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host


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


def _text_clean(value: str | None) -> str:
    if not value:
        return ""
    plain = _TAG_RE.sub(" ", value)
    plain = html.unescape(plain)
    return re.sub(r"\s+", " ", plain).strip()


def _extract_attr(attrs: str, name: str) -> str:
    pattern = re.compile(_ATTR_RE.pattern.format(name=re.escape(name)), flags=_ATTR_RE.flags)
    match = pattern.search(attrs or "")
    if not match:
        return ""
    return _text_clean(match.group(2))


def _looks_like_article_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False
    domain = _canonical_domain(url)
    if domain and domain not in TR_LOCAL_SCRAPE_DOMAINS:
        return False
    path = (parsed.path or "").lower()
    if not path or path == "/":
        return False
    if any(h in path for h in _EXCLUDE_PATH_HINTS):
        return False
    if re.search(r"/20\d{2}/", path):
        return True
    if "haberi-" in path:
        return True
    if "/haberler/" in path and len([p for p in path.split("/") if p]) >= 4:
        return True
    if "/borsa/" in path and len([p for p in path.split("/") if p]) >= 3:
        return True
    if "/sirketler/" in path and len([p for p in path.split("/") if p]) >= 3:
        return True
    return False


def _iter_jsonld_nodes(node):
    if isinstance(node, list):
        for item in node:
            yield from _iter_jsonld_nodes(item)
        return
    if not isinstance(node, dict):
        return

    typ = node.get("@type")
    types: set[str] = set()
    if isinstance(typ, list):
        types = {str(t).strip().lower() for t in typ}
    elif isinstance(typ, str):
        types = {typ.strip().lower()}

    if types.intersection({"newsarticle", "article"}):
        yield node

    if "@graph" in node:
        yield from _iter_jsonld_nodes(node.get("@graph"))

    for value in node.values():
        if isinstance(value, (dict, list)):
            yield from _iter_jsonld_nodes(value)


def _extract_url(value) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        return (value.get("@id") or value.get("url") or "").strip()
    return ""


def _extract_jsonld_items(text: str, page_url: str) -> list[dict]:
    out: list[dict] = []
    for match in _SCRIPT_JSONLD_RE.finditer(text or ""):
        raw = (match.group(1) or "").strip()
        if not raw:
            continue
        payload = html.unescape(raw).strip()
        if payload.endswith(";"):
            payload = payload[:-1]
        try:
            obj = json.loads(payload)
        except Exception:
            continue

        for node in _iter_jsonld_nodes(obj):
            title = _text_clean(node.get("headline") or node.get("name") or node.get("title"))
            url = _extract_url(node.get("url")) or _extract_url(node.get("mainEntityOfPage"))
            if not url:
                continue
            url = urljoin(page_url, url)
            if not _looks_like_article_url(url):
                continue
            if len(title) < 10:
                continue
            out.append(
                {
                    "title": title,
                    "url": url,
                    "summary": _text_clean(node.get("description")),
                    "published": (
                        node.get("datePublished")
                        or node.get("dateCreated")
                        or node.get("dateModified")
                        or ""
                    ),
                    "source": _canonical_domain(url),
                }
            )
    return out


def _extract_anchor_items(text: str, page_url: str) -> list[dict]:
    out: list[dict] = []
    for match in _ANCHOR_RE.finditer(text or ""):
        href = html.unescape((match.group("href") or "").strip())
        if not href or href.startswith("javascript:"):
            continue
        url = urljoin(page_url, href)
        if not _looks_like_article_url(url):
            continue

        attrs = f"{match.group('attrs') or ''} {match.group('rest') or ''}"
        title_attr = _extract_attr(attrs, "title")
        text_body = _text_clean(match.group("body"))
        title = title_attr or text_body
        if len(title) < 10:
            continue
        if title.casefold() in _NAV_TITLES:
            continue

        out.append(
            {
                "title": title,
                "url": url,
                "summary": "",
                "published": "",
                "source": _canonical_domain(url),
            }
        )
    return out


def _query_tokens(query: str) -> list[str]:
    base = [t.casefold() for t in _TOKEN_RE.findall(query or "") if len(t) > 2]
    deduped: list[str] = []
    for tok in base:
        if tok not in deduped:
            deduped.append(tok)
    return deduped[:12]


def _dedup_items(raw_items: list[dict], max_items: int) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for item in raw_items:
        url = (item.get("url") or "").strip()
        title = (item.get("title") or "").strip()
        if not url or not title:
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(
            {
                "title": title,
                "url": url,
                "summary": (item.get("summary") or "").strip(),
                "published": (item.get("published") or "").strip(),
                "source": (item.get("source") or _canonical_domain(url)),
            }
        )
        if len(out) >= max_items:
            break
    return out


def filter_tr_local_scraped_items(
    raw_items: list[dict],
    query: str,
    max_items: int,
    strict: bool = False,
    timespan: str | None = None,
) -> list[dict]:
    if not raw_items:
        return []

    cutoff = _timespan_cutoff(timespan)
    scoped: list[dict] = []
    for item in raw_items:
        published = _parse_published(item.get("published"))
        if cutoff and published is not None and published < cutoff:
            continue
        scoped.append(item)

    tokens = _query_tokens(query)
    default_terms = list(_BIST_TERMS)
    query_text = (query or "").casefold()
    query_has_local_hint = any(h in query_text for h in ("bist", "borsa", "hisse", "kap", "spk", "halka"))

    filtered: list[dict] = []
    for item in scoped:
        haystack = f"{item.get('title') or ''} {item.get('url') or ''}".casefold()
        if tokens:
            token_hit = any(tok in haystack for tok in tokens)
            local_hit = any(term in haystack for term in default_terms)
            if token_hit or (query_has_local_hint and (not strict) and local_hit):
                filtered.append(item)
            continue
        if any(term in haystack for term in default_terms):
            filtered.append(item)

    if tokens and not strict:
        floor = max(3, max_items // 3)
        if len(filtered) < floor and (not query_has_local_hint):
            filtered = scoped

    if tokens:
        source_items = filtered if filtered else ([] if strict else scoped)
    else:
        source_items = filtered

    return _dedup_items(source_items, max_items=max_items)


def search_tr_local_scrape(
    query: str,
    max_items: int = 50,
    pages: list[str] | None = None,
    strict: bool = False,
    timespan: str | None = None,
    timeout: float = 4.0,
    raw: bool = False,
) -> list[dict]:
    pages = pages or TR_LOCAL_SCRAPE_PAGES
    items: list[dict] = []

    for page_url in pages:
        try:
            text = get_text(page_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, retries=0)
        except Exception:
            continue
        items.extend(_extract_jsonld_items(text, page_url))
        items.extend(_extract_anchor_items(text, page_url))

    deduped = _dedup_items(items, max_items=max(max_items * 3, 120))
    if raw:
        return deduped[:max_items]
    return filter_tr_local_scraped_items(deduped, query, max_items=max_items, strict=strict, timespan=timespan)
