from __future__ import annotations

import difflib
import hashlib
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from pathlib import Path
import yaml
from zoneinfo import ZoneInfo

from app.models import (
    CompanyEntity,
    CountryEntity,
    CountryTag,
    CryptoEntity,
    EventEntities,
    EventFeed,
    EventItem,
    ImpactedAsset,
    MarketReaction,
    NewsItem,
    PersonEntity,
    PersonEvent,
)
from app.engine.sector_config import (
    BACKSTOP_DOMAIN_TIER,
    BACKSTOP_GIANTS_PER_SECTOR,
    BACKSTOP_MAXRECORDS,
    BACKSTOP_MIN_HITS,
    PREFERRED_SOURCELANG,
    SECTOR_CORE_ALWAYS,
    SECTOR_DYNAMIC_COUNT,
    SECTOR_GIANTS_REGISTRY,
    SECTOR_ORDER,
    SECTOR_ROTATION_MAX,
    SECTOR_RULES,
    SECTOR_TRIGGER_KEYWORDS,
)
from app.llm.openai_client import summarize_article_openai, summary_enabled
from app.providers.gdelt import search_gdelt, search_gdelt_context
from app.providers.rss import PRESS_RELEASE_FEEDS, TR_NEWS_FEEDS, search_rss, search_rss_local
from app.providers.finnhub_news import fetch_finnhub_company_news
from app.engine.labels import (
    PERSONAL_GROUPS,
    PERSONAL_KEYWORDS,
    PERSONAL_TITLE_ALIASES,
    canonical_person_name,
    person_id,
)
from app.engine.person_impact import extract_person_event, score_person_impact
from app.engine.sector_impact import attach_scope_and_sectors


TIER_A = {"bloomberg.com", "investing.com", "coinglass.com"}
TIER_B = {
    "reuters.com",
    "ft.com",
    "wsj.com",
    "yahoo.com",
    "fortune.com",
    "forbes.com",
    "marketscreener.com",
}
TIER_WEIGHTS = {"A": 1.0, "B": 0.75, "C": 0.5}

MIN_NEWS = 12
MIN_NEWS_LONG = 6
TOP_K_NEWS = 30
TIME_BUCKET_MINUTES = 15
TIMESPAN_FALLBACK = ["1h", "6h", "24h"]
DOMAIN_SOFT_CAP = 5
MAX_QUERIES_PER_SPAN = int(os.getenv("MAX_QUERIES_PER_SPAN", "4"))
NEWS_TIME_BUDGET = 18.0
PERSONAL_TOP_K = 10
TOP_K_TOTAL_EVENTS = 40
TOP_K_PER_CATEGORY = 10
EVENT_TIME_BUCKET_MINUTES = 30
SAME_DOMAIN_SOFT_CAP = 5
EVENT_TIME_BUDGET = 12.0
DEFAULT_NEWS_RANK_WEIGHTS = {"relevance": 0.45, "quality": 0.30, "impact": 0.15, "scope": 0.10}
DEFAULT_NEWS_RANK_PROFILES = {
    "default": DEFAULT_NEWS_RANK_WEIGHTS,
    "risk_off": {"relevance": 0.35, "quality": 0.25, "impact": 0.25, "scope": 0.15},
    "high_volatility": {"relevance": 0.35, "quality": 0.20, "impact": 0.30, "scope": 0.15},
}
SECTOR_ROLLING_EVENTS: List[Tuple[datetime, str, int]] = []
SECTOR_ROTATION_QUEUE: List[str] = []
SECTOR_ROLLING_SEEN: Dict[str, datetime] = {}
PERSON_MATCH_ALL_NEWS = os.getenv("PERSON_MATCH_ALL_NEWS_ENABLED", "true").lower() in ("1", "true", "yes", "on")
PERSONAL_RSS_FALLBACK = os.getenv("PERSONAL_RSS_FALLBACK_ENABLED", "true").lower() in ("1", "true", "yes", "on")
PERSONAL_BUDGET_MS = float(os.getenv("PERSONAL_BUDGET_MS", "800"))

NEWS_LONG_GATING_UNIQUE_CLUSTER_MIN = int(os.getenv("NEWS_LONG_GATING_UNIQUE_CLUSTER_MIN", "3"))
NEWS_LONG_GATING_UNIQUE_DOMAIN_MIN = int(os.getenv("NEWS_LONG_GATING_UNIQUE_DOMAIN_MIN", "3"))
NEWS_LONG_AGE_MIX_ENABLED = os.getenv("NEWS_LONG_AGE_MIX_ENABLED", "true").lower() in ("1", "true", "yes", "on")
NEWS_7D_MIN_AGE_DAYS = int(os.getenv("NEWS_7D_MIN_AGE_DAYS", "3"))
NEWS_7D_MIN_AGE_COUNT = int(os.getenv("NEWS_7D_MIN_AGE_COUNT", "1"))
NEWS_30D_MIN_AGE_DAYS = int(os.getenv("NEWS_30D_MIN_AGE_DAYS", "10"))
NEWS_30D_MIN_AGE_COUNT = int(os.getenv("NEWS_30D_MIN_AGE_COUNT", "2"))
NEWS_EXTRA_MAX_TICKERS = int(os.getenv("NEWS_EXTRA_MAX_TICKERS", "8"))
NEWS_EXTRA_MAX_FEEDS = int(os.getenv("NEWS_EXTRA_MAX_FEEDS", "5"))
RSS_FALLBACK_QUERY_SWEEP = int(os.getenv("RSS_FALLBACK_QUERY_SWEEP", "3"))

NEWS_RECENCY_DECAY_LAMBDA = float(os.getenv("NEWS_RECENCY_DECAY_LAMBDA", "0.045"))
NEWS_RECENCY_DECAY_FLOOR = float(os.getenv("NEWS_RECENCY_DECAY_FLOOR", "0.35"))
NEWS_RECENCY_UNKNOWN_DECAY = float(os.getenv("NEWS_RECENCY_UNKNOWN_DECAY", "0.25"))
NEWS_RECENCY_BONUS_LAMBDA = float(os.getenv("NEWS_RECENCY_BONUS_LAMBDA", "0.18"))
NEWS_RECENCY_BONUS_FLOOR = int(os.getenv("NEWS_RECENCY_BONUS_FLOOR", "2"))
NEWS_RECENCY_UNKNOWN_BONUS = int(os.getenv("NEWS_RECENCY_UNKNOWN_BONUS", "0"))
NEWS_ENTITY_DIMINISH_WEIGHT = float(os.getenv("NEWS_ENTITY_DIMINISH_WEIGHT", "6"))
TIER_AS_DECAY_ENABLED = os.getenv("NEWS_TIER_AS_DECAY_ENABLED", "true").lower() in ("1", "true", "yes", "on")
TIER_DECAY_MODIFIERS = {"A": 0.7, "B": 0.85, "C": 1.0}

PRESS_DOMAINS = {"prnewswire.com", "businesswire.com", "globenewswire.com"}
BIST_BOOST_TERMS = [
    "bist",
    "borsa istanbul",
    "bist 100",
    "bist100",
    "kapanis",
    "kapanış",
    "bilanco",
    "bilanço",
    "kap",
    "temett",
    "temettu",
    "bedelli",
    "bedelsiz",
    "tupas",
    "tüpraş",
    "socm",
    "sokm",
    "sok market",
    "enjsa",
    "astor",
]

_BIST_ALIAS_CACHE: list[str] | None = None


def _load_bist_alias_terms() -> list[str]:
    global _BIST_ALIAS_CACHE
    if _BIST_ALIAS_CACHE is not None:
        return _BIST_ALIAS_CACHE
    try:
        base_dir = Path(__file__).resolve().parents[3]
        alias_path = base_dir / "frontend" / "src" / "lib" / "portfolio_aliases.json"
        if not alias_path.exists():
            _BIST_ALIAS_CACHE = []
            return _BIST_ALIAS_CACHE
        data = yaml.safe_load(alias_path.read_text())
        symbols = (data or {}).get("symbols") or {}
        terms: list[str] = []
        for sym, info in symbols.items():
            if (info or {}).get("asset_class") != "BIST":
                continue
            aliases = (info or {}).get("aliases") or []
            for a in aliases[:6]:
                if not isinstance(a, str):
                    continue
                if " " in a:
                    continue
                if len(a) < 4:
                    continue
                terms.append(a)
        _BIST_ALIAS_CACHE = terms
        return _BIST_ALIAS_CACHE
    except Exception:
        _BIST_ALIAS_CACHE = []
        return _BIST_ALIAS_CACHE


def _local_news_domains() -> set[str]:
    raw = os.getenv("LOCAL_NEWS_DOMAINS", "")
    if not raw:
        return set()
    parts = [p.strip().lower() for p in raw.split(",")]
    return {p for p in parts if p}


def _is_local_domain(domain: str, local_domains: set[str]) -> bool:
    if not domain or not local_domains:
        return False
    return domain.lower() in local_domains
TR_DOMAINS = {"kap.org.tr"}

TAG_RULES = [
    ("War", 4, [r"\bwar\b", r"ceasefire", r"missile", r"conflict", r"ukraine", r"gaza", r"iran", r"yemen", r"red sea"]),
    ("Energy", 4, [r"\boil\b", r"brent", r"wti", r"opec", r"\bgas\b", r"lng", r"energy", r"pipeline"]),
    ("Reg", 3, [r"regulat", r"ban", r"sanction", r"antitrust", r"lawsuit", r"sec", r"cftc"]),
    ("ETF", 3, [r"\betf\b", r"spot etf", r"approval", r"inflow", r"outflow"]),
    ("CEO", 3, [r"\bceo\b", r"earnings", r"guidance", r"outlook", r"chief executive", r"results"]),
    ("Defense", 3, [r"defense", r"military", r"weapons", r"drone"]),
    ("DataCenter", 2, [r"data center", r"datacenter", r"ai chip", r"gpu", r"server farm", r"semiconductor"]),
    ("Stablecoin", 2, [r"stablecoin", r"usdt", r"usdc", r"tether", r"circle"]),
]

BLOCKLIST = {
    "kfru.com",
    "1019online.com",
    "reflex.cz",
}

BASE_INTENT = (
    "statement remarks briefing interview scheduled will speak press conference "
    "ceo chief executive earnings guidance outlook forecast announces announced says said"
)

CATEGORY_QUERY_KEYWORDS = {
    "kripto": "bitcoin crypto exchange stablecoin etf blockchain treasury",
    "enerji": "oil gas opec lng refinery pipeline output supply",
    "teknoloji": "ai chip semiconductor datacenter gpu cloud software platform",
}

CATEGORY_CONTEXT = {
    "kripto": ["bitcoin", "crypto", "blockchain", "exchange", "stablecoin", "treasury", "btc", "eth", "etf"],
    "enerji": ["oil", "gas", "opec", "lng", "refinery", "pipeline", "energy", "petrol"],
    "teknoloji": ["chip", "semiconductor", "ai", "gpu", "datacenter", "cloud", "software", "platform"],
}

EXTRA_QUERY_TERMS = (
    "fed rates inflation cpi pce payroll yields rate cut rate hike "
    "etf spot etf inflow outflow stablecoin depeg redemption "
    "exchange outage custody market structure settlement approval "
    "sanctions tariff export controls conflict ceasefire"
)

AMBIGUOUS_TICKERS = {
    "BP",
    "COP",
    "SO",
    "CAT",
    "META",
    "IR",
    "AI",
    "ORCL",
    "GE",
    "NOW",
    "NET",
    "SHOP",
    "SNAP",
    "RIO",
    "ALL",
}

AMBIGUOUS_CRYPTO = {"SOL", "DOT", "UNI", "LINK", "ONE", "APE", "SAND", "ICP"}

AMBIGUOUS_COUNTRIES = {"georgia", "jordan", "turkey", "türkiye", "chad", "niger"}

EQUITY_CONTEXT_WORDS = {
    "shares",
    "share",
    "stock",
    "hisse",
    "nyse",
    "nasdaq",
    "lse",
    "hkex",
    "rises",
    "falls",
    "up",
    "down",
    "volume",
    "hacim",
    "percent",
    "pct",
}

CRYPTO_CONTEXT_WORDS = {"token", "coin", "crypto", "blockchain", "on-chain", "exchange", "wallet", "defi"}

COUNTRY_CONTEXT_WORDS = {
    "country",
    "government",
    "president",
    "prime",
    "minister",
    "ministry",
    "parliament",
    "sanctions",
    "border",
    "capital",
    "eu",
    "nato",
    "ulke",
    "hukumet",
    "cumhurbaskani",
    "basbakan",
    "bakanlik",
    "parlamento",
    "yaptirim",
    "sinir",
    "baskent",
    "ab",
}

CRYPTO_SYMBOLS = {
    "BTC": ["bitcoin"],
    "ETH": ["ethereum"],
    "SOL": ["solana"],
    "XRP": ["xrp", "ripple"],
    "BNB": ["bnb", "binance"],
    "DOGE": ["dogecoin", "doge"],
    "ADA": ["cardano", "ada"],
    "DOT": ["polkadot", "dot"],
    "LINK": ["chainlink", "link"],
    "UNI": ["uniswap", "uni"],
    "APE": ["apecoin", "ape"],
    "SAND": ["sandbox", "sand"],
    "ICP": ["internet computer", "icp"],
    "USDT": ["tether", "usdt"],
    "USDC": ["usd coin", "usdc"],
}

COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "XRP": "ripple",
    "BNB": "binancecoin",
    "DOGE": "dogecoin",
    "ADA": "cardano",
    "DOT": "polkadot",
    "LINK": "chainlink",
    "UNI": "uniswap",
    "APE": "apecoin",
    "SAND": "the-sandbox",
    "ICP": "internet-computer",
    "USDT": "tether",
    "USDC": "usd-coin",
}

COUNTRY_META = {
    "United States": ("US", "Americas"),
    "United Kingdom": ("GB", "Europe"),
    "Germany": ("DE", "Europe"),
    "France": ("FR", "Europe"),
    "Italy": ("IT", "Europe"),
    "Russia": ("RU", "Europe"),
    "China": ("CN", "Asia"),
    "India": ("IN", "Asia"),
    "Japan": ("JP", "Asia"),
    "South Korea": ("KR", "Asia"),
    "Indonesia": ("ID", "Asia"),
    "Saudi Arabia": ("SA", "MENA"),
    "United Arab Emirates": ("AE", "MENA"),
    "Iran": ("IR", "MENA"),
    "Brazil": ("BR", "Americas"),
    "Canada": ("CA", "Americas"),
    "Mexico": ("MX", "Americas"),
    "Argentina": ("AR", "Americas"),
    "Türkiye": ("TR", "MENA"),
}

CRYPTO_WATCHLIST = [
    ("Tesla", ["Tesla", "TSLA"]),
    ("Coinbase", ["Coinbase", "COIN"]),
    ("PayPal", ["PayPal", "PYPL"]),
    ("Grayscale Bitcoin Trust", ["GBTC", "Grayscale Bitcoin Trust", "Grayscale"]),
    ("Strategy", ["Strategy", "MicroStrategy", "MSTR"]),
    ("Block", ["Block", "Square", "XYZ"]),
    ("Iris Energy", ["IREN", "Iris Energy"]),
    ("Bitmine", ["Bitmine", "BMNR"]),
    ("Galaxy Digital", ["Galaxy Digital", "GLXY", "7LX.F"]),
    ("Cipher Mining", ["Cipher Mining", "CIFR"]),
    ("Riot Platforms", ["Riot", "Riot Platforms", "RIOT"]),
    ("Hut 8", ["Hut 8", "HUT"]),
    ("TeraWulf", ["TeraWulf", "WULF"]),
    ("Core Scientific", ["Core Scientific", "CORZ"]),
    ("Metaplanet", ["Metaplanet", "3350.T", "MTPLF"]),
    ("MARA Holdings", ["MARA", "Marathon Digital", "Marathon"]),
    ("CleanSpark", ["CleanSpark", "CLSK"]),
    ("Bitdeer", ["Bitdeer", "BTDR"]),
    ("Bitfarms", ["Bitfarms", "BITF"]),
    ("Phoenix Group", ["Phoenix Group", "PHX.AE"]),
]

ENERGY_WATCHLIST = [
    ("Saudi Aramco", ["Saudi Aramco", "Aramco", "2222.SR"]),
    ("Exxon Mobil", ["Exxon", "Exxon Mobil", "XOM"]),
    ("Chevron", ["Chevron", "CVX"]),
    ("PetroChina", ["PetroChina", "0857.HK"]),
    ("Shell", ["Shell", "SHEL"]),
    ("GE Vernova", ["GE Vernova", "GEV"]),
    ("NextEra Energy", ["NextEra", "NEE"]),
    ("Iberdrola", ["Iberdrola", "IBE.MC"]),
    ("TotalEnergies", ["TotalEnergies", "Total", "TTE"]),
    ("CNOOC", ["CNOOC", "0883.HK"]),
    ("Siemens Energy", ["Siemens Energy", "ENR.F"]),
    ("Constellation Energy", ["Constellation Energy", "CEG"]),
    ("ConocoPhillips", ["ConocoPhillips", "COP"]),
    ("China Shenhua", ["China Shenhua", "Shenhua", "601088.SS"]),
    ("Enel", ["Enel", "ENEL.MI"]),
    ("Sinopec", ["Sinopec", "600028.SS", "China Petroleum & Chemical"]),
    ("Enbridge", ["Enbridge", "ENB"]),
    ("Southern Company", ["Southern Company", "SO"]),
    ("Duke Energy", ["Duke Energy", "DUK"]),
    ("BP", ["BP", "BP plc"]),
]

TECH_WATCHLIST = [
    ("NVIDIA", ["NVIDIA", "NVDA"]),
    ("Alphabet", ["Alphabet", "Google", "GOOG", "GOOGL"]),
    ("Apple", ["Apple", "AAPL"]),
    ("Microsoft", ["Microsoft", "MSFT"]),
    ("Amazon", ["Amazon", "AMZN", "AWS"]),
    ("TSMC", ["TSMC", "Taiwan Semiconductor", "TSM"]),
    ("Broadcom", ["Broadcom", "AVGO"]),
    ("Meta", ["Meta", "Facebook", "Instagram", "META"]),
    ("Tesla", ["Tesla", "TSLA"]),
    ("Tencent", ["Tencent", "TCEHY"]),
    ("Samsung Electronics", ["Samsung", "Samsung Electronics", "005930.KS"]),
    ("Oracle", ["Oracle", "ORCL"]),
    ("ASML", ["ASML", "ASML Holding"]),
    ("Palantir", ["Palantir", "PLTR"]),
    ("Alibaba", ["Alibaba", "BABA"]),
    ("Micron", ["Micron", "MU"]),
    ("AMD", ["AMD", "Advanced Micro Devices"]),
    ("Netflix", ["Netflix", "NFLX"]),
    ("SK Hynix", ["SK Hynix", "000660.KS"]),
    ("Cisco", ["Cisco", "CSCO"]),
]

WATCHLIST_BY_CATEGORY = {
    "kripto": CRYPTO_WATCHLIST,
    "enerji": ENERGY_WATCHLIST,
    "teknoloji": TECH_WATCHLIST,
}


def _merge_watchlist(base: List[Tuple[str, List[str]]], override: List[dict]) -> List[Tuple[str, List[str]]]:
    merged = list(base)
    if not override:
        return merged
    existing = {name.lower() for name, _ in base}
    for entry in override:
        name = (entry.get("name") or "").strip()
        aliases = entry.get("aliases") or []
        if not name or not aliases:
            continue
        if name.lower() in existing:
            continue
        merged.append((name, list(aliases)))
        existing.add(name.lower())
    return merged


def _load_watchlist_overrides() -> Dict[str, List[Tuple[str, List[str]]]]:
    override_path = os.getenv("WATCHLIST_OVERRIDE_PATH") or str(Path(__file__).resolve().parents[3] / "watchlists_override.yaml")
    try:
        path = Path(override_path)
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
    except Exception:
        return {}
    out: Dict[str, List[Tuple[str, List[str]]]] = {}
    for key in ("kripto", "enerji", "teknoloji"):
        override = payload.get(key) or []
        if isinstance(override, list):
            out[key] = _merge_watchlist(WATCHLIST_BY_CATEGORY.get(key, []), override)
    return out


_override_watchlists = _load_watchlist_overrides()
if _override_watchlists:
    for cat, entries in _override_watchlists.items():
        WATCHLIST_BY_CATEGORY[cat] = entries


COUNTRY_ALIASES = {
    "United States": ["united states", "usa", "u.s.", "america"],
    "United Kingdom": ["united kingdom", "uk", "britain", "u.k."],
    "Germany": ["germany", "deutschland"],
    "France": ["france"],
    "Italy": ["italy"],
    "Russia": ["russia"],
    "China": ["china"],
    "India": ["india"],
    "Japan": ["japan"],
    "South Korea": ["south korea", "korea"],
    "Indonesia": ["indonesia"],
    "Saudi Arabia": ["saudi arabia", "saudi"],
    "United Arab Emirates": ["united arab emirates", "uae"],
    "Iran": ["iran"],
    "Brazil": ["brazil"],
    "Canada": ["canada"],
    "Mexico": ["mexico"],
    "Argentina": ["argentina"],
    "Türkiye": ["türkiye", "turkey"],
}


EVENT_RULES = [
    ("EARNINGS_GUIDANCE", [r"earnings", r"results", r"revenue", r"\beps\b", r"guidance", r"outlook", r"forecast", r"margin"]),
    ("REGULATION_LEGAL", [r"\bsec\b", r"\bkap\b", r"investigation", r"lawsuit", r"settlement", r"fine", r"antitrust", r"regulator"]),
    ("MNA", [r"acquire", r"merger", r"deal", r"buyout", r"takeover", r"sale", r"divest"]),
    ("CAPEX_INVESTMENT", [r"capex", r"investment", r"data center", r"factory", r"\bfab\b", r"plant", r"expansion", r"buildout"]),
    ("SANCTIONS_GEOPOLITICS", [r"sanction", r"export controls", r"tariff", r"embargo", r"ceasefire", r"conflict", r"pipeline attack"]),
    ("ENERGY_SUPPLY_OPEC", [r"\bopec\b", r"output", r"cut", r"supply disruption", r"\blng\b", r"refinery", r"strike", r"inventory"]),
    ("MACRO_RATES_INFLATION", [r"\bcpi\b", r"\bpce\b", r"inflation", r"rate cut", r"rate hike", r"\bfed\b", r"\becb\b", r"bond yields", r"\bdxy\b"]),
    ("CRYPTO_MARKET_STRUCTURE", [r"\betf\b", r"custody", r"exchange", r"stablecoin", r"market structure", r"\bmica\b", r"etf inflow", r"etf outflow"]),
    ("SECURITY_INCIDENT", [r"hack", r"breach", r"exploit", r"ransomware", r"outage"]),
    ("PRODUCT_PLATFORM", [r"launch", r"release", r"partnership", r"platform", r"\bapi\b", r"chip", r"model", r"datacenter gpu"]),
]

EVENT_LABELS_TR = {
    "EARNINGS_GUIDANCE": "kazanç/rehberlik",
    "CAPEX_INVESTMENT": "yatırım/capex",
    "MNA": "birleşme/satın alma",
    "REGULATION_LEGAL": "regülasyon/hukuk",
    "SANCTIONS_GEOPOLITICS": "jeopolitik/yaptırım",
    "ENERGY_SUPPLY_OPEC": "enerji arzı/OPEC",
    "MACRO_RATES_INFLATION": "makro/faiz-enflasyon",
    "CRYPTO_MARKET_STRUCTURE": "kripto piyasa yapısı",
    "SECURITY_INCIDENT": "güvenlik olayı",
    "PRODUCT_PLATFORM": "ürün/platform",
    "OTHER": "diğer",
}

STATE_REGIONS = {
    "AB": [
        "european union",
        "eurozone",
        "brussels",
        "germany",
        "france",
        "italy",
        "spain",
        "poland",
        "eu ",
        "avrupa birligi",
        "ab ",
    ],
    "ABD": [
        "united states",
        "usa",
        "u.s.",
        "washington",
        "white house",
        "federal reserve",
        "fed ",
        "abd",
    ],
    "Asya": [
        "china",
        "beijing",
        "japan",
        "tokyo",
        "boj",
        "pboc",
        "india",
        "seoul",
        "korea",
        "singapore",
        "asya",
    ],
    "Ortadogu": [
        "middle east",
        "iran",
        "iraq",
        "israel",
        "saudi",
        "uae",
        "qatar",
        "yemen",
        "turkey",
        "turkiye",
        "gaza",
        "syria",
        "ortadogu",
    ],
}


def _build_entity_index():
    index: Dict[str, List[dict]] = {}
    weights: Dict[Tuple[str, str], int] = {}
    for category, entries in WATCHLIST_BY_CATEGORY.items():
        size = len(entries)
        for rank, (name, aliases) in enumerate(entries, start=1):
            weight = max(4, int(round(40 * (1 - (rank - 1) / (size + 3)))))
            weights[(category, name)] = weight
            for alias in aliases:
                key = alias.lower()
                index.setdefault(key, []).append(
                    {
                        "name": name,
                        "category": category,
                        "rank": rank,
                        "alias": alias,
                    }
                )
    return index, weights


ENTITY_INDEX, ENTITY_WEIGHTS = _build_entity_index()


def safe_query(text: str) -> str:
    cleaned = text.replace("(", " ").replace(")", " ").replace('"', " ")
    cleaned = re.sub(r"\bAND\b|\bOR\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:360]


def safe_query_with_filters(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    return cleaned[:360]


def select_queries(queries: List[str], limit: int = MAX_QUERIES_PER_SPAN) -> List[str]:
    if limit <= 0:
        return []
    cleaned: List[str] = []
    seen: set[str] = set()
    for q in queries:
        token = (q or "").strip()
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(token)
    if len(cleaned) <= limit:
        return cleaned
    selected = [cleaned[0]]
    if limit == 1:
        return selected
    rest = cleaned[1:]
    slots = limit - 1
    if len(rest) <= slots:
        selected.extend(rest)
        return selected
    for i in range(slots):
        if slots == 1:
            idx = 0
        else:
            idx = round(i * (len(rest) - 1) / (slots - 1))
        candidate = rest[idx]
        if candidate not in selected:
            selected.append(candidate)
    for candidate in rest:
        if len(selected) >= limit:
            break
        if candidate not in selected:
            selected.append(candidate)
    return selected[:limit]


def _count_hits(text: str, keywords: List[str]) -> int:
    hits = 0
    for kw in keywords:
        if not kw:
            continue
        if " " in kw:
            if kw in text:
                hits += 1
        else:
            if re.search(rf"\b{re.escape(kw)}\b", text):
                hits += 1
    return hits


def score_sector(title: str) -> Tuple[str | None, int, int, int]:
    title_lower = title.lower()
    best_sector = None
    best_score = -9999
    best_required = 0
    best_boost = 0
    best_order = 9999
    for sector, rules in SECTOR_RULES.items():
        required_hits = _count_hits(title_lower, [k.lower() for k in rules.get("required", [])])
        boost_hits = _count_hits(title_lower, [k.lower() for k in rules.get("boost", [])])
        exclude_hits = _count_hits(title_lower, [k.lower() for k in rules.get("exclude", [])])
        if required_hits == 0:
            continue
        score = (10 * required_hits) + (3 * boost_hits) - (5 * exclude_hits)
        order = SECTOR_ORDER.index(sector) if sector in SECTOR_ORDER else 9999
        if score > best_score:
            best_sector = sector
            best_score = score
            best_required = required_hits
            best_boost = boost_hits
            best_order = order
            continue
        if score == best_score:
            if required_hits > best_required:
                best_sector = sector
                best_score = score
                best_required = required_hits
                best_boost = boost_hits
                best_order = order
            elif required_hits == best_required:
                if boost_hits > best_boost:
                    best_sector = sector
                    best_score = score
                    best_required = required_hits
                    best_boost = boost_hits
                    best_order = order
                elif boost_hits == best_boost and order < best_order:
                    best_sector = sector
                    best_score = score
                    best_required = required_hits
                    best_boost = boost_hits
                    best_order = order

    return best_sector, best_score, best_required, best_boost


def has_non_latin_heavy(title: str) -> bool:
    total = len(re.findall(r"\S", title))
    ascii_count = len(re.findall(r"[A-Za-z0-9ĞÜŞİÖÇğüşıöç]", title))
    if total == 0:
        return False
    return ascii_count / total < 0.18


def extract_domain(url: str) -> str:
    try:
        netloc = urlparse(url).netloc
    except Exception:
        return ""
    return netloc.lower().replace("www.", "")


def tier_score(url: str) -> int:
    domain = extract_domain(url)
    if domain in TIER_A:
        return 25
    if domain in TIER_B:
        return 15
    return 0


def tier_weight(domain: str) -> float:
    if domain in TIER_A:
        return TIER_WEIGHTS["A"]
    if domain in TIER_B:
        return TIER_WEIGHTS["B"]
    return TIER_WEIGHTS["C"]


def allow_backstop_domain(domain: str) -> bool:
    if "T1" in BACKSTOP_DOMAIN_TIER and domain in TIER_A:
        return True
    if "T2" in BACKSTOP_DOMAIN_TIER and domain in TIER_B:
        return True
    return False


def normalize_rank_weights(weights: dict | None) -> dict:
    if not weights:
        return dict(DEFAULT_NEWS_RANK_WEIGHTS)

    def _coerce(name: str) -> float:
        default = DEFAULT_NEWS_RANK_WEIGHTS[name]
        try:
            value = float(weights.get(name, default))
        except Exception:
            value = default
        return max(0.0, value)

    raw = {
        "relevance": _coerce("relevance"),
        "quality": _coerce("quality"),
        "impact": _coerce("impact"),
        "scope": _coerce("scope"),
    }
    total = sum(raw.values())
    if total <= 0:
        return dict(DEFAULT_NEWS_RANK_WEIGHTS)
    return {name: value / total for name, value in raw.items()}


def final_rank_score(item: NewsItem, weights: dict | None = None) -> float:
    # All inputs are expected to be on a 0..100 scale.
    w = normalize_rank_weights(weights)
    impact_person = float(getattr(item, "impact_potential", 0) or 0)
    impact_sector = float(getattr(item, "max_sector_impact", 0) or 0)
    impact = max(impact_person, impact_sector)
    scope = float(getattr(item, "scope_score", 0) or 0)
    return (
        (w["relevance"] * float(item.relevance_score))
        + (w["quality"] * float(item.quality_score))
        + (w["impact"] * impact)
        + (w["scope"] * scope)
    )


def tag_title(title: str) -> Tuple[List[str], int]:
    tags: List[str] = []
    score = 0
    for tag, w, patterns in TAG_RULES:
        if any(re.search(p, title, flags=re.IGNORECASE) for p in patterns):
            tags.append(tag)
            score += w
    if re.search(r"bitcoin|ethereum|crypto|stablecoin|etf", title, flags=re.IGNORECASE):
        score += 1
    if re.search(r"breaking|exclusive|urgent", title, flags=re.IGNORECASE):
        score += 1
    return tags, score


def _all_persons() -> List[Tuple[str, str]]:
    out = []
    for group, names in PERSONAL_GROUPS.items():
        for name in names:
            out.append((name, group))
    return out


ALL_PERSONS = _all_persons()


def build_personal_queries(chunk_size: int = 6) -> List[str]:
    names = [name for name, _ in ALL_PERSONS]
    chunks = [names[i:i + chunk_size] for i in range(0, len(names), chunk_size)]
    aliases = " ".join(PERSONAL_TITLE_ALIASES)
    queries = []
    for chunk in chunks:
        queries.append(f"{' '.join(chunk)} {aliases} quote said remarks statement")
    return queries


def match_persons(title: str) -> Tuple[List[str], List[str], int]:
    title_lower = title.lower()
    persons: List[str] = []
    groups: List[str] = []
    boost = 0
    for name, group in ALL_PERSONS:
        if name.lower() in title_lower:
            canonical = canonical_person_name(name)
            persons.append(canonical)
            if group not in groups:
                groups.append(group)
                if group == "CENTRAL_BANK_HEADS":
                    boost += 12
                elif group == "EU_OFFICIALS":
                    boost += 10
                elif group == "REGIONAL_POWER_LEADERS":
                    boost += 8

    if persons and any(k in title_lower for k in PERSONAL_KEYWORDS):
        boost += 5
    return persons, groups, boost


def match_persons_detailed(title: str) -> Tuple[List[dict], int]:
    title_lower = title.lower()
    matched: Dict[str, dict] = {}
    boost = 0
    for name, group in ALL_PERSONS:
        if name.lower() in title_lower:
            canonical = canonical_person_name(name)
            pid = person_id(canonical)
            if pid not in matched:
                matched[pid] = {
                    "name": canonical,
                    "person_id": pid,
                    "group": group,
                    "group_id": group.lower(),
                }
            if group == "CENTRAL_BANK_HEADS":
                boost += 12
            elif group == "EU_OFFICIALS":
                boost += 10
            elif group == "REGIONAL_POWER_LEADERS":
                boost += 8

    if matched and any(k in title_lower for k in PERSONAL_KEYWORDS):
        boost += 5
    return list(matched.values()), boost


def extract_countries(title: str) -> List[str]:
    title_lower = title.lower()
    out = []
    for name, aliases in COUNTRY_ALIASES.items():
        if any(a in title_lower for a in aliases):
            out.append(name)
    return out


def detect_sector(title: str) -> Tuple[str | None, int, int, int]:
    return score_sector(title)


def token_set_ratio(a: str, b: str) -> float:
    tokens_a = set(re.findall(r"\w+", a.lower()))
    tokens_b = set(re.findall(r"\w+", b.lower()))
    if not tokens_a or not tokens_b:
        return 0.0
    intersect = " ".join(sorted(tokens_a & tokens_b))
    combined_a = " ".join(sorted(tokens_a))
    combined_b = " ".join(sorted(tokens_b))
    ratio_a = difflib.SequenceMatcher(None, intersect, combined_a).ratio()
    ratio_b = difflib.SequenceMatcher(None, intersect, combined_b).ratio()
    return max(ratio_a, ratio_b)


def parse_any_date(value: str | None) -> datetime | None:
    if not value:
        return None
    value = str(value).strip()
    if re.fullmatch(r"\d{14}", value):
        try:
            dt = datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None
    try:
        if value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return datetime.fromisoformat(value)
    except Exception:
        try:
            dt = parsedate_to_datetime(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None


def to_iso(dt: datetime | None) -> str | None:
    if not dt:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def format_ts_tsi(dt: datetime | None) -> str | None:
    if not dt:
        return None
    try:
        local = dt.astimezone(ZoneInfo("Europe/Istanbul"))
    except Exception:
        local = dt
    months = [
        "Ocak",
        "Şubat",
        "Mart",
        "Nisan",
        "Mayıs",
        "Haziran",
        "Temmuz",
        "Ağustos",
        "Eylül",
        "Ekim",
        "Kasım",
        "Aralık",
    ]
    month = months[local.month - 1]
    return f"{local.day} {month} {local.year}, {local.hour:02d}:{local.minute:02d} TSİ"


def canonical_title(title: str) -> str:
    lowered = title.lower()
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_KEYS = {
    "ref",
    "ref_src",
    "source",
    "src",
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "cmpid",
    "spm",
    "igshid",
    "mkt_tok",
    "yclid",
}


def canonicalize_url(url: str | None) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return url
    if not parsed.scheme or not parsed.netloc:
        return url
    filtered = []
    for key, val in parse_qsl(parsed.query or "", keep_blank_values=False):
        k = key.lower()
        if any(k.startswith(p) for p in TRACKING_QUERY_PREFIXES):
            continue
        if k in TRACKING_QUERY_KEYS:
            continue
        filtered.append((key, val))
    clean_query = urlencode(filtered, doseq=True)
    cleaned = parsed._replace(query=clean_query, fragment="")
    return urlunparse(cleaned)


def time_bucket(dt: datetime | None) -> str:
    if not dt:
        return "unknown"
    dt = dt.astimezone(timezone.utc)
    minute = (dt.minute // TIME_BUCKET_MINUTES) * TIME_BUCKET_MINUTES
    bucket_dt = dt.replace(minute=minute, second=0, microsecond=0)
    return bucket_dt.strftime("%Y%m%d%H%M")


def build_cluster_id(key: str) -> str:
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:12]


def alias_match(title_lower: str, alias: str) -> bool:
    alias_lower = alias.lower()
    if " " in alias_lower or "." in alias_lower or "/" in alias_lower:
        return alias_lower in title_lower
    pattern = r"\b" + re.escape(alias_lower) + r"\b"
    return re.search(pattern, title_lower) is not None


def tokenize_title(title: str) -> List[str]:
    return re.findall(r"[A-Za-z0-9]+", title)


def has_strong_symbol_context(title: str, symbol: str, is_crypto: bool) -> bool:
    if not symbol:
        return False
    if re.search(rf"\({re.escape(symbol)}\)", title):
        return True
    if re.search(rf"\${re.escape(symbol)}\b", title):
        return True
    tokens = tokenize_title(title)
    lower_tokens = [t.lower() for t in tokens]
    if symbol.lower() in lower_tokens and "%" in title:
        return True
    for idx, tok in enumerate(lower_tokens):
        if tok != symbol.lower():
            continue
        start = max(0, idx - 6)
        end = min(len(lower_tokens), idx + 7)
        window = set(lower_tokens[start:end])
        if window & EQUITY_CONTEXT_WORDS:
            return True
        if is_crypto and window & CRYPTO_CONTEXT_WORDS:
            return True
    return False


def detect_ticker_alias(aliases: List[str]) -> str | None:
    for alias in aliases:
        if alias.isupper() and alias.isalpha() and 1 <= len(alias) <= 5:
            return alias
    for alias in aliases:
        if any(ch.isdigit() for ch in alias) and "." in alias:
            return alias
    return None


def is_name_distinct(name: str, ticker: str | None) -> bool:
    if not ticker:
        return True
    return name.lower() != ticker.lower()


def ambiguous_ticker_allowed(ticker: str, title_lower: str) -> bool:
    if ticker == "COP" and re.search(r"cop2\d", title_lower):
        return False
    if ticker == "BP":
        return any(k in title_lower for k in ["plc", "oil", "energy major", "london shares", "bp plc"])
    if ticker == "SO":
        return "southern company" in title_lower or "utility" in title_lower
    if ticker == "AI":
        return "c3.ai" in title_lower or "c3 ai" in title_lower
    if ticker == "META":
        return any(k in title_lower for k in ["meta platforms", "facebook", "instagram"])
    if ticker == "ORCL":
        return "oracle" in title_lower
    return True


def is_ambiguous_ticker(alias: str) -> bool:
    if not alias.isupper():
        return False
    if not alias.isalpha():
        return False
    return len(alias) <= 3


def context_match(title_lower: str, category: str, name: str) -> bool:
    if name.lower() in title_lower:
        return True
    if any(k in title_lower for k in CATEGORY_CONTEXT.get(category, [])):
        return True
    if any(k in title_lower for k in ["shares", "stock", "earnings", "guidance", "ceo"]):
        return True
    return False


def match_entities_detailed(
    title: str, entity_index_override: Dict[str, List[dict]] | None = None
) -> Tuple[List[str], Dict[str, int], Dict[str, int], Dict[str, dict]]:
    title_lower = title.lower()
    hits: Dict[str, List[dict]] = {}
    entity_match_meta: Dict[str, dict] = {}
    index = entity_index_override or ENTITY_INDEX

    for alias, entries in index.items():
        if not alias_match(title_lower, alias):
            continue
        for ent in entries:
            alias_text = ent["alias"]
            if is_ambiguous_ticker(alias_text) and not context_match(title_lower, ent["category"], ent["name"]):
                continue
            hits.setdefault(ent["name"], []).append(ent)
            meta = entity_match_meta.setdefault(
                ent["name"],
                {"method": "alias", "aliases_hit": [], "category": ent["category"], "score": 0},
            )
            meta["aliases_hit"].append(alias_text)

    entities = sorted(hits.keys())
    category_scores = {"kripto": 0, "enerji": 0, "teknoloji": 0}
    entity_scores: Dict[str, int] = {}

    for name, entries in hits.items():
        best = None
        for ent in entries:
            weight = ENTITY_WEIGHTS.get((ent["category"], ent["name"]), 4)
            if best is None or weight > best[0]:
                best = (weight, ent["category"])
        if best:
            entity_scores[name] = best[0]
            category_scores[best[1]] += best[0]
            if name in entity_match_meta:
                entity_match_meta[name]["score"] = best[0]
                entity_match_meta[name]["category"] = best[1]

    for category, keys in CATEGORY_CONTEXT.items():
        if any(k in title_lower for k in keys):
            category_scores[category] += 6

    if "Tesla" in hits:
        if any(k in title_lower for k in CATEGORY_CONTEXT["kripto"]):
            category_scores["kripto"] += 12
        else:
            category_scores["teknoloji"] += 8

    return entities, entity_scores, category_scores, entity_match_meta


def match_entities(
    title: str, entity_index_override: Dict[str, List[dict]] | None = None
) -> Tuple[List[str], Dict[str, int], Dict[str, int]]:
    entities, entity_scores, category_scores, _ = match_entities_detailed(title, entity_index_override)
    return entities, entity_scores, category_scores


def match_companies_detailed(title: str) -> Tuple[List[CompanyEntity], Dict[str, int], List[str]]:
    title_lower = title.lower()
    company_map: Dict[str, CompanyEntity] = {}
    entity_scores: Dict[str, int] = {}
    debug_flags: List[str] = []

    for entries in WATCHLIST_BY_CATEGORY.values():
        for name, aliases in entries:
            name_lower = name.lower()
            name_match = name_lower in title_lower
            alias_hits = [alias for alias in aliases if alias_match(title_lower, alias)]
            if not name_match and not alias_hits:
                continue

            ticker = detect_ticker_alias(aliases)
            ticker_match = ticker in alias_hits if ticker else False
            strong = False
            if ticker_match:
                strong = has_strong_symbol_context(title, ticker, False)

            if ticker and ticker in AMBIGUOUS_TICKERS:
                if not ambiguous_ticker_allowed(ticker, title_lower):
                    continue
                if ticker_match and not strong and not (name_match and is_name_distinct(name, ticker)):
                    continue
                if ticker_match and strong and not name_match:
                    debug_flags.append("AMBIGUOUS_TICKER")

            if ticker_match and not strong and ticker in AMBIGUOUS_TICKERS and not name_match:
                continue

            if strong and name_match and is_name_distinct(name, ticker):
                confidence = 85
            elif strong and not name_match:
                confidence = 50 if ticker in AMBIGUOUS_TICKERS else 70
            elif name_match:
                confidence = 60
            else:
                confidence = 45

            match_type = "strong" if strong else "weak"
            if ticker in AMBIGUOUS_TICKERS and match_type == "weak":
                debug_flags.append("AMBIGUOUS_TICKER")

            existing = company_map.get(name)
            if existing is None or confidence > existing.confidence:
                company_map[name] = CompanyEntity(
                    name=name,
                    ticker=ticker,
                    confidence=confidence,
                    match_type=match_type,
                )
            entity_scores[name] = max(entity_scores.get(name, 0), confidence)

    companies = sorted(company_map.values(), key=lambda c: (-c.confidence, c.name))
    return companies, entity_scores, list(dict.fromkeys(debug_flags))


def match_crypto_entities(title: str) -> Tuple[List[CryptoEntity], List[str]]:
    title_lower = title.lower()
    tokens = tokenize_title(title)
    lower_tokens = [t.lower() for t in tokens]
    debug_flags: List[str] = []
    out: List[CryptoEntity] = []

    for symbol, aliases in CRYPTO_SYMBOLS.items():
        symbol_lower = symbol.lower()
        symbol_match = symbol_lower in lower_tokens
        alias_match_any = any(alias in title_lower for alias in aliases)
        if not symbol_match and not alias_match_any:
            continue

        strong = has_strong_symbol_context(title, symbol, True)
        if symbol in AMBIGUOUS_CRYPTO:
            if "sol" == symbol_lower and any(k in title_lower for k in ["solar", "renewable"]):
                continue
            if "dot" == symbol_lower and not alias_match_any and not strong:
                continue
            if not strong and not any(k in title_lower for k in CRYPTO_CONTEXT_WORDS):
                continue

        if strong:
            confidence = 78 if symbol in AMBIGUOUS_CRYPTO else 80
            match_type = "strong"
        else:
            confidence = 55
            match_type = "weak"

        out.append(
            CryptoEntity(
                symbol=symbol,
                coingecko_id=COINGECKO_IDS.get(symbol),
                confidence=confidence,
                match_type=match_type,
            )
        )

    return out, debug_flags


def match_countries_detailed(title: str, category: str) -> Tuple[List[CountryEntity], List[CountryTag], int, List[str]]:
    title_lower = title.lower()
    debug_flags: List[str] = []
    countries: List[CountryEntity] = []
    tags: List[CountryTag] = []
    country_confidence = 0

    for name, aliases in COUNTRY_ALIASES.items():
        hit_count = 0
        for alias in aliases:
            if alias in title_lower:
                hit_count += 1
        if hit_count == 0:
            continue

        ambiguous = name.lower() in AMBIGUOUS_COUNTRIES
        context_ok = any(k in title_lower for k in COUNTRY_CONTEXT_WORDS)
        iso2, region = COUNTRY_META.get(name, (None, None))

        if ambiguous and not context_ok:
            if category != "REGIONAL":
                continue
            confidence = 30
            match_type = "weak"
            debug_flags.append("AMBIGUOUS_COUNTRY")
        else:
            confidence = 80 if context_ok else 60
            match_type = "strong" if context_ok else "weak"

        countries.append(
            CountryEntity(
                country=name,
                iso2=iso2,
                confidence=confidence,
                match_type=match_type,
            )
        )
        tags.append(
            CountryTag(
                country=name,
                iso2=iso2,
                region=region,
                hit_count=hit_count,
                confidence=confidence,
            )
        )
        country_confidence = max(country_confidence, confidence)

    return countries, tags, country_confidence, list(dict.fromkeys(debug_flags))


def build_person_entities(title: str) -> List[PersonEntity]:
    title_lower = title.lower()
    out: List[PersonEntity] = []
    for name, group in ALL_PERSONS:
        if name.lower() not in title_lower:
            continue
        out.append(
            PersonEntity(
                name=name,
                group=group,
                org=group,
                country=None,
                confidence=70,
            )
        )
    return out


def detect_category(category_scores: Dict[str, int]) -> str:
    if max(category_scores.values()) > 0:
        return max(category_scores.items(), key=lambda x: x[1])[0]
    return "kripto"


def detect_event_type(title: str) -> str:
    for event_type, patterns in EVENT_RULES:
        if any(re.search(p, title, flags=re.IGNORECASE) for p in patterns):
            return event_type
    return "OTHER"


def has_positive_hint(title: str) -> bool:
    return bool(re.search(r"\bbeat|beats|raised|raise|upgrade|strong|record|surge|profit\b", title, re.IGNORECASE))


def has_negative_hint(title: str) -> bool:
    return bool(re.search(r"\bmiss|cut|downgrade|warn|weak|loss|fine|probe|ban\b", title, re.IGNORECASE))


def direction_text(label: str, reason: str) -> str:
    return f"{label}: {reason}"


def impact_map(event_type: str, title: str, category: str) -> Tuple[List[str], List[str], str]:
    title_lower = title.lower()
    channels: List[str] = []
    bias: List[str] = []
    expected = "karışık/belirsiz"
    reason = "veri eksik: yön çıkarımı için yeterli ipucu yok"

    if event_type == "EARNINGS_GUIDANCE":
        channels = ["büyüme"]
        if re.search(r"buyback|free cash flow|fcf", title_lower):
            channels.append("likidite")
        if has_positive_hint(title):
            bias = ["risk_on"]
            expected = "pozitif"
            reason = "beklentiyi aşan guidance büyüme algısını artırır"
        elif has_negative_hint(title):
            bias = ["risk_off"]
            expected = "negatif"
            reason = "zayıf rehberlik risk algısını artırır"
    elif event_type == "CAPEX_INVESTMENT":
        channels = ["büyüme", "arz_zinciri"]
        if category == "teknoloji":
            bias = ["risk_on", "crypto_beta_up"]
            expected = "pozitif"
            reason = "gelecek talep/kapasite artırımı büyüme beklentisi yaratır"
        elif category == "enerji":
            bias = ["energy_up"]
            if re.search(r"increase|boost|expand|capacity", title_lower):
                expected = "karışık/belirsiz"
                reason = "arz artışı fiyat baskısı yaratabilir"
            else:
                expected = "pozitif"
                reason = "arz tarafında yatırım beklentiyi güçlendirir"
        else:
            bias = ["risk_on"]
            expected = "pozitif"
            reason = "gelecek talep/kapasite artırımı büyüme beklentisi yaratır"
    elif event_type == "MNA":
        channels = ["likidite", "büyüme"]
        bias = ["risk_on"]
        if re.search(r"all-cash|cash deal", title_lower):
            expected = "pozitif"
            reason = "nakit odaklı anlaşma sinerji beklentisini güçlendirir"
        elif re.search(r"leverag|debt", title_lower):
            expected = "negatif"
            reason = "yüksek kaldıraç finansman riski yaratır"
        else:
            expected = "pozitif"
            reason = "sinerji/büyüme beklentisi oluşur"
    elif event_type == "REGULATION_LEGAL":
        channels = ["regülasyon_baskısı", "risk_primi"]
        bias = ["risk_off"]
        if category == "kripto":
            bias.append("crypto_risk_off")
        if re.search(r"clarity|approved|approval", title_lower):
            expected = "karışık/belirsiz"
            reason = "net kurallar uzun vadede olumlu olabilir"
        else:
            expected = "negatif"
            reason = "uyum maliyeti/ceza riski risk primini artırır"
    elif event_type == "SANCTIONS_GEOPOLITICS":
        channels = ["risk_primi"]
        bias = ["risk_off"]
        if any(k in title_lower for k in CATEGORY_CONTEXT["enerji"]):
            channels.append("enflasyon")
            bias.append("energy_up")
            expected = "pozitif"
            reason = "arz riski petrol/gaz fiyatlarını yukarı iter"
        else:
            expected = "negatif"
            reason = "jeopolitik belirsizlik risk primini artırır"
    elif event_type == "ENERGY_SUPPLY_OPEC":
        channels = ["enerji_maliyeti", "enflasyon", "risk_primi"]
        bias = ["energy_up"]
        if re.search(r"cut|disruption|strike", title_lower):
            bias.append("rates_up")
            expected = "negatif"
            reason = "arz kısıtı enflasyon baskısı yaratır"
        elif re.search(r"increase|boost|output", title_lower):
            expected = "karışık/belirsiz"
            reason = "arz artışı fiyat baskısı yaratabilir"
        else:
            expected = "karışık/belirsiz"
            reason = "veri eksik: yön çıkarımı için yeterli ipucu yok"
    elif event_type == "MACRO_RATES_INFLATION":
        channels = ["faiz", "likidite", "risk_primi"]
        if re.search(r"rate hike|hawkish|inflation|cpi|pce", title_lower):
            bias = ["usd_up", "rates_up", "risk_off"]
            expected = "negatif"
            reason = "faizler riskli varlıkların iskonto oranını yükseltir"
        elif re.search(r"rate cut|dovish|easing", title_lower):
            bias = ["risk_on", "crypto_beta_up"]
            expected = "pozitif"
            reason = "likidite beklentisi riskli varlık talebini artırır"
    elif event_type == "CRYPTO_MARKET_STRUCTURE":
        if re.search(r"inflow|approval|launch", title_lower):
            channels = ["likidite"]
            bias = ["crypto_beta_up"]
            expected = "pozitif"
            reason = "erisim/kurumsal talep artisi"
        else:
            channels = ["regülasyon_baskısı"]
            bias = ["crypto_risk_off"]
            expected = "negatif"
            reason = "regülasyon baskısı risk primini artırır"
    elif event_type == "SECURITY_INCIDENT":
        channels = ["güvenlik_operasyonel_risk", "risk_primi"]
        bias = ["risk_off"]
        expected = "negatif"
        reason = "operasyonel kesinti ve itibar riski"
    elif event_type == "PRODUCT_PLATFORM":
        channels = ["büyüme", "arz_zinciri"]
        bias = ["risk_on"]
        expected = "pozitif"
        reason = "ürün ivmesi gelir beklentisini artırır"

    if expected == "karışık/belirsiz" and "veri eksik" not in reason:
        reason = "veri eksik: yön çıkarımı için yeterli ipucu yok"

    return channels, bias, direction_text(expected, reason)


def topic_match_score(title: str) -> int:
    topics = [
        r"earnings|guidance|outlook|results|forecast",
        r"capex|investment|data center|factory|fab|plant|expansion|buildout",
        r"acquire|merger|deal|buyout|takeover|sale|divest",
        r"regulat|lawsuit|settlement|fine|antitrust|investigation",
        r"sanction|export controls|tariff|embargo|ceasefire|conflict",
        r"opec|supply disruption|refinery|inventory|output cut",
        r"cpi|pce|inflation|rate cut|rate hike|fed|ecb|bond yields|dxy",
        r"etf|inflow|outflow|stablecoin|market structure",
        r"hack|breach|exploit|ransomware|outage",
    ]
    hits = sum(1 for p in topics if re.search(p, title, flags=re.IGNORECASE))
    return min(30, hits * 10)


def recency_bonus(dt: datetime | None) -> int:
    if not dt:
        return NEWS_RECENCY_UNKNOWN_BONUS
    age = datetime.now(timezone.utc) - dt.astimezone(timezone.utc)
    hours = max(0.0, age.total_seconds() / 3600)
    bonus = int(round(20 * math.exp(-NEWS_RECENCY_BONUS_LAMBDA * hours)))
    if NEWS_RECENCY_BONUS_FLOOR > 0:
        bonus = max(NEWS_RECENCY_BONUS_FLOOR, bonus)
    return max(0, bonus)


def _tier_key(domain: str | None) -> str:
    if not domain:
        return "C"
    if domain in TIER_A:
        return "A"
    if domain in TIER_B:
        return "B"
    return "C"


def recency_decay(dt: datetime | None, domain: str | None = None) -> float:
    if not dt:
        return NEWS_RECENCY_UNKNOWN_DECAY
    age = datetime.now(timezone.utc) - dt.astimezone(timezone.utc)
    hours = max(0.0, age.total_seconds() / 3600)
    lam = NEWS_RECENCY_DECAY_LAMBDA
    if TIER_AS_DECAY_ENABLED and domain:
        lam *= TIER_DECAY_MODIFIERS.get(_tier_key(domain), 1.0)
    decay = math.exp(-lam * hours)
    decay = min(1.0, decay)
    return max(NEWS_RECENCY_DECAY_FLOOR, decay)


def regime_bonus(title: str, event_type: str) -> int:
    title_lower = title.lower()
    if event_type in {"MACRO_RATES_INFLATION", "ENERGY_SUPPLY_OPEC", "SANCTIONS_GEOPOLITICS"}:
        return 8
    if re.search(r"volatility|risk-off|risk on|crisis", title_lower):
        return 6
    if re.search(r"routine|minor update", title_lower):
        return -4
    return 0


def _entity_match_score(entities: List[str], entity_scores: Dict[str, int], best_default: int, cap: int) -> int:
    if not entities:
        return 0
    best = max(entity_scores.values()) if entity_scores else best_default
    extra = max(0, len(entities) - 1)
    bonus = int(round(NEWS_ENTITY_DIMINISH_WEIGHT * math.log1p(extra)))
    return min(cap, best + bonus)


def compute_relevance_score(
    title: str,
    entities: List[str],
    entity_scores: Dict[str, int],
    dt: datetime | None,
    event_type: str,
) -> int:
    entity_match = _entity_match_score(entities, entity_scores, best_default=10, cap=40)
    score = 50 + entity_match + topic_match_score(title) + recency_bonus(dt) + regime_bonus(title, event_type)
    score = max(0, min(100, int(round(score))))
    return score


def compute_quality_score(domain: str, dt: datetime | None, health_penalty: float) -> int:
    base = 100 * tier_weight(domain) * recency_decay(dt, domain) * (1 - health_penalty)
    return int(round(max(0, min(100, base))))


def compute_event_relevance_score(
    title: str,
    entities: List[str],
    entity_scores: Dict[str, int],
    dt: datetime | None,
) -> int:
    entity_match = _entity_match_score(entities, entity_scores, best_default=20, cap=50)
    score = entity_match + topic_match_score(title) + recency_bonus(dt)
    score = max(0, min(100, int(round(score))))
    return score


def build_short_summary(title: str, event_type: str, entities: List[str]) -> str:
    trimmed = title.strip()
    if len(trimmed) > 140:
        trimmed = trimmed[:137] + "..."
    label = EVENT_LABELS_TR.get(event_type, "diger")
    entity = entities[0] if entities else "ilgili varlik"
    return f'Başlıkta "{trimmed}" ifadesi öne çıkıyor. Haber, {entity} için {label} temalı olarak sınıflandı.'


def build_watch_queries(watchlist: List[str]) -> List[str]:
    extra = " ".join(watchlist) if watchlist else ""
    region_terms = _region_query_terms()
    extras = f"{EXTRA_QUERY_TERMS} {region_terms}".strip()
    queries = []
    for category, entries in WATCHLIST_BY_CATEGORY.items():
        top_names = " ".join([name for name, _ in entries[:6]])
        q = f"{BASE_INTENT} {CATEGORY_QUERY_KEYWORDS.get(category, '')} {top_names} {extra} {extras}".strip()
        queries.append(q)
    return queries


def build_fallback_queries() -> List[str]:
    return [
        f"bitcoin crypto ethereum etf stablecoin exchange outage {EXTRA_QUERY_TERMS}",
        "oil opec supply cut refinery lng pipeline sanctions",
        "ai chip semiconductor data center export controls",
    ]


def _region_query_terms(limit: int = 24) -> str:
    terms: List[str] = []
    for aliases in COUNTRY_ALIASES.values():
        for alias in aliases[:2]:
            if alias not in terms:
                terms.append(alias)
        if len(terms) >= limit:
            break
    return " ".join(terms)


def normalize_gdelt(raw: list[dict]) -> List[NewsItem]:
    items: List[NewsItem] = []
    for a in raw:
        title = (a.get("title") or "").strip()
        url = (a.get("url") or "").strip()
        domain = (a.get("domain") or "").strip().lower()
        description = (a.get("description") or a.get("snippet") or "").strip()
        if not title or not url:
            continue
        if domain in BLOCKLIST:
            continue
        if has_non_latin_heavy(title):
            continue
        if not domain:
            domain = extract_domain(url)
        items.append(
            NewsItem(
                title=title,
                url=url,
                source=domain,
                description=description or None,
                publishedAtISO=to_iso(parse_any_date(a.get("seendate"))),
            )
        )
    return items


def normalize_rss(raw: list[dict]) -> List[NewsItem]:
    items: List[NewsItem] = []
    for a in raw:
        title = (a.get("title") or "").strip()
        url = (a.get("url") or "").strip()
        description = (a.get("summary") or "").strip()
        if not title or not url:
            continue
        if has_non_latin_heavy(title):
            continue
        domain = extract_domain(url)
        if domain in BLOCKLIST:
            continue
        items.append(
            NewsItem(
                title=title,
                url=url,
                source=domain,
                description=description or None,
                publishedAtISO=to_iso(parse_any_date(a.get("published"))),
            )
        )
    return items


def collect_news(
    queries: List[str],
    timespan: str,
    maxrecords: int,
    timeout: float,
    watchlist: List[str] | None = None,
    min_required: int = MIN_NEWS,
    allow_extras: bool = True,
    skip_gdelt: bool = False,
) -> Tuple[List[NewsItem], List[str], float, dict, int]:
    notes: List[str] = []
    items: List[NewsItem] = []
    health_penalty = 0.0
    watchlist = watchlist or []
    min_required = max(1, min_required)
    provider_counts = {"gdelt": 0, "rss_google": 0, "press": 0, "tr": 0, "finnhub": 0}
    rss_blocklist_hits = 0

    selected = select_queries([safe_query(q) for q in queries if q], MAX_QUERIES_PER_SPAN)
    if not selected:
        selected = [safe_query("bitcoin crypto ethereum")]
    notes.append(f"query_count:{len(selected)}")

    gdelt_items: List[NewsItem] = []
    if skip_gdelt:
        notes.append("gdelt_skipped")
    else:
        def fetch_one(q: str):
            return search_gdelt(q, timespan, maxrecords, timeout)

        with ThreadPoolExecutor(max_workers=len(selected)) as ex:
            futures = {ex.submit(fetch_one, q): q for q in selected}
            for fut in as_completed(futures):
                raw, err = fut.result()
                if err:
                    notes.append(err)
                    health_penalty = max(health_penalty, 0.2)
                    if err == "gdelt_rate_limited":
                        break
                gdelt_items.extend(normalize_gdelt(raw))

        items.extend(gdelt_items)
        provider_counts["gdelt"] = len(gdelt_items)

    provider_used = "gdelt" if items else "none"
    sources_used: list[str] = ["gdelt"] if items else []

    if allow_extras and watchlist and len(items) < min_required:
        fh = fetch_finnhub_company_news(watchlist[:NEWS_EXTRA_MAX_TICKERS], timespan, timeout)
        if fh.ok and fh.data:
            fh_items = normalize_finnhub(fh.data)
            items.extend(fh_items)
            provider_counts["finnhub"] = len(fh_items)
            notes.append("finnhub_news")
            sources_used.append("finnhub")
        elif fh.error_msg:
            notes.append(f"finnhub_news_error:{fh.error_msg}")

    if allow_extras and len(items) < min_required:
        extra_feeds = (PRESS_RELEASE_FEEDS + TR_NEWS_FEEDS)[:NEWS_EXTRA_MAX_FEEDS]
        rss_raw: List[dict] = []
        sweep = max(1, min(len(selected), RSS_FALLBACK_QUERY_SWEEP))
        for q in selected[:sweep]:
            rss_raw.extend(search_rss(q, maxrecords, timespan=timespan, extra_feeds=extra_feeds))
        if rss_raw:
            for raw in rss_raw:
                url = (raw.get("url") or "").strip()
                domain = extract_domain(url)
                if domain in BLOCKLIST:
                    rss_blocklist_hits += 1
        rss_items = normalize_rss(rss_raw)
        if rss_items:
            for it in rss_items:
                domain = extract_domain(it.url or "")
                if domain in PRESS_DOMAINS:
                    provider_counts["press"] += 1
                elif domain in TR_DOMAINS:
                    provider_counts["tr"] += 1
                else:
                    provider_counts["rss_google"] += 1
            notes.append("rss_fallback")
            health_penalty = max(health_penalty, 0.15)
            items.extend(rss_items)
            sources_used.append("rss")
            provider_used = "rss"

    # Local/BIST-focused RSS sweep using watchlist symbols
    if allow_extras and watchlist:
        local_max_feeds = int(os.getenv("LOCAL_RSS_MAX_FEEDS", "4") or 4)
        local_max_terms = int(os.getenv("LOCAL_RSS_MAX_TERMS", "4") or 4)
        local_extra_feeds = TR_NEWS_FEEDS[:local_max_feeds]
        local_terms = []
        for sym in watchlist[: min(len(watchlist), 4)]:
            local_terms.append(sym)
        local_terms.extend(_load_bist_alias_terms()[:6])
        for term in local_terms[:local_max_terms]:
            q = f"{term} BIST"
            rss_raw = search_rss_local(
                q,
                max_items=min(8, maxrecords),
                extra_feeds=local_extra_feeds,
                strict=True,
                timespan=timespan,
                timeout=min(6.0, timeout),
            )
            rss_items = normalize_rss(rss_raw)
            if rss_items:
                for it in rss_items:
                    domain = extract_domain(it.url or "")
                    if domain in TR_DOMAINS:
                        provider_counts["tr"] += 1
                    else:
                        provider_counts["rss_google"] += 1
                notes.append("rss_local_bist")
                items.extend(rss_items)
                sources_used.append("rss")

    if sources_used:
        provider_used = "+".join(sorted(set(sources_used)))
    notes.append(f"provider_used:{provider_used}")

    return items, notes, health_penalty, provider_counts, rss_blocklist_hits


def collect_news_extras(
    queries: List[str],
    timespan: str,
    maxrecords: int,
    timeout: float,
    watchlist: List[str],
) -> Tuple[List[NewsItem], List[str], dict, int, dict]:
    notes: List[str] = []
    items: List[NewsItem] = []
    provider_counts = {"gdelt": 0, "rss_google": 0, "press": 0, "tr": 0, "finnhub": 0}
    rss_blocklist_hits = 0
    statuses = {"finnhub": "disabled", "press": "disabled", "tr": "disabled"}

    if watchlist:
        fh = fetch_finnhub_company_news(watchlist[:NEWS_EXTRA_MAX_TICKERS], timespan, timeout)
        if fh.ok and fh.data:
            fh_items = normalize_finnhub(fh.data)
            provider_counts["finnhub"] = len(fh_items)
            items.extend(fh_items)
            statuses["finnhub"] = "ok"
        elif fh.error_msg:
            if "missing_api_key" in fh.error_msg:
                statuses["finnhub"] = "missing_key"
            elif "rate" in fh.error_msg or "429" in fh.error_msg:
                statuses["finnhub"] = "rate_limited"
            else:
                statuses["finnhub"] = "error"
            notes.append(f"finnhub_news_error:{fh.error_msg}")

    selected = select_queries([safe_query(q) for q in queries if q], max(1, min(MAX_QUERIES_PER_SPAN, RSS_FALLBACK_QUERY_SWEEP)))
    if not selected:
        selected = [safe_query("bitcoin crypto ethereum")]

    extra_feeds = (PRESS_RELEASE_FEEDS + TR_NEWS_FEEDS)[:NEWS_EXTRA_MAX_FEEDS]
    if extra_feeds:
        rss_raw: List[dict] = []
        for q in selected:
            rss_raw.extend(search_rss(q, maxrecords, timespan=timespan, extra_feeds=extra_feeds))
        if rss_raw:
            for raw in rss_raw:
                url = (raw.get("url") or "").strip()
                domain = extract_domain(url)
                if domain in BLOCKLIST:
                    rss_blocklist_hits += 1
        rss_items = normalize_rss(rss_raw)
        if rss_items:
            for it in rss_items:
                domain = extract_domain(it.url or "")
                if domain in PRESS_DOMAINS:
                    provider_counts["press"] += 1
                elif domain in TR_DOMAINS:
                    provider_counts["tr"] += 1
                else:
                    provider_counts["rss_google"] += 1
            items.extend(rss_items)
            if provider_counts["press"] > 0:
                statuses["press"] = "ok"
            else:
                statuses["press"] = "missing_feeds"
            if provider_counts["tr"] > 0:
                statuses["tr"] = "ok"
            else:
                statuses["tr"] = "missing_feeds"
    # Local/BIST-focused RSS sweep using watchlist symbols
    if watchlist:
        local_max_feeds = int(os.getenv("LOCAL_RSS_MAX_FEEDS", "4") or 4)
        local_max_terms = int(os.getenv("LOCAL_RSS_MAX_TERMS", "4") or 4)
        local_extra_feeds = TR_NEWS_FEEDS[:local_max_feeds]
        local_terms = []
        for sym in watchlist[: min(len(watchlist), 4)]:
            local_terms.append(sym)
        local_terms.extend(_load_bist_alias_terms()[:6])
        for term in local_terms[:local_max_terms]:
            q = f"{term} BIST"
            rss_raw = search_rss_local(
                q,
                max_items=min(8, maxrecords),
                extra_feeds=local_extra_feeds,
                strict=True,
                timespan=timespan,
                timeout=min(6.0, timeout),
            )
            rss_items = normalize_rss(rss_raw)
            if rss_items:
                for it in rss_items:
                    domain = extract_domain(it.url or "")
                    if domain in TR_DOMAINS:
                        provider_counts["tr"] += 1
                    else:
                        provider_counts["rss_google"] += 1
                items.extend(rss_items)
                statuses["tr"] = "ok"
    return items, notes, provider_counts, rss_blocklist_hits, statuses


def collect_local_news(
    watchlist: List[str],
    timespan: str,
    maxrecords: int,
    timeout: float,
) -> Tuple[List[NewsItem], List[str], dict]:
    notes: List[str] = []
    items: List[NewsItem] = []
    provider_counts = {"tr": 0}
    if not watchlist and not TR_NEWS_FEEDS:
        return items, ["local_news_empty"], provider_counts

    local_max_feeds = int(os.getenv("LOCAL_RSS_MAX_FEEDS", "4") or 4)
    local_max_terms = int(os.getenv("LOCAL_RSS_MAX_TERMS", "4") or 4)
    local_extra_feeds = TR_NEWS_FEEDS[:local_max_feeds]

    # Base local sweep without query filtering (headline-only)
    base_raw = search_rss_local(
        "",
        max_items=maxrecords,
        extra_feeds=local_extra_feeds,
        strict=False,
        timespan=timespan,
        timeout=min(6.0, timeout),
    )
    base_items = normalize_rss(base_raw)
    if base_items:
        items.extend(base_items)
        notes.append("rss_local_base")

    if len(items) >= maxrecords:
        items = items[:maxrecords]
        if items:
            items = dedup_global(items)
        for it in items:
            domain = extract_domain(it.url or "")
            if domain in TR_DOMAINS:
                provider_counts["tr"] += 1
        return items, notes, provider_counts

    local_terms: list[str] = []
    for sym in watchlist[: min(len(watchlist), 4)]:
        local_terms.append(sym)
    local_terms.extend(_load_bist_alias_terms()[:6])
    local_terms.extend(["BIST", "Borsa Istanbul"])

    max_per_term = min(8, maxrecords)
    for term in local_terms[:local_max_terms]:
        q = f"{term} BIST" if term not in ("BIST", "Borsa Istanbul") else term
        strict = term not in ("BIST", "Borsa Istanbul")
        rss_raw = search_rss_local(
            q,
            max_items=max_per_term,
            extra_feeds=local_extra_feeds,
            strict=strict,
            timespan=timespan,
            timeout=min(6.0, timeout),
        )
        rss_items = normalize_rss(rss_raw)
        if not rss_items:
            continue
        items.extend(rss_items)
        notes.append("rss_local_term")

    if items:
        items = dedup_global(items)
        for it in items:
            domain = extract_domain(it.url or "")
            if domain in TR_DOMAINS:
                provider_counts["tr"] += 1

    return items, notes, provider_counts


def collect_personal_news(
    queries: List[str],
    timespan: str,
    maxrecords: int,
    timeout: float,
    allow_t3: bool,
) -> Tuple[List[NewsItem], List[str]]:
    notes: List[str] = []
    items: List[NewsItem] = []
    selected = select_queries([safe_query(q) for q in queries if q], MAX_QUERIES_PER_SPAN)
    if not selected:
        return [], ["personal_query_empty"]

    def fetch_one(q: str):
        return search_gdelt_context(q, timespan, maxrecords, timeout)

    gdelt_errors: List[str] = []
    with ThreadPoolExecutor(max_workers=len(selected)) as ex:
        futures = {ex.submit(fetch_one, q): q for q in selected}
        for fut in as_completed(futures):
            raw, err = fut.result()
            if err:
                notes.append(err)
                gdelt_errors.append(err)
                if err == "gdelt_rate_limited":
                    break
            items.extend(normalize_gdelt(raw))

    if not allow_t3:
        items = [it for it in items if extract_domain(it.url) in TIER_A or extract_domain(it.url) in TIER_B]

    provider_used = "gdelt" if items else "none"
    if (not items) and PERSONAL_RSS_FALLBACK:
        rss_items: List[NewsItem] = []
        sweep = max(1, min(len(selected), RSS_FALLBACK_QUERY_SWEEP))
        for q in selected[:sweep]:
            rss_raw = search_rss(q, maxrecords, timespan=timespan)
            if rss_raw:
                rss_items.extend(normalize_rss(rss_raw))
        if rss_items:
            items.extend(rss_items)
            provider_used = "rss"
            notes.append("personal_rss_fallback")
            if not allow_t3:
                items = [it for it in items if extract_domain(it.url) in TIER_A or extract_domain(it.url) in TIER_B]

    # Extra TR/BIST topical sweep to pull more local coverage
    extra_rss_items: List[NewsItem] = []
    for q in BIST_BOOST_TERMS[:6]:
        raw = search_rss(q, max_items=min(25, maxrecords), timespan=timespan)
        if raw:
            extra_rss_items.extend(normalize_rss(raw))
    if extra_rss_items:
        items.extend(extra_rss_items)
        provider_used = provider_used if provider_used != "none" else "rss"
        notes.append("personal_rss_local_boost")

    for it in items:
        tags = ["PERSONAL", "PERSONAL_FEED"]
        if provider_used == "rss":
            tags.append("PERSONAL_FALLBACK")
        it.tags = tags

    notes.append(f"personal_source={provider_used}")
    notes.append(f"personal_items_fetched={len(items)}")

    return items, notes


def fetch_personal_events(timespan: str, timeout: float) -> Tuple[List[NewsItem], List[str]]:
    notes: List[str] = []
    queries = build_personal_queries()
    all_items: List[NewsItem] = []

    for span in ("1h", "6h", "24h"):
        items, span_notes = collect_personal_news(queries, span, 30, timeout, allow_t3=False)
        notes.extend(span_notes)
        all_items.extend(items)
        if len(all_items) >= 4:
            break

    if len(all_items) < 4:
        items, span_notes = collect_personal_news(queries, "24h", 30, timeout, allow_t3=True)
        notes.extend(span_notes)
        all_items.extend(items)

    if not all_items:
        return [], notes

    annotated, meta = annotate_items(all_items, 0.0)
    deduped = dedup_clusters(annotated, meta)
    deduped = dedup_global(deduped)
    deduped.sort(key=lambda x: (final_rank_score(x), x.quality_score), reverse=True)
    return deduped[:PERSONAL_TOP_K], notes


def unique_by_url(items: List[NewsItem]) -> List[NewsItem]:
    seen = {}
    for item in items:
        key = item.canonical_url or item.url
        if not key:
            continue
        seen[key] = item
    return list(seen.values())


def annotate_items(items: List[NewsItem], health_penalty: float) -> Tuple[List[NewsItem], Dict[int, dict]]:
    meta: Dict[int, dict] = {}
    for item in items:
        domain = item.source or extract_domain(item.url)
        dt = parse_any_date(item.publishedAtISO)
        if not dt:
            dt = datetime.now(timezone.utc)
        existing_tags = list(item.tags) if item.tags else []
        tags, _ = tag_title(item.title)

        entities, entity_scores, category_scores = match_entities(item.title)
        category = detect_category(category_scores)
        event_type = detect_event_type(item.title)
        impact_channel, asset_class_bias, expected = impact_map(event_type, item.title, category)

        personal_boost = 0
        persons: List[str] = []
        groups: List[str] = []
        person_meta: List[dict] = []
        if PERSON_MATCH_ALL_NEWS or "PERSONAL" in existing_tags:
            person_meta, boost = match_persons_detailed(item.title)
            persons = [p["name"] for p in person_meta]
            groups = list({p["group"] for p in person_meta})
            if persons:
                personal_boost = boost
                if "PERSONAL" not in existing_tags and "PERSONAL_MATCH" not in tags:
                    tags.append("PERSONAL_MATCH")
                for person in persons:
                    if person not in entities:
                        entities.append(person)
                        entity_scores[person] = 10
                for grp in groups:
                    if grp not in tags:
                        tags.append(grp)
        if "PERSONAL" in existing_tags:
            tags.append("PERSONAL")
            if not category_scores or max(category_scores.values()) == 0:
                category = "personal"

        relevance = compute_relevance_score(item.title, entities, entity_scores, dt, event_type)
        relevance = min(100, relevance + personal_boost)
        quality = compute_quality_score(domain, dt, health_penalty)

        item.source = domain
        item.canonical_url = canonicalize_url(item.url)
        merged_tags = []
        for t in tags + existing_tags:
            if t and t not in merged_tags:
                merged_tags.append(t)
        item.tags = merged_tags
        item.tier_score = tier_score(item.url)
        item.score = relevance
        item.ts = format_ts_tsi(dt)
        item.category = category
        item.entities = entities
        item.event_type = event_type
        item.impact_channel = impact_channel
        item.asset_class_bias = asset_class_bias
        item.expected_direction_short_term = expected
        item.relevance_score = relevance
        item.quality_score = quality
        item.short_summary = build_short_summary(item.title, event_type, entities)
        item.publishedAtISO = to_iso(dt)

        person_event = extract_person_event(item)
        match_confidence = None
        match_basis = None
        if persons:
            title_lower = item.title.lower()
            match_confidence = 55
            if any(k in title_lower for k in PERSONAL_KEYWORDS):
                match_confidence += 15
                match_basis = "title+keywords"
            else:
                match_basis = "title"
            if len(persons) > 1:
                match_confidence -= 10
            match_confidence = max(30, min(90, match_confidence))
        if person_event:
            impact_potential, confidence = score_person_impact(person_event, item)
            person_event["impact_potential"] = impact_potential
            person_event["confidence"] = confidence
            if match_confidence is not None:
                person_event["match_confidence"] = match_confidence
                person_event["match_basis"] = match_basis
            if persons:
                person_event["actor_id"] = person_id(persons[0])
                person_event["actor_group_id"] = (groups[0].lower() if groups else None)
            item.person_event = PersonEvent(**person_event)
            item.impact_potential = impact_potential
        elif persons:
            minimal = {
                "actor_name": persons[0],
                "actor_id": person_id(persons[0]),
                "actor_group": groups[0] if groups else None,
                "actor_group_id": groups[0].lower() if groups else None,
                "statement_type": "OTHER",
                "stance": "UNKNOWN",
                "impact_channel": [],
                "asset_class_bias": [],
                "expected_direction_short_term": None,
                "impact_potential": 0,
                "confidence": match_confidence or 40,
                "match_confidence": match_confidence,
                "match_basis": match_basis,
            }
            item.person_event = PersonEvent(**minimal)

        try:
            attach_scope_and_sectors(item)
        except Exception:
            pass

        top_entities = sorted(entities, key=lambda e: entity_scores.get(e, 0), reverse=True)[:2]
        if not top_entities and entities:
            top_entities = entities[:2]
        meta[id(item)] = {
            "canonical": canonical_title(item.title),
            "bucket": time_bucket(dt),
            "top_entities": top_entities,
            "published": dt,
            "canonical_url": item.canonical_url or "",
            "person_match_meta": [
                {"person_id": p["person_id"], "group_id": p["group_id"]} for p in (person_meta or [])
            ],
            "category_scores": category_scores,
            "event_type": event_type,
            "impact_channel": impact_channel,
        }
    return items, meta


def select_representative(items: List[NewsItem], meta: Dict[int, dict]) -> NewsItem:
    def sort_key(item: NewsItem):
        published = meta.get(id(item), {}).get("published")
        published_ts = published.timestamp() if published else 0
        return (-item.quality_score, -item.relevance_score, -published_ts, item.source or "")

    return sorted(items, key=sort_key)[0]


def dedup_clusters(items: List[NewsItem], meta: Dict[int, dict]) -> List[NewsItem]:
    items = unique_by_url(items)
    local_domains = _local_news_domains()
    grouped: Dict[Tuple[str, str | Tuple[str, ...]], List[NewsItem]] = {}
    for item in items:
        info = meta.get(id(item), {})
        canonical_url = info.get("canonical_url") or ""
        top_entities = tuple(info.get("top_entities", []))
        if canonical_url:
            grouped.setdefault(("url", canonical_url), []).append(item)
        else:
            grouped.setdefault(("entities", top_entities), []).append(item)

    output: List[NewsItem] = []
    for (group_type, group_key), group_items in grouped.items():
        clusters: List[List[NewsItem]] = []
        if group_type == "url":
            clusters = [group_items]
        else:
            for item in group_items:
                assigned = False
                for cluster in clusters:
                    rep = cluster[0]
                    if token_set_ratio(meta[id(item)]["canonical"], meta[id(rep)]["canonical"]) >= 0.85:
                        cluster.append(item)
                        assigned = True
                        break
                if not assigned:
                    clusters.append([item])

        for cluster in clusters:
            latest_by_domain: Dict[str, NewsItem] = {}
            for it in cluster:
                domain = it.source or ""
                current = latest_by_domain.get(domain)
                if current is None:
                    latest_by_domain[domain] = it
                    continue
                dt_new = meta.get(id(it), {}).get("published")
                dt_old = meta.get(id(current), {}).get("published")
                if dt_new and (not dt_old or dt_new > dt_old):
                    latest_by_domain[domain] = it
            unique_items = list(latest_by_domain.values())
            rep = select_representative(unique_items, meta)
            other_sources = sorted({it.source for it in unique_items if it is not rep and it.source})[:3]
            canonical_url = meta.get(id(rep), {}).get("canonical_url") or ""
            if canonical_url:
                dedup_key = f"url::{canonical_url}"
            else:
                entity_key = group_key if isinstance(group_key, tuple) else tuple(meta.get(id(rep), {}).get("top_entities", []))
                dedup_key = f"{meta[id(rep)]['canonical']}::{','.join(entity_key)}"
            rep.dedup_cluster_id = build_cluster_id(dedup_key)
            rep.other_sources = other_sources
            if local_domains:
                domains = {rep.source or ""}
                domains.update(other_sources or [])
                domains = {d for d in domains if d}
                has_local = any(_is_local_domain(d, local_domains) for d in domains)
                has_global = any(not _is_local_domain(d, local_domains) for d in domains)
                if has_local and has_global:
                    rep.tags = list(rep.tags or [])
                    if "LOCAL_GLOBAL_MATCH" not in rep.tags:
                        rep.tags.append("LOCAL_GLOBAL_MATCH")
            output.append(rep)

    return output


def apply_domain_soft_cap(items: List[NewsItem], cap: int, rank_weights: dict | None = None) -> List[NewsItem]:
    counts: Dict[str, int] = {}
    output: List[NewsItem] = []
    for item in sorted(items, key=lambda x: (final_rank_score(x, rank_weights), x.quality_score), reverse=True):
        domain = item.source or ""
        counts[domain] = counts.get(domain, 0) + 1
        if counts[domain] <= cap:
            output.append(item)
    return output


def _published_ts(item: NewsItem) -> float:
    dt = parse_any_date(item.publishedAtISO)
    return dt.timestamp() if dt else 0.0


def dedup_global(items: List[NewsItem]) -> List[NewsItem]:
    items = sorted(items, key=lambda x: (-x.quality_score, -_published_ts(x), x.source or ""))
    kept: List[NewsItem] = []
    kept_titles: List[str] = []
    kept_urls: List[str] = []

    for item in items:
        canon_url = item.canonical_url or canonicalize_url(item.url)
        if canon_url and canon_url in kept_urls:
            match = None
            for kept_item in kept:
                if kept_item.canonical_url == canon_url or canonicalize_url(kept_item.url) == canon_url:
                    match = kept_item
                    break
            if match and item.source and item.source != match.source and item.source not in match.other_sources:
                if len(match.other_sources) < 3:
                    match.other_sources.append(item.source)
            continue
        title_key = canonical_title(item.title)
        match = None
        for idx, kept_item in enumerate(kept):
            if token_set_ratio(title_key, kept_titles[idx]) >= 0.85:
                match = kept_item
                break
        if match:
            if item.source and item.source != match.source and item.source not in match.other_sources:
                if len(match.other_sources) < 3:
                    match.other_sources.append(item.source)
            continue
        kept.append(item)
        kept_titles.append(title_key)
        if canon_url:
            kept_urls.append(canon_url)

    return kept


def event_time_bucket(dt: datetime | None) -> str:
    if not dt:
        return "unknown"
    dt = dt.astimezone(timezone.utc)
    minute = (dt.minute // EVENT_TIME_BUCKET_MINUTES) * EVENT_TIME_BUCKET_MINUTES
    bucket_dt = dt.replace(minute=minute, second=0, microsecond=0)
    return bucket_dt.strftime("%Y%m%d%H%M")


def _event_ts(item) -> float:
    dt = parse_any_date(getattr(item, "ts", None))
    if not dt:
        dt = parse_any_date(getattr(item, "publishedAtISO", None))
    return dt.timestamp() if dt else 0.0


def apply_domain_soft_cap_events(items: List["EventItem"], cap: int) -> List["EventItem"]:
    counts: Dict[str, int] = {}
    output: List["EventItem"] = []
    for item in sorted(items, key=lambda x: (x.relevance_score, x.quality_score), reverse=True):
        domain = item.source_domain or ""
        counts[domain] = counts.get(domain, 0) + 1
        if counts[domain] <= cap:
            output.append(item)
    return output


def build_news_summary(items: List[NewsItem], low_news: bool = False) -> List[str]:
    if not items:
        return ["akışta haber verisi zayıf"]
    categories = {}
    event_types = {}
    channels = {}
    for item in items:
        if item.category:
            categories[item.category] = categories.get(item.category, 0) + 1
        if item.event_type:
            event_types[item.event_type] = event_types.get(item.event_type, 0) + 1
        for ch in item.impact_channel:
            channels[ch] = channels.get(ch, 0) + 1
    top_category = max(categories.items(), key=lambda x: x[1])[0] if categories else "belirsiz"
    top_event = max(event_types.items(), key=lambda x: x[1])[0] if event_types else "belirsiz"
    top_channels = sorted(channels.items(), key=lambda x: x[1], reverse=True)[:2]
    channel_list = ", ".join([c for c, _ in top_channels]) if top_channels else "belirsiz"
    summary = [
        f"baskın kategori: {top_category}",
        f"baskın event_type: {top_event}",
        f"etki kanalları: {channel_list}",
    ]
    if low_news:
        summary.append("macro kanal ağırlığı artırıldı (faiz/dolar/VIX/petrol)")
    return summary


def _news_key(item: NewsItem) -> str:
    return (getattr(item, "canonical_url", None) or item.url or f"{item.title}|{item.publishedAtISO}").strip()


def _age_hours(item: NewsItem) -> float:
    dt = parse_any_date(item.publishedAtISO)
    if not dt:
        return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)


def compute_news_quality_stats(items: List[NewsItem]) -> dict:
    clusters = {getattr(i, "dedup_cluster_id", None) for i in items if getattr(i, "dedup_cluster_id", None)}
    domains = {extract_domain(i.url) for i in items if i.url}
    return {"unique_clusters": len(clusters), "unique_domains": len(domains)}


def rank_key(item: NewsItem) -> Tuple[float, float, str]:
    impact = float(getattr(item, "impact_potential", 0) or 0)
    score = abs(impact) if impact > 0 else (float(item.relevance_score) / 100.0) * (float(item.quality_score) / 100.0)
    published = parse_any_date(getattr(item, "publishedAtISO", None))
    ts = published.timestamp() if published else 0.0
    return (-score, -ts, getattr(item, "evidence_id", "") or "")


def _apply_age_mix(items: List[NewsItem], timespan: str, notes: List[str]) -> List[NewsItem]:
    if not NEWS_LONG_AGE_MIX_ENABLED:
        return items
    if timespan not in ("7d", "30d"):
        return items
    if timespan == "7d":
        min_age_days = max(1, NEWS_7D_MIN_AGE_DAYS)
        min_count = max(0, NEWS_7D_MIN_AGE_COUNT)
    else:
        min_age_days = max(1, NEWS_30D_MIN_AGE_DAYS)
        min_count = max(0, NEWS_30D_MIN_AGE_COUNT)
    notes.append(f"age_mix_rule={timespan}:min_age_days:{min_age_days},min_count:{min_count}")
    if not items or min_count <= 0:
        return items
    threshold = datetime.now(timezone.utc) - timedelta(days=min_age_days)
    candidates = [it for it in items if parse_any_date(it.publishedAtISO) and parse_any_date(it.publishedAtISO) < threshold]
    notes.append(f"age_mix_candidates={len(candidates)}")
    if not candidates:
        notes.append("age_mix_injected=0_reason=insufficient_old_items")
        return items
    candidates_sorted = sorted(candidates, key=lambda i: (rank_key(i), i.publishedAtISO or ""))
    top_k = min(TOP_K_NEWS, len(items))
    out = list(items[:top_k])
    out_keys = {_news_key(it) for it in out}
    injected_ids: list[str] = []
    for it in candidates_sorted:
        if len(injected_ids) >= min_count:
            break
        key = _news_key(it)
        if key in out_keys:
            continue
        if len(out) < top_k:
            out.append(it)
        else:
            replace_idx = max(0, top_k - 1 - len(injected_ids))
            out[replace_idx] = it
        out_keys.add(key)
        injected_ids.append(getattr(it, "evidence_id", "") or key)
    if injected_ids:
        notes.append(f"age_mix_injected={len(injected_ids)}")
        notes.append(f"age_mix_ids={','.join(injected_ids[:5])}")
    else:
        notes.append("age_mix_injected=0_reason=insufficient_old_items")
    filtered = [it for it in items if _news_key(it) in out_keys]
    return filtered[:top_k]


def fetch_news(
    query: str,
    timespan: str,
    maxrecords: int,
    timeout: float,
    watchlist: List[str] | None = None,
    rank_weights: dict | None = None,
    skip_gdelt: bool = False,
):
    notes: List[str] = []
    watchlist = watchlist or []
    provider_counts = {"gdelt": 0, "rss_google": 0, "press": 0, "tr": 0, "finnhub": 0}
    rss_blocklist_hits = 0
    queries = []
    if query:
        queries.append(query)
    queries.extend(build_watch_queries(watchlist))

    is_long_timespan = timespan.endswith("d")
    if timespan in TIMESPAN_FALLBACK:
        start = TIMESPAN_FALLBACK.index(timespan)
        span_order = TIMESPAN_FALLBACK[start:]
    else:
        span_order = [timespan] if is_long_timespan else [timespan] + TIMESPAN_FALLBACK
    all_items: List[NewsItem] = []
    used_timespan = timespan
    health_penalty = 0.0
    start_ts = time.monotonic()
    min_required = MIN_NEWS_LONG if is_long_timespan else MIN_NEWS

    for span in span_order:
        if time.monotonic() - start_ts > NEWS_TIME_BUDGET:
            notes.append("news_time_budget_exceeded")
            break
        items, span_notes, penalty, counts, block_hits = collect_news(
            queries,
            span,
            maxrecords,
            timeout,
            watchlist=watchlist,
            min_required=min_required,
            allow_extras=not is_long_timespan,
            skip_gdelt=skip_gdelt,
        )
        notes.extend(span_notes)
        health_penalty = max(health_penalty, penalty)
        all_items.extend(items)
        for key, val in counts.items():
            provider_counts[key] = provider_counts.get(key, 0) + val
        rss_blocklist_hits += block_hits

        annotated, meta = annotate_items(all_items, health_penalty)
        deduped = dedup_clusters(annotated, meta)
        deduped = dedup_global(deduped)
        deduped = apply_domain_soft_cap(deduped, DOMAIN_SOFT_CAP, rank_weights=rank_weights)
        if is_long_timespan:
            stats = compute_news_quality_stats(deduped)
            notes.append(f"news_quality_stats=clusters:{stats['unique_clusters']},domains:{stats['unique_domains']}")
            thin_count = len(deduped) < min_required
            thin_quality = (stats["unique_clusters"] < NEWS_LONG_GATING_UNIQUE_CLUSTER_MIN) or (
                stats["unique_domains"] < NEWS_LONG_GATING_UNIQUE_DOMAIN_MIN
            )
            gating_reason = "none"
            if thin_count:
                gating_reason = "thin_count"
            elif thin_quality:
                gating_reason = "thin_quality"
            notes.append(f"news_gating_reason={gating_reason}")
            if gating_reason != "none":
                extra_items, extra_notes, extra_counts, extra_block_hits, statuses = collect_news_extras(
                    queries,
                    span,
                    maxrecords,
                    timeout,
                    watchlist,
                )
                notes.extend(extra_notes)
                all_items.extend(extra_items)
                for key, val in extra_counts.items():
                    provider_counts[key] = provider_counts.get(key, 0) + val
                rss_blocklist_hits += extra_block_hits
                notes.append(f"finnhub_status={statuses.get('finnhub')}")
                notes.append(f"press_status={statuses.get('press')}")
                notes.append(f"tr_status={statuses.get('tr')}")
                annotated, meta = annotate_items(all_items, health_penalty)
                deduped = dedup_clusters(annotated, meta)
                deduped = dedup_global(deduped)
                deduped = apply_domain_soft_cap(deduped, DOMAIN_SOFT_CAP, rank_weights=rank_weights)
        if len(deduped) >= min_required:
            used_timespan = span
            deduped.sort(key=lambda x: (final_rank_score(x, rank_weights), x.quality_score), reverse=True)
            if is_long_timespan:
                deduped = _apply_age_mix(deduped, timespan, notes)
            raw_count = len(all_items)
            dedup_count = max(0, raw_count - len(deduped))
            notes.append(f"dedup_count:{dedup_count}")
            notes.append(
                f"provider_counts=gdelt:{provider_counts['gdelt']},rss_google:{provider_counts['rss_google']},press:{provider_counts['press']},tr:{provider_counts['tr']},finnhub:{provider_counts['finnhub']}"
            )
            notes.append(
                f"caps_applied=tickers:{min(len(watchlist), NEWS_EXTRA_MAX_TICKERS)},feeds:{min(len(PRESS_RELEASE_FEEDS) + len(TR_NEWS_FEEDS), NEWS_EXTRA_MAX_FEEDS)}"
            )
            notes.append(f"rss_blocklist_hits={rss_blocklist_hits}")
            return deduped[:TOP_K_NEWS], notes, used_timespan
        used_timespan = span

    notes.append("haber_verisi_zayıf")
    if "news_time_budget_exceeded" not in notes and not is_long_timespan:
        fallback_queries = build_fallback_queries()
        items, span_notes, penalty, counts, block_hits = collect_news(
            fallback_queries,
            "24h",
            maxrecords,
            timeout,
            watchlist=watchlist,
            min_required=min_required,
            allow_extras=True,
        )
        notes.extend(span_notes)
        health_penalty = max(health_penalty, penalty)
        all_items.extend(items)
        for key, val in counts.items():
            provider_counts[key] = provider_counts.get(key, 0) + val
        rss_blocklist_hits += block_hits
    if is_long_timespan:
        notes.append("news_long_timespan_no_fallback")

    annotated, meta = annotate_items(all_items, health_penalty)
    deduped = dedup_clusters(annotated, meta)
    deduped = dedup_global(deduped)
    deduped = apply_domain_soft_cap(deduped, DOMAIN_SOFT_CAP, rank_weights=rank_weights)
    deduped.sort(key=lambda x: (final_rank_score(x, rank_weights), x.quality_score), reverse=True)
    if is_long_timespan:
        deduped = _apply_age_mix(deduped, timespan, notes)
    raw_count = len(all_items)
    dedup_count = max(0, raw_count - len(deduped))
    notes.append(f"dedup_count:{dedup_count}")
    notes.append(
        f"provider_counts=gdelt:{provider_counts['gdelt']},rss_google:{provider_counts['rss_google']},press:{provider_counts['press']},tr:{provider_counts['tr']},finnhub:{provider_counts['finnhub']}"
    )
    notes.append(
        f"caps_applied=tickers:{min(len(watchlist), NEWS_EXTRA_MAX_TICKERS)},feeds:{min(len(PRESS_RELEASE_FEEDS) + len(TR_NEWS_FEEDS), NEWS_EXTRA_MAX_FEEDS)}"
    )
    notes.append(f"rss_blocklist_hits={rss_blocklist_hits}")
    return deduped[:TOP_K_NEWS], notes, used_timespan


def classify_region(title: str) -> str | None:
    t = title.lower()
    for region, keys in STATE_REGIONS.items():
        if any(k in t for k in keys):
            return region
    return None


def build_leaders_groups(
    timespan: str,
    timeout: float,
    watchlist: List[str],
    max_per_group: int = 25,
    rank_weights: dict | None = None,
):
    intent = BASE_INTENT

    states_query = f"{intent} president prime minister chancellor central bank fed ecb boj pboc sanctions ceasefire"
    energy_query = f"{intent} Exxon Chevron Shell BP TotalEnergies Aramco OPEC IEA oil gas LNG"
    sp500_query = (
        f"{intent} Apple Microsoft Amazon Nvidia Meta Alphabet Tesla JPMorgan Visa Mastercard UnitedHealth Pfizer"
        " Berkshire CocaCola Exxon AMD TSMC Intel Broadcom Qualcomm ASML"
    )
    crypto_query = f"{intent} Coinbase Binance Kraken MicroStrategy Marathon Riot Tether Circle BlackRock crypto exchange"

    if watchlist:
        wl = " ".join(watchlist)
        energy_query = f"{energy_query} {wl}"
        sp500_query = f"{sp500_query} {wl}"
        crypto_query = f"{crypto_query} {wl}"

    groups = []
    for key, title, q in (
        ("states", "Devlet Baskanlari", states_query),
        ("energy", "Enerji Sirket CEO", energy_query),
        ("sp500", "S&P 500 CEO", sp500_query),
        ("crypto_ceos", "Kripto Sirket CEO", crypto_query),
    ):
        items, _, _ = fetch_news(q, timespan, max_per_group, timeout, watchlist=watchlist, rank_weights=rank_weights)
        if key == "states":
            for item in items:
                region = classify_region(item.title)
                if region and region not in item.tags:
                    item.tags.append(region)
        groups.append({"key": key, "title": title, "items": items})

    return groups


def build_leaders_from_news(news: List[NewsItem]):
    groups = {
        "states": [],
        "energy": [],
        "sp500": [],
        "crypto_ceos": [],
    }

    for item in news:
        region = classify_region(item.title)
        if region:
            if region not in item.tags:
                item.tags.append(region)
            groups["states"].append(item)
        if "PERSONAL" in (item.tags or []):
            groups["states"].append(item)
        if item.category == "enerji":
            groups["energy"].append(item)
        if item.category == "teknoloji":
            groups["sp500"].append(item)
        if item.category == "kripto":
            groups["crypto_ceos"].append(item)

    return [
        {"key": "states", "title": "Devlet Baskanlari", "items": groups["states"][:10]},
        {"key": "energy", "title": "Enerji Sirket CEO", "items": groups["energy"][:10]},
        {"key": "sp500", "title": "S&P 500 CEO", "items": groups["sp500"][:10]},
        {"key": "crypto_ceos", "title": "Kripto Sirket CEO", "items": groups["crypto_ceos"][:10]},
    ]


REGIONAL_POWER_COUNTRIES = {
    "United States",
    "Brazil",
    "Canada",
    "Mexico",
    "Argentina",
    "Germany",
    "United Kingdom",
    "France",
    "Italy",
    "Russia",
    "China",
    "India",
    "Japan",
    "South Korea",
    "Indonesia",
    "Saudi Arabia",
    "Iran",
    "United Arab Emirates",
    "Türkiye",
}


def build_event_summary(
    title: str,
    companies: List[CompanyEntity],
    countries: List[CountryEntity],
    persons: List[PersonEntity],
    sector_name: str | None,
) -> str:
    trimmed = title.strip()
    if len(trimmed) > 160:
        trimmed = trimmed[:157] + "..."
    parts = []
    if companies:
        parts.append("Şirket: " + ", ".join([c.name for c in companies[:2]]))
    if countries:
        parts.append("Ülke: " + ", ".join([c.country for c in countries[:2]]))
    if persons:
        parts.append("Kişi: " + ", ".join([p.name for p in persons[:2]]))
    if sector_name and sector_name != "UNKNOWN":
        parts.append("Sektör: " + sector_name)
    if parts:
        return f"{trimmed}. " + " ".join(parts)
    return trimmed


def should_enrich_summary(title: str, summary: str | None) -> bool:
    if not title or not summary:
        return False
    t = title.strip().lower()
    s = summary.strip().lower()
    if not t or not s:
        return False
    if s == t or s.startswith(t):
        return True
    return token_set_ratio(t, s) >= 0.9


def build_impacted_assets(
    companies: List[CompanyEntity],
    crypto: List[CryptoEntity],
    sector_name: str | None,
) -> List[ImpactedAsset]:
    assets: List[ImpactedAsset] = []
    for comp in companies:
        if comp.match_type != "strong":
            continue
        symbol = comp.ticker or comp.name
        assets.append(
            ImpactedAsset(
                asset_type="equity",
                symbol_or_id=symbol,
                reason="direct",
                confidence=comp.confidence,
            )
        )
        if len(assets) >= 5:
            return assets
    for coin in crypto:
        if coin.match_type != "strong":
            continue
        assets.append(
            ImpactedAsset(
                asset_type="crypto",
                symbol_or_id=coin.symbol,
                reason="direct",
                confidence=coin.confidence,
            )
        )
        if len(assets) >= 5:
            return assets

    if assets or not sector_name or sector_name == "UNKNOWN":
        return assets

    giants = SECTOR_GIANTS_REGISTRY.get(sector_name, [])[:3]
    for name in giants:
        assets.append(
            ImpactedAsset(
                asset_type="equity",
                symbol_or_id=name,
                reason="sector",
                confidence=55,
            )
        )
    return assets


def compute_impact_label(reactions: List[MarketReaction]) -> str:
    if not reactions:
        return "UNKNOWN"
    avg_move = sum(r.price_change_pct for r in reactions) / max(1, len(reactions))
    if avg_move > 0.2:
        return "POS"
    if avg_move < -0.2:
        return "NEG"
    return "NEUTRAL"


def compute_overall_confidence(
    quality_score: int,
    relevance_score: int,
    sector_confidence: int,
    country_confidence: int,
    reactions: List[MarketReaction],
    data_missing_penalty: float,
) -> int:
    eventstudy_conf = 0.0
    if reactions:
        eventstudy_conf = sum(r.confidence for r in reactions) / len(reactions)
    base = (
        (0.25 * quality_score)
        + (0.25 * relevance_score)
        + (0.20 * max(sector_confidence, country_confidence))
        + (0.15 * eventstudy_conf)
        + (0.15 * (1 - data_missing_penalty) * 100)
    )
    return int(max(0, min(100, round(base))))


def build_event_items(
    category: str,
    items: List[NewsItem],
    sources_used: List[str],
    health_penalty: float,
    sector_filter: str | None = None,
    summary_timeout: float | None = None,
) -> Tuple[List[EventItem], Dict[int, dict]]:
    events: List[EventItem] = []
    meta: Dict[int, dict] = {}

    for item in items:
        dt = parse_any_date(item.publishedAtISO)
        if not dt:
            dt = datetime.now(timezone.utc)
        domain = item.source or extract_domain(item.url)

        companies: List[CompanyEntity] = []
        countries: List[CountryEntity] = []
        persons: List[PersonEntity] = []
        crypto: List[CryptoEntity] = []
        sector_name: str | None = None
        country_tags: List[CountryTag] = []
        debug_flags: List[str] = []
        sector_score = 0
        entity_scores: Dict[str, int] = {}
        boost = 0

        if category == "COMPANY":
            companies, entity_scores, debug_flags = match_companies_detailed(item.title)
            if not companies:
                continue
        elif category == "REGIONAL":
            countries, country_tags, country_confidence, country_flags = match_countries_detailed(item.title, category)
            debug_flags.extend(country_flags)
            if not countries:
                continue
            for c in countries:
                entity_scores[c.country] = 25
                if c.country in REGIONAL_POWER_COUNTRIES:
                    boost += 10
        elif category == "SECTOR":
            sector_name, sector_score, _, _ = detect_sector(item.title)
            if not sector_name:
                continue
            if sector_filter and sector_name != sector_filter:
                continue
            entity_scores[sector_name] = 20
        elif category == "PERSONAL":
            persons = build_person_entities(item.title)
            if not persons:
                continue
            persons_raw, _, pboost = match_persons(item.title)
            boost += pboost
            for p in persons_raw:
                entity_scores[p] = 30
        else:
            continue

        crypto, crypto_flags = match_crypto_entities(item.title)
        debug_flags.extend(crypto_flags)

        if category in {"COMPANY", "SECTOR", "REGIONAL"}:
            extra_companies, extra_scores, extra_flags = match_companies_detailed(item.title)
            if extra_companies and not companies:
                companies = extra_companies
            for name, score in extra_scores.items():
                entity_scores[name] = max(entity_scores.get(name, 0), score)
            debug_flags.extend(extra_flags)

        if category != "REGIONAL":
            extra_countries, extra_tags, extra_conf, extra_flags = match_countries_detailed(item.title, category)
            if extra_countries and not countries:
                countries = extra_countries
                country_tags = extra_tags
            debug_flags.extend(extra_flags)

        entities_for_score = [c.name for c in companies] + [c.country for c in countries] + [p.name for p in persons]
        if not entities_for_score and sector_name:
            entities_for_score = [sector_name]

        relevance = compute_event_relevance_score(item.title, entities_for_score, entity_scores, dt)
        relevance = min(100, relevance + boost)
        quality = compute_quality_score(domain, dt, health_penalty)
        sector_confidence = min(100, max(0, sector_score * 4)) if sector_name else 0
        country_confidence = 0
        if countries:
            country_confidence = max(c.confidence for c in countries)

        ts = format_ts_tsi(dt)
        short_summary = build_event_summary(item.title, companies, countries, persons, sector_name)
        if should_enrich_summary(item.title, short_summary):
            summary = summarize_article_openai(
                title=item.title,
                description=item.description,
                content_text=item.content_text,
                source_domain=domain,
                published_ts=ts,
                url=item.url,
                timeout=summary_timeout or 12.0,
            )
            if summary and summary.confidence > 0 and summary.summary_tr:
                short_summary = summary.summary_tr
            elif summary and "openai_disabled" in summary.data_missing:
                debug_flags.append("OPENAI_DISABLED")
        sector_label = sector_name or "UNKNOWN"
        impacted_assets = build_impacted_assets(companies, crypto, sector_label)
        market_reaction: List[MarketReaction] = []
        data_missing_penalty = 0.0
        if impacted_assets and not market_reaction:
            data_missing_penalty += 0.25
            debug_flags.append("DATA_MISSING_PRICE")
        if category == "REGIONAL" and not countries:
            data_missing_penalty += 0.15
            debug_flags.append("AMBIGUOUS_COUNTRY")
        if sector_label == "UNKNOWN":
            data_missing_penalty += 0.10
            debug_flags.append("LOW_SIGNAL")

        impact_label = compute_impact_label(market_reaction)
        overall_confidence = compute_overall_confidence(
            quality,
            relevance,
            sector_confidence,
            country_confidence,
            market_reaction,
            data_missing_penalty,
        )

        event = EventItem(
            event_category=category,
            entities=EventEntities(
                companies=companies,
                countries=countries,
                persons=persons,
                crypto=crypto,
            ),
            title=item.title,
            ts=ts,
            source_domain=domain,
            source_url=item.url,
            short_summary=short_summary,
            sector_name=sector_label,
            country_tags=country_tags,
            sector_confidence=sector_confidence,
            country_confidence=country_confidence,
            impacted_assets=impacted_assets,
            market_reaction=market_reaction,
            relevance_score=relevance,
            quality_score=quality,
            prepricing_tag="UNKNOWN",
            prepricing_score=None,
            impact_label=impact_label,
            overall_confidence=overall_confidence,
            other_sources=[],
            sources_used=sources_used,
            debug_flags=list(dict.fromkeys(debug_flags)),
        )

        top_entities = (
            [c.name for c in companies]
            + [c.country for c in countries]
            + [p.name for p in persons]
            + [c.symbol for c in crypto]
        )[:2]
        if not top_entities and sector_name:
            top_entities = [sector_name]
        meta[id(event)] = {
            "canonical": canonical_title(item.title),
            "bucket": event_time_bucket(dt),
            "top_entities": top_entities,
            "published": dt,
            "sector_score": sector_score,
        }
        events.append(event)

    return events, meta


def dedup_event_items(items: List[EventItem], meta: Dict[int, dict]) -> List[EventItem]:
    grouped: Dict[Tuple[str, Tuple[str, ...]], List[EventItem]] = {}
    for item in items:
        info = meta.get(id(item), {})
        bucket = info.get("bucket", "unknown")
        top_entities = tuple(info.get("top_entities", []))
        grouped.setdefault((bucket, top_entities), []).append(item)

    output: List[EventItem] = []
    for (bucket, top_entities), group_items in grouped.items():
        clusters: List[List[EventItem]] = []
        for item in group_items:
            assigned = False
            for cluster in clusters:
                rep = cluster[0]
                if token_set_ratio(meta[id(item)]["canonical"], meta[id(rep)]["canonical"]) >= 0.85:
                    cluster.append(item)
                    assigned = True
                    break
            if not assigned:
                clusters.append([item])

        for cluster in clusters:
            latest_by_domain: Dict[str, EventItem] = {}
            for it in cluster:
                domain = it.source_domain or ""
                current = latest_by_domain.get(domain)
                if current is None:
                    latest_by_domain[domain] = it
                    continue
                dt_new = meta.get(id(it), {}).get("published")
                dt_old = meta.get(id(current), {}).get("published")
                if dt_new and (not dt_old or dt_new > dt_old):
                    latest_by_domain[domain] = it
            unique_items = list(latest_by_domain.values())
            def _rep_key(x: EventItem):
                dt = meta.get(id(x), {}).get("published")
                ts = dt.timestamp() if dt else 0.0
                return (-x.quality_score, -ts, x.source_domain or "")

            rep = sorted(unique_items, key=_rep_key)[0]
            other_sources = sorted({it.source_domain for it in unique_items if it is not rep and it.source_domain})[:3]
            dedup_key = f"{meta[id(rep)]['canonical']}::{','.join(top_entities)}::{bucket}"
            rep.dedup_cluster_id = build_cluster_id(dedup_key)
            rep.other_sources = other_sources
            output.append(rep)

    return output


def collect_events_doc(
    queries: List[str],
    timespan: str,
    maxrecords: int,
    timeout: float,
    allow_t3: bool,
    allow_filters: bool = False,
) -> Tuple[List[NewsItem], List[str], float]:
    notes: List[str] = []
    items: List[NewsItem] = []
    health_penalty = 0.0
    sanitizer = safe_query_with_filters if allow_filters else safe_query
    selected = select_queries([sanitizer(q) for q in queries if q], MAX_QUERIES_PER_SPAN)
    if not selected:
        return [], ["event_query_empty"], 0.2

    for q in selected:
        raw, err = search_gdelt(q, timespan, maxrecords, timeout)
        if err:
            notes.append(err)
            health_penalty = max(health_penalty, 0.2)
            if err == "gdelt_rate_limited":
                break
        items.extend(normalize_gdelt(raw))

    if not allow_t3:
        items = [it for it in items if extract_domain(it.url) in TIER_A or extract_domain(it.url) in TIER_B]

    return items, notes, health_penalty


def build_regional_queries(simplified: bool) -> List[str]:
    countries = list(COUNTRY_ALIASES.keys())
    themes = "sanctions conflict oil trade rates"
    limit = 5 if simplified else 10
    return [f"{' '.join(countries[:limit])} {themes}"]


def build_company_queries(watchlist: List[str], simplified: bool) -> List[str]:
    tokens = []
    for entries in WATCHLIST_BY_CATEGORY.values():
        for name, aliases in entries:
            tokens.append(name)
            if aliases:
                tokens.append(aliases[0])
    tokens.extend(watchlist)
    limit = 5 if simplified else 12
    return [" ".join(tokens[:limit])]


def or_group(items: List[str]) -> str:
    cleaned = []
    for item in items:
        token = item.strip()
        if not token:
            continue
        if " " in token:
            token = f"\"{token}\""
        cleaned.append(token)
    return " OR ".join(cleaned)


def build_sector_query_core(sector: str) -> str:
    rules = SECTOR_RULES.get(sector, {})
    sector_terms = rules.get("required", []) + rules.get("boost", [])
    sector_terms = sector_terms[:10]
    trigger_terms = SECTOR_TRIGGER_KEYWORDS[:10]
    sector_part = or_group(sector_terms)
    trigger_part = or_group(trigger_terms)
    if sector_part and trigger_part:
        return f"({sector_part}) ({trigger_part})"
    return f"{sector_part} {trigger_part}".strip()


def build_sector_query_primary(sector: str) -> str:
    core = build_sector_query_core(sector)
    lang_part = " OR ".join([f"sourcelang:{lang}" for lang in PREFERRED_SOURCELANG])
    if core:
        return f"{core} ({lang_part})"
    return f"({lang_part})"


def build_sector_query_backstop(sector: str) -> str:
    core = build_sector_query_core(sector)
    giants = SECTOR_GIANTS_REGISTRY.get(sector, [])[:BACKSTOP_GIANTS_PER_SECTOR]
    giants_part = f"({or_group(giants)})" if giants else ""
    if core and giants_part:
        return f"{core} {giants_part}"
    return f"{core} {giants_part}".strip()


def _prune_sector_rolling(now: datetime) -> None:
    cutoff = now - timedelta(hours=24)
    SECTOR_ROLLING_EVENTS[:] = [row for row in SECTOR_ROLLING_EVENTS if row[0] >= cutoff]
    for key in list(SECTOR_ROLLING_SEEN.keys()):
        if SECTOR_ROLLING_SEEN[key] < cutoff:
            SECTOR_ROLLING_SEEN.pop(key, None)


def _sector_default_order() -> List[str]:
    return [s for s in SECTOR_GIANTS_REGISTRY.keys() if s not in SECTOR_CORE_ALWAYS]


def select_sectors_for_rotation() -> List[str]:
    now = datetime.now(timezone.utc)
    _prune_sector_rolling(now)
    scores: Dict[str, int] = {}
    for ts, sector, score in SECTOR_ROLLING_EVENTS:
        scores[sector] = scores.get(sector, 0) + score

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    dynamic = [sector for sector, _ in ranked if sector not in SECTOR_CORE_ALWAYS][:SECTOR_DYNAMIC_COUNT]
    if not dynamic:
        dynamic = _sector_default_order()[:SECTOR_DYNAMIC_COUNT]

    max_dynamic = max(0, SECTOR_ROTATION_MAX - len(SECTOR_CORE_ALWAYS))
    if len(dynamic) > max_dynamic:
        overflow = dynamic[max_dynamic:]
        dynamic = dynamic[:max_dynamic]
        for sector in overflow:
            if sector not in SECTOR_ROTATION_QUEUE and sector not in SECTOR_CORE_ALWAYS:
                SECTOR_ROTATION_QUEUE.append(sector)

    while len(dynamic) < max_dynamic and SECTOR_ROTATION_QUEUE:
        candidate = SECTOR_ROTATION_QUEUE.pop(0)
        if candidate not in SECTOR_CORE_ALWAYS and candidate not in dynamic:
            dynamic.append(candidate)

    selected = []
    for sector in SECTOR_CORE_ALWAYS + dynamic:
        if sector not in selected:
            selected.append(sector)
    return selected


def count_giant_hits(items: List[NewsItem], giants: List[str]) -> int:
    if not items or not giants:
        return 0
    hits = set()
    for item in items:
        title = (item.title or "").lower()
        for name in giants:
            if name.lower() in title:
                hits.add(name.lower())
    return len(hits)


def update_sector_rolling(events: List[EventItem], meta: Dict[int, dict]) -> None:
    now = datetime.now(timezone.utc)
    _prune_sector_rolling(now)
    for evt in events:
        info = meta.get(id(evt), {})
        sector = evt.sector_name
        score = info.get("sector_score", 0)
        published = info.get("published") or now
        cluster_id = evt.dedup_cluster_id
        if not sector:
            continue
        if cluster_id:
            last_seen = SECTOR_ROLLING_SEEN.get(cluster_id)
            if last_seen and last_seen >= now - timedelta(hours=24):
                continue
            SECTOR_ROLLING_SEEN[cluster_id] = published
        SECTOR_ROLLING_EVENTS.append((published, sector, score))


def fetch_category_events(
    category: str,
    timespan: str,
    timeout: float,
    watchlist: List[str],
) -> Tuple[List[EventItem], List[str]]:
    notes: List[str] = []
    events: List[EventItem] = []

    if category == "REGIONAL":
        queries = build_regional_queries(simplified=False)
        simple_queries = build_regional_queries(simplified=True)
    elif category == "COMPANY":
        queries = build_company_queries(watchlist, simplified=False)
        simple_queries = build_company_queries(watchlist, simplified=True)
    elif category == "SECTOR":
        return [], ["event_category_use_fetch_sector"]
    else:
        return [], ["event_category_unknown"]

    if timespan in TIMESPAN_FALLBACK:
        start = TIMESPAN_FALLBACK.index(timespan)
        span_order = TIMESPAN_FALLBACK[start:]
    else:
        span_order = [timespan] + TIMESPAN_FALLBACK

    low_volume = True
    for span in span_order:
        qset = simple_queries if low_volume else queries
        raw_items, span_notes, penalty = collect_events_doc(qset, span, 60, timeout, allow_t3=False)
        notes.extend(span_notes)
        built, meta = build_event_items(category, raw_items, ["GDELT_DOC"], penalty)
        deduped = dedup_event_items(built, meta)
        deduped = apply_domain_soft_cap_events(deduped, SAME_DOMAIN_SOFT_CAP)
        deduped.sort(key=lambda x: (x.relevance_score, x.quality_score), reverse=True)
        events = deduped[:TOP_K_PER_CATEGORY]
        if len(events) >= 3:
            return events, notes
        low_volume = True

    raw_items, span_notes, penalty = collect_events_doc(simple_queries, "24h", 60, timeout, allow_t3=True)
    notes.extend(span_notes)
    built, meta = build_event_items(category, raw_items, ["GDELT_DOC"], penalty)
    deduped = dedup_event_items(built, meta)
    deduped = apply_domain_soft_cap_events(deduped, SAME_DOMAIN_SOFT_CAP)
    deduped.sort(key=lambda x: (x.relevance_score, x.quality_score), reverse=True)
    return deduped[:TOP_K_PER_CATEGORY], notes


def fetch_sector_events_for_sector(
    sector: str,
    timespan: str,
    timeout: float,
) -> Tuple[List[EventItem], Dict[int, dict], List[str]]:
    notes: List[str] = []
    combined: List[EventItem] = []
    meta_all: Dict[int, dict] = {}
    giants = SECTOR_GIANTS_REGISTRY.get(sector, [])[:BACKSTOP_GIANTS_PER_SECTOR]

    if timespan in TIMESPAN_FALLBACK:
        start = TIMESPAN_FALLBACK.index(timespan)
        span_order = TIMESPAN_FALLBACK[start:]
    else:
        span_order = [timespan] + TIMESPAN_FALLBACK

    pass1_items: List[NewsItem] = []
    pass1_events: List[EventItem] = []
    pass1_meta: Dict[int, dict] = {}

    for span in span_order:
        query = build_sector_query_primary(sector)
        raw_items, span_notes, penalty = collect_events_doc(
            [query],
            span,
            60,
            timeout,
            allow_t3=False,
            allow_filters=True,
        )
        notes.extend(span_notes)
        pass1_items = raw_items
        built, meta = build_event_items("SECTOR", raw_items, ["GDELT_DOC"], penalty, sector_filter=sector)
        deduped = dedup_event_items(built, meta)
        deduped = apply_domain_soft_cap_events(deduped, SAME_DOMAIN_SOFT_CAP)
        deduped.sort(key=lambda x: (x.relevance_score, x.quality_score), reverse=True)
        pass1_events = deduped
        pass1_meta = meta
        if len(deduped) >= 2:
            break

    giants_hit_count = count_giant_hits(pass1_items, giants)
    need_backstop = len(pass1_events) < 2 or giants_hit_count < BACKSTOP_MIN_HITS

    combined.extend(pass1_events)
    meta_all.update(pass1_meta)

    if need_backstop and giants:
        query = build_sector_query_backstop(sector)
        raw_items, span_notes, penalty = collect_events_doc(
            [query],
            "24h",
            BACKSTOP_MAXRECORDS,
            timeout,
            allow_t3=False,
            allow_filters=True,
        )
        notes.extend(span_notes)
        raw_items = [it for it in raw_items if allow_backstop_domain(extract_domain(it.url))]
        built, meta = build_event_items("SECTOR", raw_items, ["GDELT_DOC"], penalty, sector_filter=sector)
        combined.extend(built)
        meta_all.update(meta)
        notes.append(f"sector_backstop:{sector.lower()}")

    if not combined:
        return [], meta_all, notes

    deduped = dedup_event_items(combined, meta_all)
    deduped = apply_domain_soft_cap_events(deduped, SAME_DOMAIN_SOFT_CAP)
    deduped.sort(key=lambda x: (x.relevance_score, x.quality_score), reverse=True)
    return deduped, meta_all, notes


def fetch_sector_events(timespan: str, timeout: float) -> Tuple[List[EventItem], List[str]]:
    notes: List[str] = []
    selected_sectors = select_sectors_for_rotation()
    combined: List[EventItem] = []
    meta_all: Dict[int, dict] = {}

    for sector in selected_sectors:
        events, meta, sector_notes = fetch_sector_events_for_sector(sector, timespan, timeout)
        notes.extend(sector_notes)
        combined.extend(events)
        meta_all.update(meta)

    if not combined:
        return [], notes

    deduped = dedup_event_items(combined, meta_all)
    deduped = apply_domain_soft_cap_events(deduped, SAME_DOMAIN_SOFT_CAP)
    deduped.sort(key=lambda x: (x.relevance_score, x.quality_score), reverse=True)
    update_sector_rolling(deduped, meta_all)
    return deduped[:TOP_K_PER_CATEGORY], notes


def build_event_feed_from_news(items: List[NewsItem]) -> EventFeed:
    buckets: Dict[str, List[NewsItem]] = {"REGIONAL": [], "COMPANY": [], "SECTOR": [], "PERSONAL": []}

    for item in items:
        title = item.title or ""
        if not title:
            continue
        if build_person_entities(title):
            buckets["PERSONAL"].append(item)
            continue
        sector_name, _, _, _ = detect_sector(title)
        if sector_name:
            buckets["SECTOR"].append(item)
            continue
        countries, _, _, _ = match_countries_detailed(title, "REGIONAL")
        if countries:
            buckets["REGIONAL"].append(item)
            continue
        companies, _, _ = match_companies_detailed(title)
        if companies:
            buckets["COMPANY"].append(item)

    output: Dict[str, List[EventItem]] = {"REGIONAL": [], "COMPANY": [], "SECTOR": [], "PERSONAL": []}

    for category, bucket_items in buckets.items():
        if not bucket_items:
            continue
        built, meta = build_event_items(category, bucket_items, ["GDELT_DOC"], 0.2)
        deduped = dedup_event_items(built, meta)
        deduped = apply_domain_soft_cap_events(deduped, SAME_DOMAIN_SOFT_CAP)
        deduped.sort(key=lambda x: (x.relevance_score, x.quality_score), reverse=True)
        output[category] = deduped[:TOP_K_PER_CATEGORY]

    return EventFeed(
        regional=output["REGIONAL"],
        company=output["COMPANY"],
        sector=output["SECTOR"],
        personal=output["PERSONAL"],
        sources_used=[],
        provider_health=["rss_fallback_event_feed"],
    )


def fetch_personal_events_v4(timespan: str, timeout: float) -> Tuple[List[EventItem], List[str]]:
    notes: List[str] = []
    queries = build_personal_queries()

    if timespan in TIMESPAN_FALLBACK:
        start = TIMESPAN_FALLBACK.index(timespan)
        span_order = TIMESPAN_FALLBACK[start:]
    else:
        span_order = [timespan] + TIMESPAN_FALLBACK

    events: List[EventItem] = []
    for span in span_order:
        raw_items, span_notes = collect_personal_news(queries, span, 40, timeout, allow_t3=False)
        notes.extend(span_notes)
        built, meta = build_event_items("PERSONAL", raw_items, ["GDELT_CONTEXT"], 0.0)
        deduped = dedup_event_items(built, meta)
        deduped = apply_domain_soft_cap_events(deduped, SAME_DOMAIN_SOFT_CAP)
        deduped.sort(key=lambda x: (x.relevance_score, x.quality_score), reverse=True)
        events = deduped[:TOP_K_PER_CATEGORY]
        if len(events) >= 3:
            return events, notes

    raw_items, span_notes = collect_personal_news(queries, "24h", 40, timeout, allow_t3=True)
    notes.extend(span_notes)
    built, meta = build_event_items("PERSONAL", raw_items, ["GDELT_CONTEXT"], 0.0)
    deduped = dedup_event_items(built, meta)
    deduped = apply_domain_soft_cap_events(deduped, SAME_DOMAIN_SOFT_CAP)
    deduped.sort(key=lambda x: (x.relevance_score, x.quality_score), reverse=True)
    return deduped[:TOP_K_PER_CATEGORY], notes


def build_event_feed(watchlist: List[str], timespan: str, timeout: float) -> Tuple[EventFeed, List[str]]:
    notes: List[str] = []
    start_ts = time.monotonic()
    regional, n1 = fetch_category_events("REGIONAL", timespan, timeout, watchlist)
    notes.extend(n1)
    personal_budget_ms = min(EVENT_TIME_BUDGET * 1000 * 0.25, PERSONAL_BUDGET_MS)
    personal_timeout = min(timeout, max(0.5, personal_budget_ms / 1000.0))
    personal_start = time.monotonic()
    personal, n4 = fetch_personal_events_v4(timespan, personal_timeout)
    notes.extend(n4)
    personal_used_ms = int((time.monotonic() - personal_start) * 1000)
    notes.append(f"personal_budget_ms={int(personal_budget_ms)}")
    notes.append(f"personal_budget_used_ms={personal_used_ms}")
    if time.monotonic() - start_ts > EVENT_TIME_BUDGET:
        notes.append("event_time_budget_exceeded")
        feed = EventFeed(
            regional=regional,
            company=[],
            sector=[],
            personal=personal,
            sources_used=["GDELT_DOC", "GDELT_CONTEXT"],
            provider_health=[n for n in notes if "gdelt" in n][:5],
        )
        return feed, notes

    company, n2 = fetch_category_events("COMPANY", timespan, timeout, watchlist)
    notes.extend(n2)
    if time.monotonic() - start_ts > EVENT_TIME_BUDGET:
        notes.append("event_time_budget_exceeded")
        feed = EventFeed(
            regional=regional,
            company=company,
            sector=[],
            personal=personal,
            sources_used=["GDELT_DOC", "GDELT_CONTEXT"],
            provider_health=[n for n in notes if "gdelt" in n][:5],
        )
        return feed, notes

    sector, n3 = fetch_sector_events(timespan, timeout)
    notes.extend(n3)
    if time.monotonic() - start_ts > EVENT_TIME_BUDGET:
        notes.append("event_time_budget_exceeded")
        feed = EventFeed(
            regional=regional,
            company=company,
            sector=sector,
            personal=personal,
            sources_used=["GDELT_DOC", "GDELT_CONTEXT"],
            provider_health=[n for n in notes if "gdelt" in n][:5],
        )
        return feed, notes

    total = len(regional) + len(company) + len(sector) + len(personal)
    if total > TOP_K_TOTAL_EVENTS:
        regional = regional[:TOP_K_PER_CATEGORY]
        company = company[:TOP_K_PER_CATEGORY]
        sector = sector[:TOP_K_PER_CATEGORY]
        personal = personal[:TOP_K_PER_CATEGORY]

    sources_used = ["GDELT_DOC", "GDELT_CONTEXT"]
    provider_health = [n for n in notes if "gdelt" in n]
    feed = EventFeed(
        regional=regional,
        company=company,
        sector=sector,
        personal=personal,
        sources_used=sources_used,
        provider_health=provider_health[:5],
    )
    return feed, notes
def normalize_finnhub(raw: list[dict]) -> List[NewsItem]:
    items: List[NewsItem] = []
    for a in raw:
        title = (a.get("headline") or "").strip()
        url = (a.get("url") or "").strip()
        source = (a.get("source") or "").strip().lower()
        description = (a.get("summary") or "").strip()
        ts = a.get("datetime")
        if not title or not url:
            continue
        if has_non_latin_heavy(title):
            continue
        domain = extract_domain(url)
        if domain in BLOCKLIST:
            continue
        published = None
        if isinstance(ts, (int, float)) and ts > 0:
            published = datetime.fromtimestamp(ts, tz=timezone.utc)
        items.append(
            NewsItem(
                title=title,
                url=url,
                source=source or domain,
                description=description or None,
                publishedAtISO=to_iso(published),
            )
        )
    return items
