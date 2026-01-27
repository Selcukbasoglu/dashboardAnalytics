from __future__ import annotations

from typing import Any, List

from pydantic import BaseModel, Field


class IntelRequest(BaseModel):
    timeframe: str = "1h"
    newsTimespan: str = "6h"
    watchlist: List[str] | None = None


class NewsItem(BaseModel):
    title: str
    url: str
    canonical_url: str | None = None
    source: str = ""
    description: str | None = None
    content_text: str | None = None
    publishedAtISO: str | None = None
    tags: List[str] = Field(default_factory=list)
    score: int = 0
    tier_score: int = 0
    ts: str | None = None
    category: str | None = None
    entities: List[str] = Field(default_factory=list)
    event_type: str | None = None
    impact_channel: List[str] = Field(default_factory=list)
    asset_class_bias: List[str] = Field(default_factory=list)
    expected_direction_short_term: str | None = None
    relevance_score: int = 0
    quality_score: int = 0
    dedup_cluster_id: str | None = None
    other_sources: List[str] = Field(default_factory=list)
    short_summary: str | None = None
    impact_potential: int = 0
    person_event: "PersonEvent | None" = None
    news_scope: str | None = None
    scope_score: int = 0
    scope_signals: List[str] = Field(default_factory=list)
    sector_impacts: List["SectorImpact"] = Field(default_factory=list)
    max_sector_impact: int = 0
    sector_summary: str | None = None


class PersonEvent(BaseModel):
    actor_name: str | None = None
    actor_id: str | None = None
    actor_group: str | None = None
    actor_group_id: str | None = None
    statement_type: str = "OTHER"
    stance: str = "UNKNOWN"
    impact_channel: List[str] = Field(default_factory=list)
    asset_class_bias: List[str] = Field(default_factory=list)
    expected_direction_short_term: str | None = None
    impact_potential: int = 0
    confidence: int = 0
    match_confidence: int | None = None
    match_basis: str | None = None


class SectorImpact(BaseModel):
    sector: str
    direction: str
    confidence: int = 0
    rationale: str | None = None
    impact_score: int = 0


class LeadersGroup(BaseModel):
    key: str
    title: str
    items: List[NewsItem] = Field(default_factory=list)


class CompanyEntity(BaseModel):
    name: str
    ticker: str | None = None
    confidence: int = 0
    match_type: str = "weak"


class CryptoEntity(BaseModel):
    symbol: str
    coingecko_id: str | None = None
    confidence: int = 0
    match_type: str = "weak"


class CountryEntity(BaseModel):
    country: str
    iso2: str | None = None
    confidence: int = 0
    match_type: str = "weak"


class PersonEntity(BaseModel):
    name: str
    group: str
    org: str | None = None
    country: str | None = None
    confidence: int = 0


class EventEntities(BaseModel):
    companies: List[CompanyEntity] = Field(default_factory=list)
    countries: List[CountryEntity] = Field(default_factory=list)
    persons: List[PersonEntity] = Field(default_factory=list)
    crypto: List[CryptoEntity] = Field(default_factory=list)


class CountryTag(BaseModel):
    country: str
    iso2: str | None = None
    region: str | None = None
    hit_count: int = 0
    confidence: int = 0


class ImpactedAsset(BaseModel):
    asset_type: str
    symbol_or_id: str
    reason: str
    confidence: int = 0


class MarketReaction(BaseModel):
    asset_type: str
    symbol_or_id: str
    window: str
    price_change_pct: float = 0.0
    volume_change_pct: float = 0.0
    prepricing_tag: str = "UNKNOWN"
    confidence: int = 0


class EventItem(BaseModel):
    event_category: str
    entities: EventEntities
    title: str | None = None
    ts: str | None = None
    source_domain: str | None = None
    source_url: str | None = None
    short_summary: str | None = None
    sector_name: str | None = None
    country_tags: List[CountryTag] = Field(default_factory=list)
    sector_confidence: int = 0
    country_confidence: int = 0
    impacted_assets: List[ImpactedAsset] = Field(default_factory=list)
    market_reaction: List[MarketReaction] = Field(default_factory=list)
    relevance_score: int = 0
    quality_score: int = 0
    dedup_cluster_id: str | None = None
    prepricing_tag: str | None = None
    prepricing_score: float | None = None
    impact_label: str | None = None
    overall_confidence: int = 0
    other_sources: List[str] = Field(default_factory=list)
    sources_used: List[str] = Field(default_factory=list)
    debug_flags: List[str] = Field(default_factory=list)


class EventFeed(BaseModel):
    regional: List[EventItem] = Field(default_factory=list)
    company: List[EventItem] = Field(default_factory=list)
    sector: List[EventItem] = Field(default_factory=list)
    personal: List[EventItem] = Field(default_factory=list)
    sources_used: List[str] = Field(default_factory=list)
    provider_health: List[str] = Field(default_factory=list)


class CoinGeckoSnapshot(BaseModel):
    btc_price_usd: float = 0.0
    eth_price_usd: float = 0.0
    btc_chg_24h: float = 0.0
    eth_chg_24h: float = 0.0
    total_vol_usd: float = 0.0
    total_mcap_usd: float = 0.0
    dominance: dict[str, float] = Field(default_factory=dict)
    deltas: dict[str, float] = Field(default_factory=dict)
    altcoin_total_value_ex_btc_usd: float | None = None
    altcoin_total_value_ex_btc_source: str | None = None
    altcoin_total_value_ex_btc_ts_utc: str | None = None
    altcoin_total_value_ex_btc_tsISO: str | None = None


class YahooSnapshot(BaseModel):
    dxy: float = 0.0
    qqq: float = 0.0
    nasdaq: float = 0.0
    ftse: float = 0.0
    eurostoxx: float = 0.0
    oil: float = 0.0
    gold: float = 0.0
    silver: float = 0.0
    copper: float = 0.0
    bist: float = 0.0
    vix: float = 0.0
    btc: float = 0.0
    eth: float = 0.0
    btc_chg_24h: float = 0.0
    eth_chg_24h: float = 0.0
    dxy_chg_24h: float = 0.0
    qqq_chg_24h: float = 0.0
    nasdaq_chg_24h: float = 0.0
    ftse_chg_24h: float = 0.0
    eurostoxx_chg_24h: float = 0.0
    oil_chg_24h: float = 0.0
    gold_chg_24h: float = 0.0
    silver_chg_24h: float = 0.0
    copper_chg_24h: float = 0.0
    bist_chg_24h: float = 0.0


class MarketSnapshot(BaseModel):
    tsISO: str
    coingecko: CoinGeckoSnapshot
    yahoo: YahooSnapshot
    snapshot_meta: dict[str, Any] | None = None


class EventPoint(BaseModel):
    id: str
    title: str
    tsISO: str
    timeframe: str
    volume_z: float = 0.0
    pre_post_ratio: float = 1.0
    price_move_pct: float = 0.0
    event_id: str | None = None
    headline: str | None = None
    published_at_utc: str | None = None
    published_at_tsi: str | None = None
    scope: str | None = None
    sectors: List[str] = Field(default_factory=list)
    reactions: dict[str, "EventReactions"] = Field(default_factory=dict)
    combined: "CombinedReaction | None" = None


class ReactionWindow(BaseModel):
    ret: float | None = None
    z: float | None = None


class EventReactions(BaseModel):
    pre: ReactionWindow | None = None
    post: dict[str, ReactionWindow] = Field(default_factory=dict)
    around: dict[str, ReactionWindow] = Field(default_factory=dict)
    pre_30m_ret: float | None = None
    post_30m_ret: float | None = None
    volume_z: float = 0.0
    spark_pre: List[float] = Field(default_factory=list)
    spark_post: List[float] = Field(default_factory=list)
    data_status: str = "ok"
    missing_fields: List[str] = Field(default_factory=list)


class CombinedReaction(BaseModel):
    mode: str = "LOW_SIGNAL"
    severity: float = 0.0


class FlowPanel(BaseModel):
    flow_score: int
    evidence: List[str] = Field(default_factory=list)
    watch_metrics: List[str] = Field(default_factory=list)
    event_study: List[EventPoint] = Field(default_factory=list)


class DerivativesPanel(BaseModel):
    open_interest: float = 0.0
    funding_rate: float = 0.0
    liquidations_24h: float = 0.0
    status: str = "ok"


class RiskPanel(BaseModel):
    flags: List[str] = Field(default_factory=list)
    rsi: float = 0.0
    funding_z: float = 0.0
    oi_delta: float = 0.0
    fear_greed: float | None = None


class DebugInfo(BaseModel):
    data_missing: List[str] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)
    provider_metrics_summary: str | None = None
    providers: List[dict] = Field(default_factory=list)


class DailyEquityMoverEvidence(BaseModel):
    event_id: str | None = None
    category: str | None = None
    relevance: int = 0
    quality: int = 0


class DailyEquityMoverItem(BaseModel):
    ticker: str
    company_name: str | None = None
    sector: str | None = None
    expected_direction: str = "NEUTRAL"
    expected_move_band_pct: float = 0.0
    move_score: int = 0
    confidence: int = 0
    horizon: str = "0-24h"
    why: str = ""
    catalysts: List[str] = Field(default_factory=list)
    evidence: List[DailyEquityMoverEvidence] = Field(default_factory=list)
    prepricing_tag: str = "UNKNOWN"
    pricing_status: str = "MISSING"


class DailyEquityMoversDebug(BaseModel):
    candidates_seen: int = 0
    dropped_non_us: int = 0
    reason_if_empty: str | None = None


class DailyEquityMovers(BaseModel):
    asof: str | None = None
    universe: str = "US"
    method_version: str = "movers_v1"
    items: List[DailyEquityMoverItem] = Field(default_factory=list)
    debug: DailyEquityMoversDebug = Field(default_factory=DailyEquityMoversDebug)


class CryptoOutlook(BaseModel):
    asof: str | None = None
    btc_bias: int = 0
    eth_bias: int = 0
    confidence: int = 0
    drivers: List[str] = Field(default_factory=list)
    watch_metrics: List[str] = Field(default_factory=list)
    risk_mode: str = "NEUTRAL"
    risk_score: int = 0


class ForecastPanel(BaseModel):
    crypto_outlook: CryptoOutlook = Field(default_factory=CryptoOutlook)


class IntelResponse(BaseModel):
    tsISO: str
    timeframe: str
    newsTimespan: str
    market: MarketSnapshot
    leaders: List[LeadersGroup]
    top_news: List[NewsItem]
    event_feed: EventFeed = Field(default_factory=EventFeed)
    flow: FlowPanel
    derivatives: DerivativesPanel
    risk: RiskPanel
    debug: DebugInfo
    daily_equity_movers: DailyEquityMovers = Field(default_factory=DailyEquityMovers)
    forecast: ForecastPanel = Field(default_factory=ForecastPanel)
    etag: str | None = None
    block_hashes: dict[str, str] = Field(default_factory=dict)
    changed_blocks: List[str] = Field(default_factory=list)


class ForecastView(BaseModel):
    forecast_id: str
    ts_utc: str
    tf: str
    target: str
    direction: str
    confidence: float
    expires_at_utc: str
    rationale_text: str | None = None
    drivers: dict = Field(default_factory=dict)


class ForecastMetrics(BaseModel):
    tf: str
    hit_rate_24h: float
    hit_rate_7d: float
    brier_24h: float
    brier_7d: float
    flip_rate_7d: float
    coverage_24h: float


class EventClusterView(BaseModel):
    cluster_id: str
    headline: str
    ts_utc: str
    source_tier: str
    tags: List[str] = Field(default_factory=list)
    impact_score: float
    credibility_score: float
    severity_score: float
    direction: int
    targets: List[dict] = Field(default_factory=list)


class EventClusterResponse(BaseModel):
    last_scan_ts: str | None = None
    clusters: List[EventClusterView] = Field(default_factory=list)
