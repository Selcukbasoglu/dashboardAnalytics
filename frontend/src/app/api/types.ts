export type NewsItem = {
  title: string;
  url: string;
  source: string;
  publishedAtISO: string | null;
  tags: string[];
  score: number;
  tier_score: number;
  ts?: string | null;
  category?: string | null;
  entities?: string[];
  event_type?: string | null;
  impact_channel?: string[];
  asset_class_bias?: string[];
  expected_direction_short_term?: string | null;
  relevance_score?: number;
  quality_score?: number;
  dedup_cluster_id?: string | null;
  other_sources?: string[];
  short_summary?: string | null;
  impact_potential?: number;
  person_event?: PersonEvent | null;
  news_scope?: string | null;
  scope_score?: number;
  scope_signals?: string[];
  sector_impacts?: SectorImpact[] | null;
  max_sector_impact?: number;
  sector_summary?: string | null;
};

export type PersonEvent = {
  actor_name: string | null;
  actor_group: string | null;
  statement_type: "SPEECH" | "INTERVIEW" | "TWEET" | "PRESS" | "OTHER";
  stance:
    | "HAWKISH"
    | "DOVISH"
    | "NEUTRAL"
    | "RISK_ESCALATE"
    | "RISK_DEESCALATE"
    | "UNKNOWN";
  impact_channel: string[];
  asset_class_bias: string[];
  expected_direction_short_term: string | null;
  impact_potential: number;
  confidence: number;
  match_confidence?: number | null;
  match_basis?: string | null;
};

export type SectorImpact = {
  sector: string;
  direction: string;
  confidence?: number;
  rationale?: string | null;
  impact_score?: number;
};

export type LeadersGroupKey = "states" | "energy" | "sp500" | "crypto_ceos";

export type LeadersGroup = {
  key: LeadersGroupKey;
  title: string;
  items: NewsItem[];
};

export type CompanyEntity = {
  name: string;
  ticker?: string | null;
  confidence: number;
  match_type: "strong" | "weak";
};

export type CryptoEntity = {
  symbol: string;
  coingecko_id?: string | null;
  confidence: number;
  match_type: "strong" | "weak";
};

export type CountryEntity = {
  country: string;
  iso2?: string | null;
  confidence: number;
  match_type: "strong" | "weak";
};

export type PersonEntity = {
  name: string;
  group: string;
  org?: string | null;
  country?: string | null;
  confidence: number;
};

export type EventEntities = {
  companies: CompanyEntity[];
  countries: CountryEntity[];
  persons: PersonEntity[];
  crypto: CryptoEntity[];
};

export type CountryTag = {
  country: string;
  iso2?: string | null;
  region?: string | null;
  hit_count: number;
  confidence: number;
};

export type ImpactedAsset = {
  asset_type: "equity" | "crypto";
  symbol_or_id: string;
  reason: "direct" | "sector" | "regional";
  confidence: number;
};

export type MarketReaction = {
  asset_type: "equity" | "crypto";
  symbol_or_id: string;
  window: "pre[-5,-1]" | "post[0,+2]";
  price_change_pct: number;
  volume_change_pct: number;
  prepricing_tag: string;
  confidence: number;
};

export type EventItem = {
  event_category: "REGIONAL" | "COMPANY" | "SECTOR" | "PERSONAL";
  entities: EventEntities;
  title: string | null;
  ts: string | null;
  source_domain: string | null;
  source_url: string | null;
  short_summary: string | null;
  sector_name: string | null;
  country_tags: CountryTag[];
  sector_confidence: number;
  country_confidence: number;
  impacted_assets: ImpactedAsset[];
  market_reaction: MarketReaction[];
  prepricing_tag: string | null;
  prepricing_score: number | null;
  impact_label: "POS" | "NEG" | "NEUTRAL" | "UNKNOWN" | null;
  relevance_score: number;
  quality_score: number;
  dedup_cluster_id: string | null;
  other_sources: string[];
  sources_used: string[];
  overall_confidence: number;
  debug_flags?: string[];
};

export type EventFeed = {
  regional: EventItem[];
  company: EventItem[];
  sector: EventItem[];
  personal: EventItem[];
  sources_used: string[];
  provider_health: string[];
};

export type ForecastDrivers = {
  raw_score?: number;
  market_score?: number;
  news_score?: number;
  evidence?: string[];
  feature_contrib?: Array<{
    name: string;
    value: number;
    weight: number;
    contribution: number;
  }>;
  news_contrib?: Array<{
    cluster_id?: string;
    headline?: string;
    contribution: number;
  }>;
  rank_reason?: string[];
  contrib_sum?: number;
  clusters?: Array<{
    cluster_id: string;
    headline: string;
    impact: number;
    credibility: number;
    source_tier: string;
    direction: number;
    tags: string[];
    scope?: string | null;
    sectors?: string[];
    realized_ret?: number | null;
    realized_z?: number | null;
  }>;
};

export type ForecastView = {
  forecast_id: string;
  ts_utc: string;
  tf: string;
  target: string;
  direction: "UP" | "DOWN" | "NEUTRAL";
  confidence: number;
  expires_at_utc: string;
  rationale_text?: string | null;
  drivers?: ForecastDrivers;
};

export type ForecastMetrics = {
  tf: string;
  hit_rate_24h: number;
  hit_rate_7d: number;
  brier_24h: number;
  brier_7d: number;
  flip_rate_7d: number;
  coverage_24h: number;
  calibration_error_7d?: number;
  reliability_bins_7d?: Array<{ bin: number; count: number; avg_pred: number; avg_obs: number }>;
};

export type EventClusterView = {
  cluster_id: string;
  headline: string;
  ts_utc: string;
  source_tier: string;
  tags: string[];
  impact_score: number;
  credibility_score: number;
  severity_score: number;
  direction: number;
  targets: Array<{ asset: string; relevance: number }>;
  realized_impacts?: Array<{
    target: string;
    tf: string;
    realized_ret: number | null;
    realized_z: number | null;
    computed_at?: string | null;
  }>;
};

export type EventClusterResponse = {
  last_scan_ts?: string | null;
  clusters: EventClusterView[];
  data_status?: string;
  fetched_at?: string;
  stale_reason?: string;
};

export type LeadersPageResponse = {
  tsISO: string;
  category: LeadersGroupKey;
  page: number;
  pageSize: number;
  total: number;
  items: NewsItem[];
};

export type NewsPageResponse = {
  tsISO: string;
  page: number;
  pageSize: number;
  total: number;
  filter: string;
  items: NewsItem[];
  timespan: string;
};

export type MarketSnapshot = {
  tsISO: string;
  coingecko: {
    btc_price_usd: number;
    eth_price_usd: number;
    btc_chg_24h: number;
    eth_chg_24h: number;
    total_vol_usd: number;
    total_mcap_usd: number;
    dominance: {
      btc: number;
      eth: number;
      usdt: number;
      usdc: number;
    };
    deltas: {
      btc_d: number;
      usdt_d: number;
      usdc_d: number;
      total_vol: number;
      total_mcap: number;
    };
    altcoin_total_value_ex_btc_usd?: number | null;
    altcoin_total_value_ex_btc_source?: string | null;
    altcoin_total_value_ex_btc_ts_utc?: string | null;
    altcoin_total_value_ex_btc_tsISO?: string | null;
  };
  yahoo: {
    dxy: number;
    qqq: number;
    nasdaq: number;
    ftse: number;
    eurostoxx: number;
    oil: number;
    gold: number;
    silver: number;
    copper: number;
    bist: number;
    vix: number;
    btc: number;
    eth: number;
    btc_chg_24h: number;
    eth_chg_24h: number;
    dxy_chg_24h: number;
    qqq_chg_24h: number;
    nasdaq_chg_24h: number;
    ftse_chg_24h: number;
    eurostoxx_chg_24h: number;
    oil_chg_24h: number;
    gold_chg_24h: number;
    silver_chg_24h: number;
    copper_chg_24h: number;
    bist_chg_24h: number;
  };
  snapshot_meta?: {
    used_fallback?: boolean;
    providers?: Record<
      string,
      {
        source: string;
        is_fallback: boolean;
        freshness_seconds?: number | null;
        degraded_mode?: boolean;
      }
    >;
  };
};

export type EventPoint = {
  id: string;
  title: string;
  tsISO: string;
  timeframe: string;
  volume_z: number;
  pre_post_ratio: number;
  price_move_pct: number;
  event_id?: string | null;
  headline?: string | null;
  published_at_utc?: string | null;
  published_at_tsi?: string | null;
  scope?: string | null;
  sectors?: string[];
  reactions?: Record<
    string,
    {
      pre?: { ret: number | null; z: number | null } | null;
      post?: Record<string, { ret: number | null; z: number | null }>;
      around?: Record<string, { ret: number | null; z: number | null }>;
      pre_30m_ret?: number | null;
      post_30m_ret?: number | null;
      volume_z?: number;
      spark_pre?: number[];
      spark_post?: number[];
      data_status?: "ok" | "partial" | "missing" | string;
      missing_fields?: string[];
    }
  >;
  combined?: { mode: string; severity: number } | null;
};

export type FlowPanel = {
  flow_score: number;
  evidence: string[];
  watch_metrics: string[];
  event_study: EventPoint[];
};

export type DerivativesPanel = {
  open_interest: number;
  funding_rate: number;
  liquidations_24h: number;
  status: string;
};

export type RiskPanel = {
  flags: string[];
  rsi: number;
  funding_z: number;
  oi_delta: number;
  fear_greed?: number | null;
};

export type DebugInfo = {
  data_missing: string[];
  notes: string[];
  provider_metrics_summary?: string | null;
  providers?: ProviderDebug[];
};

export type ProviderDebug = {
  source: string;
  ok: boolean;
  latency_ms: number;
  cache_hit: boolean;
  error_code: string | null;
  error_msg: string | null;
  degraded_mode: boolean;
  last_good_age_s: number | null;
};

export type PortfolioHolding = {
  symbol: string;
  qty: number;
  price: number;
  currency: "TRY" | "USD" | string;
  yahoo_symbol: string;
  asset_class: string;
  sector?: string | null;
  mkt_value_base: number;
  weight: number;
  data_status: "ok" | "missing" | "partial";
  vol_30d?: number | null;
};

export type PortfolioAllocation = {
  by_asset_class: Record<string, number>;
  by_currency: Record<string, number>;
};

export type PortfolioRisk = {
  vol_30d: number;
  var_95_1d: number;
  max_weight: number;
  hhi: number;
  data_status: "ok" | "partial";
};

export type PortfolioNewsImpactItem = {
  title: string;
  url: string;
  publishedAtISO: string | null;
  matchedSymbols: string[];
  match_debug: Array<{ symbol: string; method: string; score: number; matched_phrase?: string | null }>;
  impactScore: number;
  direction: "positive" | "negative" | "neutral";
  low_signal: boolean;
  impact_by_symbol: Record<string, number>;
  impact_by_symbol_direct?: Record<string, number>;
  impact_by_symbol_indirect?: Record<string, number>;
  evidence?: any;
};

export type PortfolioRecommendation = {
  period: "daily" | "weekly" | "monthly";
  actions: Array<{
    symbol: string;
    action: "increase" | "decrease";
    deltaWeight: number;
    reason: string[];
    confidence: number;
    score_breakdown?: Record<string, number>;
  }>;
  status: "ok" | "partial" | "hold";
  notes: string[];
  turnover_cap: number;
  mode?: "HOLD" | "ACTIVE";
  hold_reason?: string;
};

export type PortfolioResponse = {
  asOfISO: string;
  baseCurrency: "TRY" | "USD";
  fx: { USDTRY: number; status: string; source: string };
  holdings: PortfolioHolding[];
  allocation: PortfolioAllocation;
  risk: PortfolioRisk;
  newsImpact: {
    horizon: "24h" | "7d" | "30d";
    items: PortfolioNewsImpactItem[];
    relatedNews?: Record<
      string,
      Array<{
        title: string | null;
        url: string | null;
        publishedAtISO: string | null;
        direction?: "positive" | "negative" | "neutral";
        impactScore?: number;
        low_signal?: boolean;
      }>
    >;
    headlines?: Array<{
      title: string;
      source?: string | null;
      publishedAtISO?: string | null;
      url?: string | null;
    }>;
    headline_count?: number;
    local_headlines?: Array<{
      title: string;
      source?: string | null;
      publishedAtISO?: string | null;
      url?: string | null;
    }>;
    local_headline_count?: number;
    summary: {
      impact_by_symbol: Record<string, number>;
      impact_by_symbol_direct?: Record<string, number>;
      impact_by_symbol_indirect?: Record<string, number>;
      low_signal_ratio?: number;
      coverage_ratio?: number;
    };
    coverage: { total: number; matched: number };
  };
  portfolioSummary?: {
    provider?: string;
    summary?: string | null;
    data_status?: "ok" | "partial";
    error?: string;
    meta?: {
      period?: "daily" | "weekly" | "monthly";
      horizon?: "24h" | "7d" | "30d";
      news_titles_sent?: number;
      local_titles_sent?: number;
      news_titles_sent_total?: number;
    };
  };
  recommendations: PortfolioRecommendation[];
  optimizer_inputs?: { mom_z_weighted?: number };
  debug_notes: string[];
};

export type DailyEquityMoverEvidence = {
  event_id: string | null;
  category: string | null;
  relevance: number;
  quality: number;
};

export type DailyEquityMoverItem = {
  ticker: string;
  company_name: string | null;
  sector: string | null;
  expected_direction: "UP" | "DOWN" | "NEUTRAL";
  expected_move_band_pct: number;
  move_score: number;
  confidence: number;
  horizon: string;
  why: string;
  catalysts: string[];
  evidence: DailyEquityMoverEvidence[];
  prepricing_tag: string;
  pricing_status: "OK" | "MISSING" | "DISABLED";
};

export type DailyEquityMoversDebug = {
  candidates_seen: number;
  dropped_non_us: number;
  reason_if_empty?: string | null;
};

export type DailyEquityMovers = {
  asof: string | null;
  universe: string;
  method_version: string;
  items: DailyEquityMoverItem[];
  debug: DailyEquityMoversDebug;
};

export type CryptoOutlook = {
  asof: string | null;
  btc_bias: number;
  eth_bias: number;
  confidence: number;
  drivers: string[];
  watch_metrics: string[];
};

export type ForecastPanel = {
  crypto_outlook: CryptoOutlook;
};

export type DerivativesPoint = {
  t: number;
  v: number;
};

export type DerivativesSeries = {
  latest: number;
  series: DerivativesPoint[];
};

export type DerivativesComputed = {
  funding_z: number;
  oi_delta_pct: number;
};

export type DerivativesHealth = {
  latency_ms: number;
  cache_hit: boolean;
  error: string;
  degraded_mode: boolean;
  last_good_age_s: number;
};

export type DerivativesResponse = {
  ts: string;
  exchange: string;
  symbol: string;
  funding: DerivativesSeries;
  oi: DerivativesSeries;
  computed: DerivativesComputed;
  health: DerivativesHealth;
};

export type IntelResponse = {
  tsISO: string;
  timeframe: string;
  newsTimespan: string;
  market: MarketSnapshot;
  leaders: LeadersGroup[];
  top_news: NewsItem[];
  event_feed: EventFeed;
  flow: FlowPanel;
  derivatives: DerivativesPanel;
  risk: RiskPanel;
  debug: DebugInfo;
  daily_equity_movers: DailyEquityMovers;
  forecast: ForecastPanel;
  etag?: string | null;
  block_hashes?: Record<string, string>;
  changed_blocks?: string[];
};
