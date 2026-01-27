package models

type IntelRequest struct {
	Timeframe    string   `json:"timeframe"`
	NewsTimespan string   `json:"newsTimespan"`
	Watchlist    []string `json:"watchlist"`
}

type IntelResponse struct {
	TsISO        string           `json:"tsISO"`
	Timeframe    string           `json:"timeframe"`
	NewsTimespan string           `json:"newsTimespan"`
	Market       MarketSnapshot   `json:"market"`
	Leaders      []LeadersGroup   `json:"leaders"`
	TopNews      []NewsItem       `json:"top_news"`
	EventFeed    EventFeed        `json:"event_feed"`
	Flow         FlowPanel        `json:"flow"`
	Derivatives  DerivativesPanel `json:"derivatives"`
	Risk         RiskPanel        `json:"risk"`
	Debug        DebugInfo        `json:"debug"`
	DailyMovers  DailyEquityMovers `json:"daily_equity_movers"`
	Forecast     ForecastPanel     `json:"forecast"`
	Etag         string            `json:"etag"`
	BlockHashes  map[string]string `json:"block_hashes"`
	ChangedBlocks []string         `json:"changed_blocks"`
}

type MarketSnapshot struct {
	TsISO     string            `json:"tsISO"`
	CoinGecko CoinGeckoSnapshot `json:"coingecko"`
	Yahoo     YahooSnapshot     `json:"yahoo"`
	SnapshotMeta map[string]any `json:"snapshot_meta,omitempty"`
}

type CoinGeckoSnapshot struct {
	BtcPriceUsd  float64            `json:"btc_price_usd"`
	EthPriceUsd  float64            `json:"eth_price_usd"`
	BtcChg24     float64            `json:"btc_chg_24h"`
	EthChg24     float64            `json:"eth_chg_24h"`
	TotalVolUsd  float64            `json:"total_vol_usd"`
	TotalMcapUsd float64            `json:"total_mcap_usd"`
	Dominance    map[string]float64 `json:"dominance"`
	Deltas       map[string]float64 `json:"deltas"`
	AltcoinTotalValueExBtcUsd *float64 `json:"altcoin_total_value_ex_btc_usd"`
	AltcoinTotalValueExBtcSource *string `json:"altcoin_total_value_ex_btc_source"`
	AltcoinTotalValueExBtcTsUtc *string `json:"altcoin_total_value_ex_btc_ts_utc"`
	AltcoinTotalValueExBtcTsISO *string `json:"altcoin_total_value_ex_btc_tsISO"`
}

type YahooSnapshot struct {
	DXY      float64 `json:"dxy"`
	QQQ      float64 `json:"qqq"`
	Nasdaq   float64 `json:"nasdaq"`
	FTSE     float64 `json:"ftse"`
	EuroStoxx float64 `json:"eurostoxx"`
	Oil      float64 `json:"oil"`
	Gold     float64 `json:"gold"`
	Silver   float64 `json:"silver"`
	Copper   float64 `json:"copper"`
	Bist     float64 `json:"bist"`
	VIX      float64 `json:"vix"`
	Btc      float64 `json:"btc"`
	Eth      float64 `json:"eth"`
	BtcChg24 float64 `json:"btc_chg_24h"`
	EthChg24 float64 `json:"eth_chg_24h"`
	DXYChg24 float64 `json:"dxy_chg_24h"`
	QQQChg24 float64 `json:"qqq_chg_24h"`
	NasdaqChg24 float64 `json:"nasdaq_chg_24h"`
	FTSEChg24 float64 `json:"ftse_chg_24h"`
	EuroStoxxChg24 float64 `json:"eurostoxx_chg_24h"`
	OilChg24 float64 `json:"oil_chg_24h"`
	GoldChg24 float64 `json:"gold_chg_24h"`
	SilverChg24 float64 `json:"silver_chg_24h"`
	CopperChg24 float64 `json:"copper_chg_24h"`
	BistChg24 float64 `json:"bist_chg_24h"`
}

type NewsItem struct {
	Title                      string   `json:"title"`
	URL                        string   `json:"url"`
	Source                     string   `json:"source"`
	PublishedAtISO             string   `json:"publishedAtISO"`
	Tags                       []string `json:"tags"`
	Score                      int      `json:"score"`
	TierScore                  int      `json:"tier_score"`
	Ts                         string   `json:"ts"`
	Category                   string   `json:"category"`
	Entities                   []string `json:"entities"`
	EventType                  string   `json:"event_type"`
	ImpactChannel              []string `json:"impact_channel"`
	AssetClassBias             []string `json:"asset_class_bias"`
	ExpectedDirectionShortTerm string   `json:"expected_direction_short_term"`
	RelevanceScore             int      `json:"relevance_score"`
	QualityScore               int      `json:"quality_score"`
	DedupClusterID             string   `json:"dedup_cluster_id"`
	OtherSources               []string `json:"other_sources"`
	ShortSummary               string   `json:"short_summary"`
	ImpactPotential            int      `json:"impact_potential"`
	PersonEvent                *PersonEvent `json:"person_event"`
}

type PersonEvent struct {
	ActorName                  string   `json:"actor_name"`
	ActorGroup                 string   `json:"actor_group"`
	StatementType              string   `json:"statement_type"`
	Stance                     string   `json:"stance"`
	ImpactChannel              []string `json:"impact_channel"`
	AssetClassBias             []string `json:"asset_class_bias"`
	ExpectedDirectionShortTerm string   `json:"expected_direction_short_term"`
	ImpactPotential            int      `json:"impact_potential"`
	Confidence                 int      `json:"confidence"`
}

type LeadersGroup struct {
	Key   string     `json:"key"`
	Title string     `json:"title"`
	Items []NewsItem `json:"items"`
}

type CompanyEntity struct {
	Name       string `json:"name"`
	Ticker     string `json:"ticker"`
	Confidence int    `json:"confidence"`
	MatchType  string `json:"match_type"`
}

type CryptoEntity struct {
	Symbol      string `json:"symbol"`
	CoingeckoID string `json:"coingecko_id"`
	Confidence  int    `json:"confidence"`
	MatchType   string `json:"match_type"`
}

type CountryEntity struct {
	Country    string `json:"country"`
	ISO2       string `json:"iso2"`
	Confidence int    `json:"confidence"`
	MatchType  string `json:"match_type"`
}

type PersonEntity struct {
	Name       string `json:"name"`
	Group      string `json:"group"`
	Org        string `json:"org"`
	Country    string `json:"country"`
	Confidence int    `json:"confidence"`
}

type EventEntities struct {
	Companies []CompanyEntity `json:"companies"`
	Countries []CountryEntity `json:"countries"`
	Persons   []PersonEntity  `json:"persons"`
	Crypto    []CryptoEntity  `json:"crypto"`
}

type CountryTag struct {
	Country    string `json:"country"`
	ISO2       string `json:"iso2"`
	Region     string `json:"region"`
	HitCount   int    `json:"hit_count"`
	Confidence int    `json:"confidence"`
}

type ImpactedAsset struct {
	AssetType  string `json:"asset_type"`
	SymbolOrID string `json:"symbol_or_id"`
	Reason     string `json:"reason"`
	Confidence int    `json:"confidence"`
}

type MarketReaction struct {
	AssetType       string  `json:"asset_type"`
	SymbolOrID      string  `json:"symbol_or_id"`
	Window          string  `json:"window"`
	PriceChangePct  float64 `json:"price_change_pct"`
	VolumeChangePct float64 `json:"volume_change_pct"`
	PrepricingTag   string  `json:"prepricing_tag"`
	Confidence      int     `json:"confidence"`
}

type EventItem struct {
	EventCategory     string           `json:"event_category"`
	Entities          EventEntities    `json:"entities"`
	Title             string           `json:"title"`
	Ts                string           `json:"ts"`
	SourceDomain      string           `json:"source_domain"`
	SourceURL         string           `json:"source_url"`
	ShortSummary      string           `json:"short_summary"`
	SectorName        string           `json:"sector_name"`
	CountryTags       []CountryTag     `json:"country_tags"`
	SectorConfidence  int              `json:"sector_confidence"`
	CountryConfidence int              `json:"country_confidence"`
	ImpactedAssets    []ImpactedAsset  `json:"impacted_assets"`
	MarketReaction    []MarketReaction `json:"market_reaction"`
	PrepricingTag     string           `json:"prepricing_tag"`
	PrepricingScore   *float64         `json:"prepricing_score"`
	ImpactLabel       string           `json:"impact_label"`
	RelevanceScore    int              `json:"relevance_score"`
	QualityScore      int              `json:"quality_score"`
	DedupClusterID    string           `json:"dedup_cluster_id"`
	OtherSources      []string         `json:"other_sources"`
	SourcesUsed       []string         `json:"sources_used"`
	OverallConfidence int              `json:"overall_confidence"`
	DebugFlags        []string         `json:"debug_flags"`
}

type EventFeed struct {
	Regional       []EventItem `json:"regional"`
	Company        []EventItem `json:"company"`
	Sector         []EventItem `json:"sector"`
	Personal       []EventItem `json:"personal"`
	SourcesUsed    []string    `json:"sources_used"`
	ProviderHealth []string    `json:"provider_health"`
}

type FlowPanel struct {
	FlowScore    int          `json:"flow_score"`
	Evidence     []string     `json:"evidence"`
	WatchMetrics []string     `json:"watch_metrics"`
	EventStudy   []EventPoint `json:"event_study"`
}

type EventPoint struct {
	ID           string  `json:"id"`
	Title        string  `json:"title"`
	TsISO        string  `json:"tsISO"`
	Timeframe    string  `json:"timeframe"`
	VolumeZ      float64 `json:"volume_z"`
	PrePostRatio float64 `json:"pre_post_ratio"`
	PriceMovePct float64 `json:"price_move_pct"`
	EventID      string  `json:"event_id"`
	Headline     string  `json:"headline"`
	PublishedAtUTC string `json:"published_at_utc"`
	PublishedAtTSI string `json:"published_at_tsi"`
	Scope        string  `json:"scope"`
	Sectors      []string `json:"sectors"`
	Reactions    map[string]EventReactions `json:"reactions"`
	Combined     *CombinedReaction `json:"combined"`
}

type ReactionWindow struct {
	Ret *float64 `json:"ret"`
	Z   *float64 `json:"z"`
}

type EventReactions struct {
	Pre       *ReactionWindow         `json:"pre"`
	Post      map[string]ReactionWindow `json:"post"`
	Around    map[string]ReactionWindow `json:"around"`
	Pre30mRet *float64               `json:"pre_30m_ret"`
	Post30mRet *float64              `json:"post_30m_ret"`
	VolumeZ   float64                 `json:"volume_z"`
	SparkPre  []float64               `json:"spark_pre"`
	SparkPost []float64               `json:"spark_post"`
	DataStatus string                `json:"data_status"`
	MissingFields []string           `json:"missing_fields"`
}

type CombinedReaction struct {
	Mode     string  `json:"mode"`
	Severity float64 `json:"severity"`
}

type DerivativesPanel struct {
	OpenInterest   float64 `json:"open_interest"`
	FundingRate    float64 `json:"funding_rate"`
	Liquidations24 float64 `json:"liquidations_24h"`
	Status         string  `json:"status"`
}

type RiskPanel struct {
	Flags    []string `json:"flags"`
	RSI      float64  `json:"rsi"`
	FundingZ float64  `json:"funding_z"`
	OIDelta  float64  `json:"oi_delta"`
	FearGreed *float64 `json:"fear_greed,omitempty"`
}

type DebugInfo struct {
	DataMissing []string `json:"data_missing"`
	Notes       []string `json:"notes"`
	ProviderMetricsSummary string          `json:"provider_metrics_summary"`
	Providers              []ProviderDebug `json:"providers"`
}

type ProviderDebug struct {
	Source       string `json:"source"`
	Ok           bool   `json:"ok"`
	LatencyMs    int    `json:"latency_ms"`
	CacheHit     bool   `json:"cache_hit"`
	ErrorCode    string `json:"error_code"`
	ErrorMsg     string `json:"error_msg"`
	DegradedMode bool   `json:"degraded_mode"`
	LastGoodAgeS int    `json:"last_good_age_s"`
}

type DailyEquityMoverEvidence struct {
	EventID  string `json:"event_id"`
	Category string `json:"category"`
	Relevance int   `json:"relevance"`
	Quality  int    `json:"quality"`
}

type DailyEquityMoverItem struct {
	Ticker              string                     `json:"ticker"`
	CompanyName         string                     `json:"company_name"`
	Sector              string                     `json:"sector"`
	ExpectedDirection   string                     `json:"expected_direction"`
	ExpectedMoveBandPct float64                    `json:"expected_move_band_pct"`
	MoveScore           int                        `json:"move_score"`
	Confidence          int                        `json:"confidence"`
	Horizon             string                     `json:"horizon"`
	Why                 string                     `json:"why"`
	Catalysts           []string                   `json:"catalysts"`
	Evidence            []DailyEquityMoverEvidence `json:"evidence"`
	PrepricingTag       string                     `json:"prepricing_tag"`
	PricingStatus       string                     `json:"pricing_status"`
}

type DailyEquityMoversDebug struct {
	CandidatesSeen int    `json:"candidates_seen"`
	DroppedNonUS   int    `json:"dropped_non_us"`
	ReasonIfEmpty  string `json:"reason_if_empty"`
}

type DailyEquityMovers struct {
	Asof          string                 `json:"asof"`
	Universe      string                 `json:"universe"`
	MethodVersion string                 `json:"method_version"`
	Items         []DailyEquityMoverItem `json:"items"`
	Debug         DailyEquityMoversDebug `json:"debug"`
}

type CryptoOutlook struct {
	Asof        string   `json:"asof"`
	BtcBias     int      `json:"btc_bias"`
	EthBias     int      `json:"eth_bias"`
	Confidence  int      `json:"confidence"`
	Drivers     []string `json:"drivers"`
	WatchMetrics []string `json:"watch_metrics"`
}

type ForecastPanel struct {
	CryptoOutlook CryptoOutlook `json:"crypto_outlook"`
}

type DerivativesPoint struct {
	T int64   `json:"t"`
	V float64 `json:"v"`
}

type DerivativesSeries struct {
	Latest float64            `json:"latest"`
	Series []DerivativesPoint `json:"series"`
}

type DerivativesComputed struct {
	FundingZ   float64 `json:"funding_z"`
	OIDeltaPct float64 `json:"oi_delta_pct"`
}

type DerivativesHealth struct {
	LatencyMs    int64  `json:"latency_ms"`
	CacheHit     bool   `json:"cache_hit"`
	Error        string `json:"error"`
	DegradedMode bool   `json:"degraded_mode"`
	LastGoodAgeS int64  `json:"last_good_age_s"`
}

type DerivativesResponse struct {
	Ts       string              `json:"ts"`
	Exchange string              `json:"exchange"`
	Symbol   string              `json:"symbol"`
	Funding  DerivativesSeries   `json:"funding"`
	OI       DerivativesSeries   `json:"oi"`
	Computed DerivativesComputed `json:"computed"`
	Health   DerivativesHealth   `json:"health"`
}

// External API responses

type HealthResponse struct {
	Ok          bool              `json:"ok"`
	TsISO       string            `json:"tsISO"`
	Service     string            `json:"service"`
	Version     string            `json:"version,omitempty"`
	Deps        []string          `json:"deps"`
	DepsStatus  map[string]DepStatus `json:"deps_status,omitempty"`
	DataMissing []string          `json:"data_missing"`
	Env         map[string]bool   `json:"env"`
	Features    map[string]bool   `json:"features"`
}

type DepStatus struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type NewsPageResponse struct {
	TsISO    string     `json:"tsISO"`
	Page     int        `json:"page"`
	PageSize int        `json:"pageSize"`
	Total    int        `json:"total"`
	Filter   string     `json:"filter"`
	Items    []NewsItem `json:"items"`
	Timespan string     `json:"timespan"`
}

type LeadersResponse struct {
	TsISO    string     `json:"tsISO"`
	Category string     `json:"category"`
	Page     int        `json:"page"`
	PageSize int        `json:"pageSize"`
	Total    int        `json:"total"`
	Items    []NewsItem `json:"items"`
}
