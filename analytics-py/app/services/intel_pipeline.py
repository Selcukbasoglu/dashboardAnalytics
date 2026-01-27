from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import List

from app.config import Settings
from app.engine.event_study import compute_event_study
from app.engine.flow import build_flow
from app.engine.movers import build_crypto_outlook, build_daily_equity_movers
from app.engine.event_feed import (
    build_event_feed,
    build_event_feed_from_news,
    build_leaders_from_news,
    build_news_summary,
    fetch_news,
)
from app.engine.signals import build_risk_flags, compute_fear_greed
from app.infra.cache import cache_key, now_iso
from app.infra.db import DB, purge_old
from app.infra.metrics import hash_block, provider_metrics_summary
from app.llm.openai_client import generate_summary
from app.models import (
    DebugInfo,
    DerivativesPanel,
    FlowPanel,
    IntelRequest,
    IntelResponse,
    LeadersGroup,
    MarketSnapshot,
    NewsItem,
    EventFeed,
    RiskPanel,
)
from app.providers.base import ProviderResult
from app.providers.binance_futures import fetch_binance_derivatives
from app.providers.coinpaprika import get_coinpaprika_global
from app.providers.coingecko import fetch_coingecko
from app.providers.yahoo import (
    PRIMARY_CANDLE_SYMBOLS,
    fetch_asset_candles,
    fetch_yahoo_snapshot,
    get_asset_candles,
    get_btc_candles,
    resample_candles,
)
from app.providers.candle_fallbacks import fetch_finnhub_candles, fetch_twelvedata_candles
from app.services.quote_router import get_quote_router
from app.services.altcoin import compute_altcoin_total_value_ex_btc
from app.services.global_overview import normalize_coingecko_global, select_global_overview
from app.services.event_store import record_ingest, should_ingest, store_events, get_kv, set_kv, backfill_event_targets
from app.services.forecasting import generate_forecasts, score_expired_forecasts, sync_price_bars
from app.services.event_impact import compute_realized_impacts, upsert_realized_impacts, fetch_cluster_times
from app.engine.news_engine import DEFAULT_NEWS_RANK_PROFILES, normalize_rank_weights


_YAHOO_DB_FALLBACK_MAP = {
    "nasdaq": ("NASDAQ", "nasdaq_chg_24h"),
    "ftse": ("FTSE", "ftse_chg_24h"),
    "eurostoxx": ("EUROSTOXX", "eurostoxx_chg_24h"),
    "bist": ("BIST", "bist_chg_24h"),
    "dxy": ("DXY", "dxy_chg_24h"),
    "qqq": ("QQQ", "qqq_chg_24h"),
    "oil": ("OIL", "oil_chg_24h"),
    "gold": ("GOLD", "gold_chg_24h"),
    "silver": ("SILVER", "silver_chg_24h"),
    "copper": ("COPPER", "copper_chg_24h"),
    "vix": ("VIX", None),
}


def _apply_yahoo_db_fallback(db: DB, snapshot: dict, debug: DebugInfo) -> dict:
    patched = dict(snapshot)
    for key, (asset, chg_key) in _YAHOO_DB_FALLBACK_MAP.items():
        value = snapshot.get(key, 0)
        if value not in (0, None):
            continue
        row = db.fetchone(
            "SELECT ts_utc, close FROM price_bars WHERE asset = ? ORDER BY ts_utc DESC LIMIT 1",
            (asset,),
        )
        if not row or not row["close"]:
            continue
        last_close = float(row["close"])
        patched[key] = last_close
        if chg_key:
            try:
                last_dt = datetime.fromisoformat(row["ts_utc"].replace("Z", "+00:00"))
            except Exception:
                last_dt = datetime.now(timezone.utc)
            cutoff = (last_dt - timedelta(hours=24)).isoformat().replace("+00:00", "Z")
            prev = db.fetchone(
                "SELECT close FROM price_bars WHERE asset = ? AND ts_utc <= ? ORDER BY ts_utc DESC LIMIT 1",
                (asset, cutoff),
            )
            if prev and prev["close"]:
                prev_close = float(prev["close"])
                if prev_close:
                    patched[chg_key] = (last_close - prev_close) / prev_close * 100.0
        debug.notes.append(f"yahoo_db_fallback:{key}")
    return patched


def _detect_news_regime(yahoo: dict) -> str:
    vix = float(yahoo.get("vix") or 0.0)
    risk_off = float(os.getenv("NEWS_RANK_RISK_OFF_VIX", "22") or 22)
    high_vol = float(os.getenv("NEWS_RANK_HIGH_VOL_VIX", "28") or 28)
    if vix >= high_vol:
        return "high_volatility"
    if vix >= risk_off:
        return "risk_off"
    return "default"


def _select_news_rank_weights(settings: Settings, yahoo: dict) -> dict:
    profile_override = (settings.news_rank_profile or "").strip()
    profiles = {**DEFAULT_NEWS_RANK_PROFILES, **(settings.news_rank_profiles or {})}
    if profile_override:
        selected = profiles.get(profile_override) or settings.news_rank_weights
        return normalize_rank_weights(selected)
    if settings.news_rank_profile_auto:
        regime = _detect_news_regime(yahoo)
        selected = profiles.get(regime) or settings.news_rank_weights
        return normalize_rank_weights(selected)
    return normalize_rank_weights(settings.news_rank_weights)


def _should_fetch_bars(db: DB, asset: str, min_minutes: int) -> bool:
    last = get_kv(db, f"bars:last_fetch:{asset}")
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
    except Exception:
        return True
    return datetime.now(timezone.utc) - last_dt >= timedelta(minutes=min_minutes)


def _sync_additional_price_bars(db: DB, timeout: float, min_minutes: int = 12) -> None:
    due: dict[str, str] = {}
    for asset, symbol in PRIMARY_CANDLE_SYMBOLS.items():
        if asset == "BTC":
            continue
        if _should_fetch_bars(db, asset, min_minutes):
            due[asset] = symbol
    if not due:
        return
    results = fetch_asset_candles(due, interval="15m", timeout=timeout, max_workers=4)
    now = now_iso()
    for asset in due:
        df = results.get(asset)
        if df is not None and not df.empty:
            sync_price_bars(db, asset, df)
        set_kv(db, f"bars:last_fetch:{asset}", now)


class IntelPipelineService:
    def __init__(self, settings: Settings, cache, db: DB):
        self.settings = settings
        self.cache = cache
        self.db = db
        self.last_news_stats = {
            "items_fetched": 0,
            "rss_fallback_used": False,
            "clusters_built": 0,
            "dedup_count": 0,
            "query_count": 0,
            "provider_used": None,
            "tsISO": None,
        }

    def get_news_stats(self) -> dict:
        return dict(self.last_news_stats)

    def _update_news_stats(self, items_fetched: int, notes: list[str], event_feed: EventFeed | None) -> None:
        rss_fallback = any(note == "rss_fallback" for note in notes)
        dedup_count = 0
        query_count = 0
        provider_used = None
        for note in notes:
            if note.startswith("dedup_count:"):
                try:
                    dedup_count = int(note.split(":", 1)[1])
                except Exception:
                    dedup_count = 0
            if note.startswith("query_count:"):
                try:
                    query_count = int(note.split(":", 1)[1])
                except Exception:
                    query_count = 0
            if note.startswith("provider_used:"):
                provider_used = note.split(":", 1)[1]
        clusters_built = 0
        if event_feed:
            clusters_built = (
                len(event_feed.regional or [])
                + len(event_feed.company or [])
                + len(event_feed.sector or [])
                + len(event_feed.personal or [])
            )
        self.last_news_stats = {
            "items_fetched": int(items_fetched),
            "rss_fallback_used": bool(rss_fallback),
            "clusters_built": int(clusters_built),
            "dedup_count": int(dedup_count),
            "query_count": int(query_count),
            "provider_used": provider_used,
            "tsISO": now_iso(),
        }

    def run(self, req: IntelRequest) -> IntelResponse:
        debug = DebugInfo(data_missing=[], notes=[], providers=[])
        providers: List[ProviderResult] = []

        cg_result = fetch_coingecko(self.cache, self.settings.request_timeout)
        providers.append(cg_result)
        if not cg_result.ok:
            debug.data_missing.append("coingecko")
            if cg_result.error_msg:
                debug.notes.append(f"coingecko_error:{cg_result.error_msg}")

        cp_result = get_coinpaprika_global(self.cache, self.settings.request_timeout)
        providers.append(cp_result)
        if not cp_result.ok:
            debug.data_missing.append("coinpaprika")
            if cp_result.error_msg:
                debug.notes.append(f"coinpaprika_error:{cp_result.error_msg}")

        yh_result = fetch_yahoo_snapshot(self.settings.request_timeout)
        providers.append(yh_result)
        if not yh_result.ok:
            debug.data_missing.append("yahoo")
            if yh_result.error_msg:
                debug.notes.append(f"yahoo_error:{yh_result.error_msg}")

        coingecko = cg_result.data or {}
        yahoo = yh_result.data or {}
        snapshot_meta = {"used_fallback": False, "providers": {}}
        try:
            router = get_quote_router()
            yahoo, snapshot_meta = router.patch_snapshot(yahoo)
            if snapshot_meta.get("used_fallback"):
                debug.notes.append("yahoo_fallback_used")
        except Exception as exc:
            debug.notes.append(f"yahoo_fallback_error:{exc}")
        yahoo = _apply_yahoo_db_fallback(self.db, yahoo, debug)

        if coingecko:
            if coingecko.get("btc_price_usd", 0) == 0 and yahoo.get("btc", 0) > 0:
                coingecko["btc_price_usd"] = yahoo.get("btc", 0)
            if coingecko.get("eth_price_usd", 0) == 0 and yahoo.get("eth", 0) > 0:
                coingecko["eth_price_usd"] = yahoo.get("eth", 0)
            if coingecko.get("btc_chg_24h", 0) == 0 and yahoo.get("btc_chg_24h", 0) != 0:
                coingecko["btc_chg_24h"] = yahoo.get("btc_chg_24h", 0)
            if coingecko.get("eth_chg_24h", 0) == 0 and yahoo.get("eth_chg_24h", 0) != 0:
                coingecko["eth_chg_24h"] = yahoo.get("eth_chg_24h", 0)
        overview = select_global_overview(
            cp_result.data if cp_result.ok else None,
            normalize_coingecko_global(coingecko),
        )
        if overview:
            debug.notes.append(f"global_overview:{overview.get('source')}")
            alt_value = compute_altcoin_total_value_ex_btc(
                overview.get("total_mcap_usd"),
                overview.get("btc_dominance_pct"),
            )
            if alt_value is not None:
                coingecko["altcoin_total_value_ex_btc_usd"] = alt_value
                coingecko["altcoin_total_value_ex_btc_source"] = overview.get("source")
                ts_iso = overview.get("tsISO") or now_iso()
                coingecko["altcoin_total_value_ex_btc_ts_utc"] = ts_iso
                coingecko["altcoin_total_value_ex_btc_tsISO"] = ts_iso
                debug.notes.append("altcoin_total_value_ex_btc:computed")
            else:
                debug.notes.append("altcoin_total_value_ex_btc:missing_inputs")
        else:
            debug.notes.append("global_overview:missing")

        market = MarketSnapshot(
            tsISO=now_iso(),
            coingecko=coingecko,
            yahoo=yahoo,
            snapshot_meta=snapshot_meta,
        )

        deriv_result = fetch_binance_derivatives(self.cache, "BTCUSDT", timeout=self.settings.request_timeout)
        providers.append(deriv_result)
        if not deriv_result.ok:
            debug.data_missing.append("derivatives")
            if deriv_result.error_msg:
                debug.notes.append(f"derivatives_error:{deriv_result.error_msg}")

        deriv_data = deriv_result.data or {}
        derivatives = DerivativesPanel(
            open_interest=(deriv_data.get("oi") or {}).get("latest", 0.0),
            funding_rate=(deriv_data.get("funding") or {}).get("latest", 0.0),
            liquidations_24h=0.0,
            status="degraded" if deriv_result.degraded_mode else ("ok" if deriv_result.ok else "disabled"),
        )

        watchlist = req.watchlist or []
        news_query = "bitcoin crypto ethereum"
        if watchlist:
            news_query = f"{news_query} {' '.join(watchlist)}"

        rank_weights = _select_news_rank_weights(self.settings, yahoo)
        wl_key = ",".join(watchlist)
        news_cache_key = cache_key("news", req.newsTimespan, wl_key)
        cached_news = self.cache.get(news_cache_key)
        notes: List[str] = []
        low_news = False
        if cached_news:
            top_news = [NewsItem(**n) for n in cached_news]
            debug.notes.append("news_cache_hit")
        else:
            maxrecords = 48
            if req.newsTimespan.endswith("d"):
                maxrecords = 80
            top_news, notes, used_timespan = fetch_news(
                news_query,
                req.newsTimespan,
                maxrecords,
                self.settings.request_timeout,
                watchlist=watchlist,
                rank_weights=rank_weights,
            )
            debug.notes.extend(notes)
            if used_timespan != req.newsTimespan:
                debug.notes.append(f"news_timespan_fallback:{used_timespan}")

            ttl = 90
            low_news = any("haber_verisi_zayıf" in note for note in notes)
            if low_news:
                ttl = 180
            self.cache.set(news_cache_key, [n.model_dump() for n in top_news], ttl)

        last_ingest = get_kv(self.db, "news_ingest_at")
        if should_ingest(last_ingest, self.settings.news_ingest_interval_minutes):
            stored = store_events(self.db, top_news, self.settings)
            record_ingest(self.db)
            debug.notes.append(f"news_ingested:{stored}")
            purge_old(self.db, self.settings.retention_days)

        gdelt_error = next((n for n in debug.notes if n.startswith("gdelt_")), None)
        gdelt_ok = gdelt_error is None
        providers.append(
            ProviderResult(
                ok=gdelt_ok,
                source="gdelt",
                data=None,
                latency_ms=0,
                cache_hit="news_cache_hit" in debug.notes,
                error_code="429" if gdelt_error and "rate_limited" in gdelt_error else ("gdelt_error" if gdelt_error else None),
                error_msg=gdelt_error,
                degraded_mode=bool(gdelt_error and "rate_limited" in gdelt_error),
                last_good_age_s=None,
            )
        )
        providers.append(
            ProviderResult(
                ok=True,
                source="rss",
                data=None,
                latency_ms=0,
                cache_hit="news_cache_hit" in debug.notes,
                error_code=None,
                error_msg=None,
                degraded_mode=False,
                last_good_age_s=None,
            )
        )

        for line in build_news_summary(top_news, low_news=low_news):
            debug.notes.append(f"akış_özeti:{line}")

        leaders_raw = build_leaders_from_news(top_news)
        leaders = [LeadersGroup(**g) for g in leaders_raw]

        event_cache_key = cache_key("event_feed", req.newsTimespan, wl_key)
        cached_event = self.cache.get(event_cache_key)
        if cached_event:
            event_feed = EventFeed(**cached_event)
            empty_feed = not (event_feed.regional or event_feed.company or event_feed.sector or event_feed.personal)
            if empty_feed:
                cached_event = None
            else:
                debug.notes.append("event_feed_cache_hit")

        if not cached_event:
            event_feed, event_notes = build_event_feed(watchlist, req.newsTimespan, self.settings.request_timeout)
            debug.notes.extend(event_notes)
            empty_feed = not (event_feed.regional or event_feed.company or event_feed.sector or event_feed.personal)
            if empty_feed and "gdelt_rate_limited" in event_notes and top_news:
                event_feed = build_event_feed_from_news(top_news)
                debug.notes.append("event_feed_fallback_news")
                empty_feed = False
            if not empty_feed and "gdelt_rate_limited" not in event_notes and "event_feed_fallback_news" not in debug.notes:
                self.cache.set(event_cache_key, event_feed.model_dump(), 120)

        self._update_news_stats(len(top_news), notes, event_feed)

        assets = {
            "BTC": "BTC-USD",
            "ETH": "ETH-USD",
            "NQ": "NQ=F",
            "QQQ": "QQQ",
            "NDX": "^NDX",
        }
        asset_candles = fetch_asset_candles(assets, interval="15m", timeout=self.settings.request_timeout)
        fallback_symbols = {
            "BTC": {"finnhub": "BINANCE:BTCUSDT", "twelvedata": "BTC/USD"},
            "ETH": {"finnhub": "BINANCE:ETHUSDT", "twelvedata": "ETH/USD"},
            "NQ": {"finnhub": "CME_MINI:NQ1!", "twelvedata": "NQ"},
            "QQQ": {"finnhub": "QQQ", "twelvedata": "QQQ"},
            "NDX": {"finnhub": "NDX", "twelvedata": "NDX"},
        }
        # Yahoo fallback for missing event-study candles
        eth_df = asset_candles.get("ETH")
        if eth_df is None or eth_df.empty:
            eth_df = get_asset_candles("ETH-USD", "15m", self.settings.request_timeout)
            if eth_df is not None and not eth_df.empty:
                asset_candles["ETH"] = eth_df
        nq_df = asset_candles.get("NQ")
        if nq_df is None or nq_df.empty:
            nq_df = get_asset_candles("NQ=F", "15m", self.settings.request_timeout)
            if nq_df is not None and not nq_df.empty:
                asset_candles["NQ"] = nq_df
        qqq_df = asset_candles.get("QQQ")
        if qqq_df is None or qqq_df.empty:
            qqq_df = get_asset_candles("QQQ", "15m", self.settings.request_timeout)
            if qqq_df is not None and not qqq_df.empty:
                asset_candles["QQQ"] = qqq_df
        ndx_df = asset_candles.get("NDX")
        if ndx_df is None or ndx_df.empty:
            ndx_df = get_asset_candles("^NDX", "15m", self.settings.request_timeout)
            if ndx_df is not None and not ndx_df.empty:
                asset_candles["NDX"] = ndx_df
        # True provider fallback: Finnhub -> TwelveData
        has_finnhub = bool(os.getenv("FINNHUB_API_KEY"))
        has_twelvedata = bool(os.getenv("TWELVEDATA_API_KEY"))
        if not has_finnhub:
            debug.notes.append("finnhub_key_missing")
        if not has_twelvedata:
            debug.notes.append("twelvedata_key_missing")
        for key, symbols in fallback_symbols.items():
            df = asset_candles.get(key)
            if df is not None and not df.empty:
                continue
            fh_symbol = symbols.get("finnhub")
            if fh_symbol and has_finnhub:
                fh_df = fetch_finnhub_candles(fh_symbol, interval="15m", timeout=self.settings.request_timeout)
                if fh_df is not None and not fh_df.empty:
                    asset_candles[key] = fh_df
                    debug.notes.append(f"event_study_fallback:finnhub:{key}")
                    continue
            td_symbol = symbols.get("twelvedata")
            if td_symbol and has_twelvedata:
                td_df = fetch_twelvedata_candles(td_symbol, interval="15m", timeout=self.settings.request_timeout)
                if td_df is not None and not td_df.empty:
                    asset_candles[key] = td_df
                    debug.notes.append(f"event_study_fallback:twelvedata:{key}")
        btc_df = asset_candles.get("BTC")
        if btc_df is None or btc_df.empty:
            btc_df = get_btc_candles(interval="15m", period="7d", timeout=self.settings.request_timeout)
        candles_15m = btc_df
        candles = candles_15m if req.timeframe == "15m" else resample_candles(candles_15m, req.timeframe)
        if candles_15m is not None:
            asset_candles["BTC"] = candles_15m
        event_points = compute_event_study(top_news, asset_candles, timeframe=req.timeframe)
        if candles_15m is not None and not candles_15m.empty:
            sync_price_bars(self.db, "BTC", candles_15m)
            set_kv(self.db, "bars:last_fetch:BTC", now_iso())

        _sync_additional_price_bars(self.db, self.settings.request_timeout)

        last_impact = get_kv(self.db, "event_impact_backfill_at")
        if should_ingest(last_impact, 120):
            cluster_times = fetch_cluster_times(self.db, self.settings.retention_days)
            rows = compute_realized_impacts(
                self.db,
                cluster_times.keys(),
                cluster_times,
                targets=("BTC", "ETH", "ALTS"),
                lookback_days=self.settings.retention_days,
            )
            if rows:
                upsert_realized_impacts(self.db, rows)
                debug.notes.append(f"event_impact_backfill:{len(rows)}")
            set_kv(self.db, "event_impact_backfill_at", now_iso())

        last_target_backfill = get_kv(self.db, "event_target_backfill_at")
        if should_ingest(last_target_backfill, 360):
            inserted = backfill_event_targets(self.db, self.settings.retention_days)
            if inserted:
                debug.notes.append(f"event_target_backfill:{inserted}")
            set_kv(self.db, "event_target_backfill_at", now_iso())

        flow_score, evidence, watch_metrics = build_flow(coingecko, yahoo, top_news, event_points)
        flow = FlowPanel(flow_score=flow_score, evidence=evidence, watch_metrics=watch_metrics, event_study=event_points)

        flags, rsi, funding_z, oi_delta, fear_greed = build_risk_flags(yahoo, deriv_result, candles)
        if fear_greed is None:
            fallback_fg = compute_fear_greed(rsi, funding_z, oi_delta)
            if fallback_fg is not None:
                fear_greed = fallback_fg
                debug.notes.append("fear_greed_fallback")
            else:
                debug.notes.append("fear_greed_missing")
        risk = RiskPanel(flags=flags, rsi=rsi, funding_z=funding_z, oi_delta=oi_delta, fear_greed=fear_greed)

        scored = score_expired_forecasts(self.db, self.settings)
        if scored:
            debug.notes.append(f"forecast_scored:{scored}")
        generated = generate_forecasts(self.db, self.settings, market, flow, risk)
        if generated:
            debug.notes.append(f"forecast_generated:{len(generated)}")

        daily_movers = build_daily_equity_movers(event_feed)
        crypto_outlook = build_crypto_outlook(market, event_feed)

        if self.settings.enable_openai_summary and self.settings.openai_api_key:
            try:
                summary = generate_summary(
                    self.settings.openai_api_key,
                    self.settings.openai_model,
                    market.model_dump(),
                    [n.model_dump() for n in top_news[:20]],
                    self.settings.request_timeout,
                )
                if summary:
                    debug.notes.append(f"openai_summary:{summary[:200]}")
            except Exception as exc:
                debug.notes.append(f"openai_error:{exc}")
        elif not self.settings.enable_openai_summary:
            debug.notes.append("openai_disabled")

        debug.providers = [p.debug_view() for p in providers]
        debug.provider_metrics_summary = provider_metrics_summary(debug.providers)

        response = IntelResponse(
            tsISO=now_iso(),
            timeframe=req.timeframe,
            newsTimespan=req.newsTimespan,
            market=market,
            leaders=leaders,
            top_news=top_news,
            event_feed=event_feed,
            flow=flow,
            derivatives=derivatives,
            risk=risk,
            debug=debug,
            daily_equity_movers=daily_movers,
            forecast=crypto_outlook,
        )

        block_hashes = {
            "market": hash_block(market.model_dump()),
            "leaders": hash_block([g.model_dump() for g in leaders]),
            "top_news": hash_block([n.model_dump() for n in top_news]),
            "eventfeed": hash_block(event_feed.model_dump()),
            "flow": hash_block(flow.model_dump()),
            "risk": hash_block(risk.model_dump()),
            "derivatives": hash_block(derivatives.model_dump()),
            "forecast": hash_block(crypto_outlook.model_dump()),
            "daily_equity_movers": hash_block(daily_movers.model_dump()),
            "debug": hash_block(debug.model_dump()),
        }
        etag = hash_block(block_hashes)

        hash_key = cache_key("intel_hash", req.timeframe, req.newsTimespan, wl_key)
        prev = self.cache.get(hash_key) or {}
        prev_hashes = prev.get("block_hashes") or {}
        changed = [k for k, v in block_hashes.items() if prev_hashes.get(k) != v]
        self.cache.set(hash_key, {"etag": etag, "block_hashes": block_hashes}, 300)

        response.etag = etag
        response.block_hashes = block_hashes
        response.changed_blocks = changed

        return response
