from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math

from app.config import Settings
from app.infra.db import DB
from app.models import FlowPanel, MarketSnapshot, RiskPanel
from app.services.event_impact import load_event_impacts


TF_MINUTES = {"15m": 15, "1h": 60, "3h": 180, "6h": 360}
TARGETS = ["BTC", "ETH", "ALTS", "STABLES"]


@dataclass
class ClusterImpact:
    cluster_id: str
    headline: str
    source_tier: str
    tags: list[str]
    direction: int
    impact: float
    credibility: float
    severity: float
    ts_utc: str
    targets: list[tuple[str, float]]


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def sync_price_bars(db: DB, asset: str, df) -> int:
    if df is None or df.empty:
        return 0
    rows = []
    for idx, row in df.iterrows():
        ts = idx.to_pydatetime().astimezone(timezone.utc)
        rows.append(
            (
                asset,
                _iso(ts),
                float(row.get("Open", 0.0)),
                float(row.get("High", 0.0)),
                float(row.get("Low", 0.0)),
                float(row.get("Close", 0.0)),
                float(row.get("Volume", 0.0)),
            )
        )
    db.insert_ignore(
        "price_bars",
        ["asset", "ts_utc", "open", "high", "low", "close", "volume"],
        rows,
    )
    return len(rows)


def _fetch_event_rows(db: DB, lookback_hours: float):
    cutoff = _iso(_now() - timedelta(hours=lookback_hours))
    return db.fetchall(
        """
        SELECT event_id, ts_utc, source_tier, headline, tags_json, impact_score,
               credibility_score, severity_score, direction, cluster_id
        FROM events WHERE ts_utc >= ?
        """,
        (cutoff,),
    )


def _fetch_asset_map(db: DB, event_ids: list[str]) -> dict[str, list[tuple[str, float]]]:
    if not event_ids:
        return {}
    placeholders = ",".join(["?"] * len(event_ids))
    rows = db.fetchall(
        f"""
        SELECT event_id, asset_or_sector, relevance_score
        FROM event_asset_map WHERE event_id IN ({placeholders})
        """,
        tuple(event_ids),
    )
    out: dict[str, list[tuple[str, float]]] = {}
    for row in rows:
        out.setdefault(row["event_id"], []).append((row["asset_or_sector"], row["relevance_score"]))
    return out


def load_clusters(db: DB, settings: Settings) -> list[ClusterImpact]:
    rows = _fetch_event_rows(db, settings.impact_half_life_hours * 3)
    event_ids = [row["event_id"] for row in rows]
    asset_map = _fetch_asset_map(db, event_ids)
    clusters: dict[str, ClusterImpact] = {}
    for row in rows:
        cluster_id = row["cluster_id"] or row["event_id"]
        impact = float(row["impact_score"] or 0.0)
        existing = clusters.get(cluster_id)
        tags = json.loads(row["tags_json"] or "[]")
        if existing is None or impact > existing.impact:
            clusters[cluster_id] = ClusterImpact(
                cluster_id=cluster_id,
                headline=row["headline"] or "—",
                source_tier=row["source_tier"] or "tier2",
                tags=tags,
                direction=int(row["direction"] or 0),
                impact=impact,
                credibility=float(row["credibility_score"] or 0.0),
                severity=float(row["severity_score"] or 0.0),
                ts_utc=row["ts_utc"],
                targets=asset_map.get(row["event_id"], []),
            )
    return list(clusters.values())


def _extract_scope(tags: list[str]) -> str | None:
    for t in tags:
        if t.lower().startswith("scope:"):
            return t.split(":", 1)[1]
    return None


def _extract_sectors(tags: list[str], max_items: int = 3) -> list[str]:
    out: list[str] = []
    for t in tags:
        if t.lower().startswith("sector:"):
            out.append(t.split(":", 1)[1])
        if len(out) >= max_items:
            break
    return out


def _decay(hours: float, half_life: float) -> float:
    if half_life <= 0:
        return 1.0
    return 0.5 ** (hours / half_life)


def _market_signal(market: MarketSnapshot, flow: FlowPanel, risk: RiskPanel, target: str) -> float:
    score, _, _ = _market_signal_details(market, flow, risk, target)
    return score


def _market_signal_details(
    market: MarketSnapshot,
    flow: FlowPanel,
    risk: RiskPanel,
    target: str,
) -> tuple[float, list[dict], list[str]]:
    deltas = (market.coingecko.deltas or {}) if market and market.coingecko else {}
    btc_d = deltas.get("btc_d")
    usdt_d = deltas.get("usdt_d")
    usdc_d = deltas.get("usdc_d")
    total_vol = deltas.get("total_vol")
    yahoo = market.yahoo if market and market.yahoo else None
    dxy_change = yahoo.dxy_chg_24h if yahoo else None
    qqq_change = yahoo.qqq_chg_24h if yahoo else None
    oil_change = yahoo.oil_chg_24h if yahoo else None
    vix_level = yahoo.vix if yahoo else None

    target_is_stables = target == "STABLES"
    features: list[dict] = []
    reasons: list[str] = []

    def _clamp(v: float, lo: float = -1.0, hi: float = 1.0) -> float:
        return max(lo, min(hi, v))

    def _add(name: str, value: float, weight: float, direction: float, reason: str | None = None):
        contrib = value * weight * direction
        features.append({"name": name, "value": round(value, 4), "weight": round(weight, 4), "contribution": contrib})
        if reason:
            reasons.append(reason)
        return contrib

    score = 0.0

    if usdt_d is not None and usdc_d is not None and total_vol is not None:
        stable_dom = usdt_d + usdc_d
        val = _clamp(stable_dom / 0.5)
        direction = 1.0 if target_is_stables else -1.0
        score += _add("stable_dom", val, 0.22, direction, "stable_dom_up" if val > 0.2 else None)

    if dxy_change is not None:
        val = _clamp(dxy_change / 2.0)
        direction = 1.0 if target_is_stables else -1.0
        score += _add("dxy", val, 0.25, direction, "usd_up" if val > 0.3 else ("usd_down" if val < -0.3 else None))

    if qqq_change is not None:
        val = _clamp(qqq_change / 2.5)
        direction = -1.0 if target_is_stables else 1.0
        score += _add("qqq", val, 0.2, direction, "qqq_up" if val > 0.3 else ("qqq_down" if val < -0.3 else None))

    if oil_change is not None:
        val = _clamp(oil_change / 3.0)
        direction = 1.0 if target_is_stables else -1.0
        score += _add("oil", val, 0.15, direction, "oil_up" if val > 0.3 else ("oil_down" if val < -0.3 else None))

    if vix_level is not None and vix_level > 0:
        val = _clamp((vix_level - 20.0) / 10.0)
        direction = 1.0 if target_is_stables else -1.0
        score += _add("vix", val, 0.15, direction, "vix_high" if val > 0.2 else None)

    if btc_d is not None:
        val = _clamp(btc_d / 0.4)
        if target == "BTC":
            direction = 1.0
        elif target == "ALTS":
            direction = -0.8
        elif target == "ETH":
            direction = -0.4
        else:
            direction = 0.0
        score += _add("btc_dominance", val, 0.18, direction, "btc_dom_up" if val > 0.3 else None)

    if flow and flow.flow_score:
        val = _clamp((flow.flow_score - 50) / 50.0)
        direction = -1.0 if target_is_stables else 1.0
        score += _add("flow", val, 0.25, direction, "flow_strong" if val > 0.3 else ("flow_weak" if val < -0.3 else None))

    if risk:
        funding_z = float(risk.funding_z or 0.0)
        oi_delta = float(risk.oi_delta or 0.0)
        if funding_z:
            val = _clamp(funding_z / 3.0)
            direction = 1.0 if target_is_stables else -1.0
            score += _add("funding_z", val, 0.2, direction, "funding_hot" if val > 0.5 else ("funding_cool" if val < -0.5 else None))
        if oi_delta:
            val = _clamp(oi_delta / 8.0)
            direction = -1.0 if target_is_stables else 1.0
            score += _add("oi_delta", val, 0.15, direction, "oi_build" if val > 0.4 else ("oi_drop" if val < -0.4 else None))
        if "MACRO_RISK_OFF" in (risk.flags or []):
            score += _add("macro_risk_off", 1.0, 0.15, 1.0 if target_is_stables else -1.0, "risk_off")
        if "LEVERAGE_HEAT" in (risk.flags or []):
            reasons.append("leverage_hot")
        if "LEVERAGE_BUILD" in (risk.flags or []):
            reasons.append("leverage_build")

    score = max(-1.0, min(1.0, score))
    return score, features, list(dict.fromkeys(reasons))


def _context_multiplier(risk: RiskPanel, direction: int) -> float:
    if not risk or not risk.flags:
        return 1.0
    if "MACRO_RISK_OFF" in risk.flags and direction < 0:
        return 1.15
    if "MACRO_RISK_OFF" in risk.flags and direction > 0:
        return 0.9
    return 1.0


def _aggregate_news_signal(
    clusters: list[ClusterImpact],
    settings: Settings,
    target: str,
    risk: RiskPanel,
    impact_overrides: dict[str, dict[str, float | None]] | None = None,
) -> tuple[float, list[ClusterImpact], list[dict]]:
    now = _now()
    total = 0.0
    scored: list[tuple[float, ClusterImpact, float, bool, dict]] = []
    neutral_weight = float(settings.thresholds.get("neutral_cluster_weight", 0.35))
    for cluster in clusters:
        if not cluster.targets:
            relevance = 0.4
        else:
            rel = [score for asset, score in cluster.targets if asset == target]
            relevance = max(rel) if rel else 0.0
        if relevance == 0.0:
            continue
        impact_norm = float(cluster.impact or 0.0) / 100.0
        credibility = float(cluster.credibility or 0.0)
        override = impact_overrides.get(cluster.cluster_id) if impact_overrides else None
        realized_ret = override.get("realized_ret") if override else None
        realized_z = override.get("realized_z") if override else None
        if realized_z is not None:
            impact_norm = min(1.0, abs(float(realized_z)) / 3.0)
        elif realized_ret is not None:
            impact_norm = min(1.0, abs(float(realized_ret)) / 5.0)
        cred_weight = 0.5 + 0.5 * min(1.0, max(0.0, credibility))
        try:
            ts = datetime.fromisoformat(cluster.ts_utc.replace("Z", "+00:00"))
        except Exception:
            ts = now
        age_hours = max(0.0, (now - ts).total_seconds() / 3600.0)
        decayed = impact_norm * _decay(age_hours, settings.impact_half_life_hours) * cred_weight
        context = _context_multiplier(risk, cluster.direction)
        base = decayed * relevance * context
        if realized_ret is not None:
            dir_sign = 0 if realized_ret == 0 else (1 if realized_ret > 0 else -1)
        else:
            dir_sign = 0 if cluster.direction == 0 else (1 if cluster.direction > 0 else -1)
        neutral = dir_sign == 0
        contrib = base * (neutral_weight if neutral else dir_sign)
        total += contrib
        scored.append(
            (
                abs(contrib),
                cluster,
                contrib,
                neutral,
                {
                    "cluster_id": cluster.cluster_id,
                    "headline": cluster.headline,
                    "contrib": contrib,
                    "impact": impact_norm,
                    "credibility": credibility,
                    "relevance": relevance,
                    "age_hours": round(age_hours, 2),
                    "direction": cluster.direction,
                    "realized_ret": realized_ret,
                    "realized_z": realized_z,
                    "neutral": neutral,
                },
            )
        )
    scored.sort(key=lambda x: x[0], reverse=True)
    top_clusters = [c for _, c, _, _, _ in scored[:3]]
    top_contribs = [meta for _, _, _, _, meta in scored[:3]]
    return max(-1.0, min(1.0, total)), top_clusters, top_contribs


def _confidence_from_score(score: float, min_conf: float) -> float:
    base = min_conf + (1 - min_conf) * min(1.0, abs(score))
    return max(min_conf, min(0.95, base))


def _direction(score: float, neutral_band: float) -> str:
    if abs(score) < neutral_band:
        return "NEUTRAL"
    return "UP" if score > 0 else "DOWN"


def _parse_drivers_json(payload: str | None) -> dict:
    try:
        return json.loads(payload or "{}")
    except Exception:
        return {}


def _should_emit(prev, score: float, confidence: float, now: datetime, tf_minutes: int) -> bool:
    if not prev:
        return True
    try:
        prev_ts = datetime.fromisoformat(prev["ts_utc"].replace("Z", "+00:00"))
    except Exception:
        return True
    age_minutes = (now - prev_ts).total_seconds() / 60
    if age_minutes >= tf_minutes * 0.5:
        return True
    if prev["direction"] != _direction(score, 0.0):
        return True
    if abs((prev["confidence"] or 0) - confidence) >= 0.1:
        return True
    return False


def _apply_hysteresis(prev, score: float, direction: str, settings: Settings, tf: str, major_event: bool) -> tuple[float, str]:
    if not prev:
        return score, direction
    try:
        prev_ts = datetime.fromisoformat(prev["ts_utc"].replace("Z", "+00:00"))
    except Exception:
        return score, direction
    hold_minutes = int(settings.min_hold_minutes.get(tf, 0))
    age_minutes = (_now() - prev_ts).total_seconds() / 60
    prev_drivers = _parse_drivers_json(_row_value(prev, "drivers_json"))
    prev_score = float(prev_drivers.get("raw_score", 0.0))
    hysteresis = float(settings.thresholds.get("flip_hysteresis", 0.12))
    if direction != prev["direction"]:
        if age_minutes < hold_minutes and not major_event:
            return prev_score, prev["direction"]
        if abs(score - prev_score) < hysteresis and not major_event:
            return prev_score, prev["direction"]
    return score, direction


def _row_value(row, key: str, default: str | None = None):
    try:
        return row[key]
    except Exception:
        return default


def _apply_calibration(model: tuple[float, float] | None, raw_score: float, base_confidence: float) -> float:
    if not model:
        return base_confidence
    a, b = model
    x = min(1.0, max(0.0, abs(raw_score)))
    calibrated = _sigmoid(a * x + b)
    return max(0.1, min(0.95, calibrated))


def _sigmoid(z: float) -> float:
    return 1.0 / (1.0 + math.exp(-z))


def _calibration_models(db: DB, lookback_days: int = 7) -> dict[str, tuple[float, float]]:
    cutoff = _iso(_now() - timedelta(days=lookback_days))
    models: dict[str, tuple[float, float]] = {}
    for tf in TF_MINUTES:
        xs, ys = _fetch_calibration_pairs(db, tf, cutoff)
        model = _fit_platt(xs, ys)
        if model:
            models[tf] = model
    return models


def _fetch_calibration_pairs(db: DB, tf: str, cutoff: str) -> tuple[list[float], list[int]]:
    rows = db.fetchall(
        """
        SELECT f.drivers_json, s.hit
        FROM forecasts f JOIN forecast_scores s ON f.forecast_id = s.forecast_id
        WHERE f.tf = ? AND s.scored_at_utc >= ?
        """,
        (tf, cutoff),
    )
    xs: list[float] = []
    ys: list[int] = []
    for row in rows:
        try:
            drivers = json.loads(row["drivers_json"] or "{}")
        except Exception:
            drivers = {}
        raw = drivers.get("raw_score")
        if raw is None:
            continue
        try:
            x = abs(float(raw))
        except Exception:
            continue
        xs.append(min(1.0, max(0.0, x)))
        ys.append(int(row["hit"] or 0))
    return xs, ys


def _fit_platt(xs: list[float], ys: list[int], max_iter: int = 200, lr: float = 0.4, l2: float = 0.01) -> tuple[float, float] | None:
    if len(xs) < 20:
        return None
    a, b = 1.0, 0.0
    n = len(xs)
    for _ in range(max_iter):
        grad_a = 0.0
        grad_b = 0.0
        for x, y in zip(xs, ys):
            p = _sigmoid(a * x + b)
            grad_a += (p - y) * x
            grad_b += (p - y)
        grad_a = grad_a / n + l2 * a
        grad_b = grad_b / n
        a -= lr * grad_a
        b -= lr * grad_b
    return a, b


def generate_forecasts(
    db: DB,
    settings: Settings,
    market: MarketSnapshot,
    flow: FlowPanel,
    risk: RiskPanel,
) -> list[dict]:
    clusters = load_clusters(db, settings)
    cluster_ids = [c.cluster_id for c in clusters]
    now = _now()
    weights_by_tf = _adaptive_weights(db, settings)
    cal_models = _calibration_models(db, lookback_days=7)
    outputs = []
    for tf, tf_minutes in TF_MINUTES.items():
        weights = weights_by_tf.get(tf) or {"market": settings.weights.get("market", 0.6), "news": settings.weights.get("news", 0.4)}
        impact_by_target = load_event_impacts(db, cluster_ids, tf)
        for target in TARGETS:
            market_score, market_features, market_reasons = _market_signal_details(market, flow, risk, target)
            overrides = impact_by_target.get(target, {})
            news_score, top_clusters, contribs = _aggregate_news_signal(clusters, settings, target, risk, overrides)
            raw_score = weights["market"] * market_score + weights["news"] * news_score
            direction = _direction(raw_score, float(settings.thresholds.get("neutral_band_pct", 0.0015)))
            major_event = any(c.impact >= 70 for c in top_clusters)

            prev = db.fetchone(
                "SELECT * FROM forecasts WHERE tf = ? AND target = ? ORDER BY ts_utc DESC LIMIT 1",
                (tf, target),
            )
            raw_score, direction = _apply_hysteresis(prev, raw_score, direction, settings, tf, major_event)

            base_conf = _confidence_from_score(raw_score, float(settings.thresholds.get("min_confidence", 0.35)))
            confidence = _apply_calibration(cal_models.get(tf), raw_score, base_conf)

            feature_contrib = []
            contrib_sum = 0.0
            for f in market_features:
                effective_weight = f["weight"] * weights["market"]
                contribution = f["contribution"] * weights["market"]
                feature_contrib.append(
                    {
                        "name": f["name"],
                        "value": f["value"],
                        "weight": round(effective_weight, 4),
                        "contribution": round(contribution, 4),
                    }
                )
                contrib_sum += contribution
            news_contrib = []
            for c in contribs:
                contribution = c.get("contrib", 0.0) * weights["news"]
                news_contrib.append(
                    {
                        "cluster_id": c.get("cluster_id"),
                        "headline": c.get("headline"),
                        "contribution": round(contribution, 4),
                    }
                )
                contrib_sum += contribution
            rank_reason = list(dict.fromkeys(market_reasons + [f"news:{t}" for t in _top_news_tags(top_clusters)]))

            drivers = {
                "raw_score": round(raw_score, 4),
                "market_score": round(market_score, 4),
                "news_score": round(news_score, 4),
                "weights": weights,
                "feature_contrib": feature_contrib,
                "news_contrib": news_contrib,
                "rank_reason": rank_reason[:8],
                "contrib_sum": round(contrib_sum, 4),
                "evidence": _build_evidence(market, flow, news_score, top_clusters, contribs),
                "clusters": [
                    {
                        "cluster_id": c.cluster_id,
                        "headline": c.headline,
                        "impact": round(c.impact, 4),
                        "credibility": round(c.credibility, 3),
                        "source_tier": c.source_tier,
                        "direction": c.direction,
                        "tags": c.tags[:3],
                        "scope": _extract_scope(c.tags),
                        "sectors": _extract_sectors(c.tags, max_items=3),
                        "realized_ret": (overrides.get(c.cluster_id) or {}).get("realized_ret"),
                        "realized_z": (overrides.get(c.cluster_id) or {}).get("realized_z"),
                    }
                    for c in top_clusters
                ],
            }

            if not _should_emit(prev, raw_score, confidence, now, tf_minutes):
                continue

            forecast_id = hashlib_sha1(f"{tf}:{target}:{_iso(now)}")
            expires_at = now + timedelta(minutes=tf_minutes)
            rationale = _build_rationale(raw_score, news_score, top_clusters)
            payload = (
                forecast_id,
                _iso(now),
                tf,
                target,
                direction,
                None,
                confidence,
                json.dumps(drivers),
                rationale,
                "v1",
                "system",
                _iso(expires_at),
            )
            db.execute(
                """
                INSERT INTO forecasts (
                    forecast_id, ts_utc, tf, target, direction, expected_move,
                    confidence, drivers_json, rationale_text, model_version,
                    created_by, expires_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            outputs.append(
                {
                    "forecast_id": forecast_id,
                    "ts_utc": _iso(now),
                    "tf": tf,
                    "target": target,
                    "direction": direction,
                    "confidence": confidence,
                    "expires_at_utc": _iso(expires_at),
                    "rationale_text": rationale,
                    "drivers_json": drivers,
                }
            )
    return outputs


def _adaptive_weights(db: DB, settings: Settings) -> dict[str, dict[str, float]]:
    cutoff = _iso(_now() - timedelta(days=7))
    output: dict[str, dict[str, float]] = {}
    base_market = float(settings.weights.get("market", 0.6))
    base_news = float(settings.weights.get("news", 0.4))

    for tf in TF_MINUTES:
        row = db.fetchone(
            """
            SELECT AVG(s.brier_component) as avg_brier, AVG(s.hit) as avg_hit
            FROM forecasts f JOIN forecast_scores s ON f.forecast_id = s.forecast_id
            WHERE f.tf = ? AND s.scored_at_utc >= ?
            """,
            (tf, cutoff),
        )
        avg_brier = float(row["avg_brier"] or 0.0) if row else 0.0
        avg_hit = float(row["avg_hit"] or 0.0) if row else 0.0

        news_w = base_news
        if avg_brier >= 0.30 or avg_hit <= 0.45:
            news_w *= 0.75
        elif avg_brier > 0 and avg_brier <= 0.18 and avg_hit >= 0.55:
            news_w *= 1.2

        market_w = base_market
        total = max(0.01, market_w + news_w)
        output[tf] = {
            "market": round(market_w / total, 4),
            "news": round(news_w / total, 4),
        }
    return output


def _build_evidence(
    market: MarketSnapshot,
    flow: FlowPanel,
    news_score: float,
    clusters: list[ClusterImpact],
    contribs: list[dict],
) -> list[str]:
    evidence = []
    deltas = (market.coingecko.deltas or {}) if market and market.coingecko else {}
    btc_d = deltas.get("btc_d")
    usdt_d = deltas.get("usdt_d")
    usdc_d = deltas.get("usdc_d")
    total_vol = deltas.get("total_vol")
    if btc_d is not None:
        evidence.append(f"BTC.D delta: {btc_d:.2f}")
    if usdt_d is not None and usdc_d is not None:
        evidence.append(f"Stable dom delta: {(usdt_d + usdc_d):.2f}")
    if total_vol is not None:
        evidence.append(f"Total vol delta: {total_vol:,.0f}")
    if flow:
        evidence.append(f"Flow score: {flow.flow_score}")
    evidence.append(f"News impact score: {news_score:.2f}")
    if any(c.get("realized_z") is not None or c.get("realized_ret") is not None for c in contribs):
        evidence.append("Event-study confirms move in post window.")
    if contribs:
        for c in contribs[:3]:
            sign = "+" if c.get("contrib", 0) >= 0 else ""
            dir_label = "NEUTRAL" if c.get("neutral") else ("UP" if c.get("direction", 0) > 0 else "DOWN")
            realized_note = ""
            if c.get("realized_z") is not None:
                realized_note = f" rz {c.get('realized_z'):.2f}"
            elif c.get("realized_ret") is not None:
                realized_note = f" r {c.get('realized_ret'):.2f}%"
            evidence.append(
                f"Cluster {sign}{c.get('contrib', 0):.3f} ({dir_label}, rel {c.get('relevance', 0):.2f}, "
                f"imp {c.get('impact', 0):.2f}, cred {c.get('credibility', 0):.2f}{realized_note}): {str(c.get('headline', ''))[:80]}"
            )
    elif clusters:
        evidence.append(f"Top cluster: {clusters[0].headline[:80]}")
    return evidence[:6]


def _top_news_tags(clusters: list[ClusterImpact]) -> list[str]:
    tags: list[str] = []
    for c in clusters:
        for t in c.tags or []:
            if t not in tags:
                tags.append(t)
        if len(tags) >= 4:
            break
    mapped = []
    for t in tags:
        if t == "War":
            mapped.append("geopolitics")
        elif t == "Energy":
            mapped.append("energy_shock")
        elif t == "Reg":
            mapped.append("regulation")
        elif t == "ETF":
            mapped.append("etf_flow")
        elif t == "Stablecoin":
            mapped.append("stablecoin_flow")
        elif t == "Defense":
            mapped.append("defense_risk")
        elif t == "DataCenter":
            mapped.append("ai_capex")
        elif t == "CEO":
            mapped.append("exec_guidance")
        else:
            mapped.append(t.lower())
    return mapped


def _build_rationale(score: float, news_score: float, clusters: list[ClusterImpact]) -> str:
    if abs(score) < 0.05:
        return "Sinyaller dengeli, yön için net teyit yok."
    tone = "pozitif" if score > 0 else "negatif"
    if clusters:
        return f"Haber etkisi {tone} tarafa baskin, en guclu baslik: {clusters[0].headline[:120]}."
    return f"Piyasa sinyalleri {tone} tarafa kayik."


def score_expired_forecasts(db: DB, settings: Settings) -> int:
    now = _iso(_now())
    rows = db.fetchall(
        """
        SELECT f.forecast_id, f.ts_utc, f.expires_at_utc, f.direction, f.confidence, f.tf
        FROM forecasts f
        LEFT JOIN forecast_scores s ON f.forecast_id = s.forecast_id
        WHERE s.forecast_id IS NULL AND f.expires_at_utc <= ?
        """,
        (now,),
    )
    scored = 0
    for row in rows:
        start = _price_at(db, "BTC", row["ts_utc"])
        end = _price_at(db, "BTC", row["expires_at_utc"])
        if start is None or end is None or start == 0:
            continue
        ret = (end - start) / start
        neutral_band = float(settings.thresholds.get("neutral_band_pct", 0.0015))
        direction = row["direction"]
        if direction == "NEUTRAL":
            hit = abs(ret) <= neutral_band
        else:
            hit = (ret > neutral_band and direction == "UP") or (ret < -neutral_band and direction == "DOWN")
        p = float(row["confidence"] or 0.5)
        brier = (p - (1.0 if hit else 0.0)) ** 2
        db.execute(
            """
            INSERT INTO forecast_scores (forecast_id, realized_return, hit, brier_component, scored_at_utc)
            VALUES (?, ?, ?, ?, ?)
            """,
            (row["forecast_id"], ret, 1 if hit else 0, brier, now),
        )
        scored += 1
    return scored


def _price_at(db: DB, asset: str, ts_utc: str) -> float | None:
    row = db.fetchone(
        """
        SELECT close FROM price_bars
        WHERE asset = ? AND ts_utc <= ?
        ORDER BY ts_utc DESC LIMIT 1
        """,
        (asset, ts_utc),
    )
    if not row:
        return None
    return float(row["close"] or 0.0)


def compute_metrics(db: DB) -> list[dict]:
    metrics = []
    now = _now()
    for tf, tf_minutes in TF_MINUTES.items():
        cutoff_24h = _iso(now - timedelta(hours=24))
        cutoff_7d = _iso(now - timedelta(days=7))
        rows_24 = db.fetchall(
            """
            SELECT s.hit, s.brier_component
            FROM forecasts f JOIN forecast_scores s ON f.forecast_id = s.forecast_id
            WHERE f.tf = ? AND s.scored_at_utc >= ?
            """,
            (tf, cutoff_24h),
        )
        rows_7d = db.fetchall(
            """
            SELECT s.hit, s.brier_component
            FROM forecasts f JOIN forecast_scores s ON f.forecast_id = s.forecast_id
            WHERE f.tf = ? AND s.scored_at_utc >= ?
            """,
            (tf, cutoff_7d),
        )
        hit_24 = _avg([int(r["hit"]) for r in rows_24])
        hit_7d = _avg([int(r["hit"]) for r in rows_7d])
        brier_24 = _avg([float(r["brier_component"]) for r in rows_24])
        brier_7d = _avg([float(r["brier_component"]) for r in rows_7d])
        flips = _flip_rate(db, tf, cutoff_7d)
        coverage = _coverage_rate(db, tf, cutoff_24h, tf_minutes)
        calibration_error, reliability_bins = _calibration_stats(db, tf, cutoff_7d)
        metrics.append(
            {
                "tf": tf,
                "hit_rate_24h": hit_24,
                "hit_rate_7d": hit_7d,
                "brier_24h": brier_24,
                "brier_7d": brier_7d,
                "flip_rate_7d": flips,
                "coverage_24h": coverage,
                "calibration_error_7d": calibration_error,
                "reliability_bins_7d": reliability_bins,
            }
        )
    return metrics


def _avg(vals: list[float]) -> float:
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


def _calibration_stats(db: DB, tf: str, cutoff: str, bins_count: int = 5) -> tuple[float, list[dict]]:
    rows = db.fetchall(
        """
        SELECT f.confidence, s.hit
        FROM forecasts f JOIN forecast_scores s ON f.forecast_id = s.forecast_id
        WHERE f.tf = ? AND s.scored_at_utc >= ?
        """,
        (tf, cutoff),
    )
    if not rows:
        return 0.0, []
    bins = [{"count": 0, "sum_pred": 0.0, "sum_obs": 0.0} for _ in range(bins_count)]
    for row in rows:
        try:
            conf = float(row["confidence"] or 0.0)
        except Exception:
            conf = 0.0
        idx = min(bins_count - 1, int(conf * bins_count))
        bins[idx]["count"] += 1
        bins[idx]["sum_pred"] += conf
        bins[idx]["sum_obs"] += int(row["hit"] or 0)
    total = sum(b["count"] for b in bins)
    if total == 0:
        return 0.0, []
    out_bins = []
    weighted_err = 0.0
    for idx, b in enumerate(bins):
        if b["count"] == 0:
            out_bins.append({"bin": idx, "count": 0, "avg_pred": 0.0, "avg_obs": 0.0})
            continue
        avg_pred = b["sum_pred"] / b["count"]
        avg_obs = b["sum_obs"] / b["count"]
        out_bins.append({"bin": idx, "count": b["count"], "avg_pred": round(avg_pred, 4), "avg_obs": round(avg_obs, 4)})
        weighted_err += abs(avg_pred - avg_obs) * b["count"]
    calibration_error = weighted_err / total
    return round(calibration_error, 4), out_bins


def _flip_rate(db: DB, tf: str, cutoff: str) -> float:
    rows = db.fetchall(
        "SELECT direction FROM forecasts WHERE tf = ? AND ts_utc >= ? ORDER BY ts_utc ASC",
        (tf, cutoff),
    )
    if len(rows) < 2:
        return 0.0
    flips = 0
    prev = rows[0]["direction"]
    for row in rows[1:]:
        if row["direction"] != prev:
            flips += 1
        prev = row["direction"]
    return flips / max(1, len(rows) - 1)


def _coverage_rate(db: DB, tf: str, cutoff: str, tf_minutes: int) -> float:
    rows = db.fetchall(
        "SELECT COUNT(1) as cnt FROM forecasts WHERE tf = ? AND ts_utc >= ?",
        (tf, cutoff),
    )
    count = int(rows[0]["cnt"]) if rows else 0
    expected = max(1, int((24 * 60) / tf_minutes))
    return min(1.0, count / expected)


def hashlib_sha1(value: str) -> str:
    import hashlib

    return hashlib.sha1(value.encode("utf-8")).hexdigest()
