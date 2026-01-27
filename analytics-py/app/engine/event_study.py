from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import hashlib
from typing import Dict, List

import numpy as np
import pandas as pd

from app.models import CombinedReaction, EventPoint, EventReactions, NewsItem, ReactionWindow


def _parse_ts(ts: str) -> datetime | None:
    try:
        if ts.endswith("Z"):
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _timeframe_seconds(timeframe: str) -> int:
    if not timeframe:
        return 3600
    value = timeframe.strip().lower()
    try:
        if value.endswith("m"):
            return int(value[:-1]) * 60
        if value.endswith("h"):
            return int(value[:-1]) * 3600
        if value.endswith("d"):
            return int(value[:-1]) * 86400
    except Exception:
        return 3600
    return 3600


def _normalize_candles(df: pd.DataFrame) -> pd.DataFrame:
    candles = df.copy().dropna()
    if candles.index.tz is None:
        candles.index = candles.index.tz_localize("UTC")
    else:
        candles.index = candles.index.tz_convert("UTC")
    return candles.sort_index()


def _volume_z_series(volume: pd.Series, window: int) -> pd.Series:
    mean = volume.rolling(window=window, min_periods=1).mean().shift(1)
    std = volume.rolling(window=window, min_periods=1).std().shift(1).replace(0, np.nan)
    return (volume - mean) / std


def _returns_series(close: pd.Series, bars: int) -> pd.Series:
    future = close.shift(-bars)
    return (future - close) / close * 100.0


def _returns_series_back(close: pd.Series, bars: int) -> pd.Series:
    past = close.shift(bars)
    return (close - past) / past * 100.0


def _spark_series(close: pd.Series, idx: int, bars: List[int]) -> List[float]:
    if idx < 0 or idx >= len(close):
        return []
    base = close.iloc[idx]
    if not base or np.isnan(base):
        return []
    out: List[float] = []
    for b in bars:
        j = idx + b
        if j < 0 or j >= len(close):
            out.append(0.0)
            continue
        out.append(float((close.iloc[j] - base) / base * 100.0))
    return out


def _align_to_15m(dt: datetime) -> datetime:
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)


def _to_tsi(dt: datetime) -> str:
    try:
        tsi = dt.astimezone(ZoneInfo("Europe/Istanbul"))
    except Exception:
        tsi = dt
    return tsi.isoformat()


def compute_event_study(
    news: List[NewsItem],
    candles: pd.DataFrame | Dict[str, pd.DataFrame],
    timeframe: str = "1h",
) -> List[EventPoint]:
    if candles is None:
        return []

    if isinstance(candles, dict):
        candles_map = {k: _normalize_candles(v) for k, v in candles.items() if v is not None and not v.empty}
    else:
        if candles.empty:
            return []
        candles_map = {"BTC": _normalize_candles(candles)}

    if not candles_map:
        return []

    asset_stats: dict[str, dict] = {}
    window_bars = {"15m": 1, "1h": 4, "4h": 16, "24h": 96}
    pre_bars = 3
    spark_pre_bars = [-4, -3, -2, -1]
    spark_post_bars = [1, 2, 3, 4]

    for asset, df in candles_map.items():
        close = df["Close"]
        volume = df["Volume"] if "Volume" in df else None
        vol_z = None
        if volume is not None and not volume.empty:
            vol_z = _volume_z_series(volume, window=max(24, min(120, len(volume))))
        ret_stats = {}
        for label, bars in window_bars.items():
            series = _returns_series(close, bars)
            mean = float(series.mean(skipna=True)) if not series.empty else 0.0
            std = float(series.std(skipna=True)) if not series.empty else 0.0
            ret_stats[label] = {"series": series, "mean": mean, "std": std}
        pre_series = _returns_series_back(close, pre_bars)
        pre_mean = float(pre_series.mean(skipna=True)) if not pre_series.empty else 0.0
        pre_std = float(pre_series.std(skipna=True)) if not pre_series.empty else 0.0
        around_series = (close.shift(-2) / close.shift(2) - 1.0) * 100.0
        coverage_days = (close.index[-1] - close.index[0]).total_seconds() / 86400.0 if len(close) > 1 else 0.0
        if coverage_days >= 30:
            cutoff = close.index[-1] - pd.Timedelta(days=30)
            around_window = around_series[around_series.index >= cutoff]
            sigma_30m = float(around_window.std(skipna=True)) if not around_window.empty else 0.0
            sigma_status = "ok"
        else:
            sigma_30m = float(around_series.std(skipna=True)) if not around_series.empty else 0.0
            sigma_status = "partial"
        if np.isnan(sigma_30m):
            sigma_30m = 0.0
        asset_stats[asset] = {
            "df": df,
            "close": close,
            "vol_z": vol_z,
            "ret_stats": ret_stats,
            "pre_series": pre_series,
            "pre_mean": pre_mean,
            "pre_std": pre_std,
            "around_series": around_series,
            "sigma_30m": sigma_30m,
            "sigma_status": sigma_status,
        }

    scored: list[tuple[float, str, EventPoint]] = []
    for item in news[:40]:
        if item.score < 6:
            continue
        ts = item.publishedAtISO
        if not ts:
            continue
        dt = _parse_ts(ts)
        if not dt:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        aligned = _align_to_15m(dt)
        ts_iso = dt.isoformat().replace("+00:00", "Z")

        reactions: dict[str, EventReactions] = {}
        btc_move = 0.0
        btc_vol_z = 0.0
        btc_pre_post = 1.0
        max_abs_z = 0.0

        for asset, stats in asset_stats.items():
            df = stats["df"]
            idx = df.index.get_indexer([aligned], method="pad")
            if len(idx) == 0 or idx[0] < 0:
                continue
            i = idx[0]
            nearest_ts = df.index[i]
            if abs((nearest_ts - aligned).total_seconds()) > 6 * 3600:
                continue
            close = stats["close"]
            if i >= len(close) or i < 0:
                continue
            base = close.iloc[i]
            if not base or np.isnan(base):
                continue

            pre_ret = 0.0
            pre_z = 0.0
            if i - 4 >= 0:
                pre_ret = float((close.iloc[i - 1] - close.iloc[i - 4]) / close.iloc[i - 4] * 100.0)
                std = stats["pre_std"] or 0.0
                if std > 0:
                    pre_z = float((stats["pre_series"].iloc[i - 1] - stats["pre_mean"]) / std)

            post = {}
            for label, bars in window_bars.items():
                if i + bars >= len(close):
                    ret = 0.0
                    z = 0.0
                else:
                    ret = float((close.iloc[i + bars] - base) / base * 100.0)
                    stat = stats["ret_stats"][label]
                    std = stat["std"] or 0.0
                    z = float((stat["series"].iloc[i] - stat["mean"]) / std) if std > 0 else 0.0
                post[label] = ReactionWindow(ret=ret, z=z)
                max_abs_z = max(max_abs_z, abs(z))

            around: Dict[str, ReactionWindow] = {}
            pre30_ret = None
            post30_ret = None
            data_status = "ok"
            missing_fields: List[str] = []
            if i - 2 < 0 or i + 2 >= len(close):
                around_ret = None
                around_z = None
                data_status = "missing"
                missing_fields.append(f"reactions.{asset}.around.30m")
            else:
                around_ret = float((close.iloc[i + 2] - close.iloc[i - 2]) / close.iloc[i - 2] * 100.0)
                pre30_ret = float((close.iloc[i] - close.iloc[i - 2]) / close.iloc[i - 2] * 100.0)
                post30_ret = float((close.iloc[i + 2] - close.iloc[i]) / close.iloc[i] * 100.0)
                sigma_30m = stats.get("sigma_30m") or 0.0
                sigma_status = stats.get("sigma_status", "ok")
                if sigma_30m and sigma_30m > 0:
                    around_z = float(around_ret / sigma_30m)
                else:
                    around_z = None
                if sigma_status != "ok":
                    data_status = "partial"
                    missing_fields.append(f"reactions.{asset}.sigma_30m_30d")
                if around_z is None:
                    data_status = "partial"
                    missing_fields.append(f"reactions.{asset}.sigma_30m")
            around["30m"] = ReactionWindow(ret=around_ret, z=around_z)

            volume_z = 0.0
            if stats["vol_z"] is not None and i < len(stats["vol_z"]):
                v = stats["vol_z"].iloc[i]
                volume_z = float(v) if not np.isnan(v) else 0.0

            spark_pre = _spark_series(close, i, spark_pre_bars)
            spark_post = _spark_series(close, i, spark_post_bars)

            reactions[asset] = EventReactions(
                pre=ReactionWindow(ret=pre_ret, z=pre_z),
                post={k: v for k, v in post.items()},
                around={k: v for k, v in around.items()},
                pre_30m_ret=pre30_ret,
                post_30m_ret=post30_ret,
                volume_z=volume_z,
                spark_pre=spark_pre,
                spark_post=spark_post,
                data_status=data_status,
                missing_fields=missing_fields,
            )

            if asset == "BTC":
                btc_vol_z = volume_z
                btc_move = post["15m"].ret if "15m" in post else 0.0
                pre_slice = [p for p in spark_pre if p is not None]
                post_slice = [p for p in spark_post if p is not None]
                pre_avg = np.mean(pre_slice) if pre_slice else 0.0
                post_avg = np.mean(post_slice) if post_slice else 0.0
                eps = 1e-9
                btc_pre_post = float((post_avg + eps) / (pre_avg + eps)) if pre_avg != 0 else 1.0

        if not reactions:
            continue

        btc_key = "BTC" if "BTC" in reactions else None
        nq_key = "NQ" if "NQ" in reactions else ("QQQ" if "QQQ" in reactions else ("NDX" if "NDX" in reactions else None))
        btc_z = 0.0
        nq_z = 0.0
        if btc_key:
            btc_z = reactions[btc_key].post.get("1h", ReactionWindow()).z if reactions[btc_key].post else 0.0
            if btc_z is None:
                btc_z = 0.0
        if nq_key:
            nq_z = reactions[nq_key].post.get("1h", ReactionWindow()).z if reactions[nq_key].post else 0.0
            if nq_z is None:
                nq_z = 0.0

        threshold = 0.4
        mode = "LOW_SIGNAL"
        if abs(btc_z) >= threshold and abs(nq_z) >= threshold:
            if btc_z > 0 and nq_z > 0:
                mode = "BOTH_UP"
            elif btc_z < 0 and nq_z < 0:
                mode = "BOTH_DOWN"
            else:
                mode = "DIVERGENCE"

        combined = CombinedReaction(mode=mode, severity=float(max(abs(btc_z), abs(nq_z), max_abs_z)))

        scope = getattr(item, "news_scope", None)
        sectors = []
        for s in getattr(item, "sector_impacts", []) or []:
            if isinstance(s, dict) and s.get("sector"):
                sectors.append(s["sector"])
        sectors = sectors[:3]

        raw_key = f"{item.title}|{ts_iso}|{timeframe}"
        key = hashlib.md5(raw_key.encode("utf-8")).hexdigest()[:10]
        rank = (1.2 * abs(btc_vol_z)) + (0.6 * abs(max_abs_z)) + (item.score / 10.0)
        point = EventPoint(
            id=f"evt_{key}",
            title=item.title,
            tsISO=ts_iso,
            timeframe=timeframe,
            volume_z=btc_vol_z,
            pre_post_ratio=btc_pre_post,
            price_move_pct=btc_move,
            event_id=f"evt_{key}",
            headline=item.title,
            published_at_utc=ts_iso,
            published_at_tsi=_to_tsi(dt),
            scope=scope,
            sectors=sectors,
            reactions=reactions,
            combined=combined,
        )
        scored.append((rank, point.id, point))

    scored.sort(key=lambda x: (-x[0], x[1]))
    return [p for _, _, p in scored[:12]]
