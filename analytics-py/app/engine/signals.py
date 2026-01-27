from __future__ import annotations

import math
import numpy as np
import pandas as pd


def compute_rsi(series: pd.Series, period: int = 14) -> float:
    if series is None or series.empty:
        return 0.0
    delta = series.diff().dropna()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    roll_up = up.rolling(window=period).mean()
    roll_down = down.rolling(window=period).mean()
    rs = roll_up / roll_down.replace({0: np.nan})
    rsi = 100.0 - (100.0 / (1.0 + rs))
    last = rsi.dropna()
    if last.empty:
        return 0.0
    return float(last.iloc[-1])


def compute_volume_anomaly(volume: pd.Series, window: int = 20) -> bool:
    if volume is None or volume.empty or len(volume) < window:
        return False
    sma = volume.rolling(window=window).mean().iloc[-1]
    last = volume.iloc[-1]
    if sma == 0:
        return False
    return last > 2.0 * sma


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _scale(value: float, src_min: float, src_max: float) -> float:
    if src_max == src_min:
        return 0.0
    norm = (value - src_min) / (src_max - src_min)
    return _clamp(norm, 0.0, 1.0) * 100.0


def compute_fear_greed(rsi: float, funding_z: float, oi_delta: float) -> float | None:
    try:
        rsi_val = float(rsi)
    except (TypeError, ValueError):
        rsi_val = 0.0
    try:
        funding_val = float(funding_z)
    except (TypeError, ValueError):
        funding_val = 0.0
    try:
        oi_val = float(oi_delta)
    except (TypeError, ValueError):
        oi_val = 0.0

    if not math.isfinite(rsi_val):
        rsi_val = 0.0
    if not math.isfinite(funding_val):
        funding_val = 0.0
    if not math.isfinite(oi_val):
        oi_val = 0.0

    if rsi_val <= 0 and funding_val == 0.0 and oi_val == 0.0:
        return None
    rsi_score = _clamp(rsi_val, 0.0, 100.0)
    funding_score = _scale(_clamp(funding_val, -3.0, 3.0), -3.0, 3.0)
    oi_score = _scale(_clamp(oi_val, -10.0, 10.0), -10.0, 10.0)
    score = (0.5 * rsi_score) + (0.3 * funding_score) + (0.2 * oi_score)
    score = _clamp(score, 0.0, 100.0)
    return round(float(score), 1)


def build_risk_flags(yahoo_snapshot: dict, derivatives_result, btc_df: pd.DataFrame):
    flags = []
    deriv_ok = False
    deriv_data = {}
    if derivatives_result is not None:
        if hasattr(derivatives_result, "ok"):
            deriv_ok = bool(getattr(derivatives_result, "ok"))
            deriv_data = getattr(derivatives_result, "data", {}) or {}
        elif isinstance(derivatives_result, dict):
            deriv_ok = bool(derivatives_result.get("ok", True))
            deriv_data = derivatives_result.get("data") or derivatives_result
    if not deriv_ok:
        flags.append("DATA_MISSING_DERIVATIVES")

    oil_chg = yahoo_snapshot.get("oil_chg_24h", 0.0)
    qqq_chg = yahoo_snapshot.get("qqq_chg_24h", 0.0)
    if oil_chg >= 3.0 and qqq_chg <= -1.0:
        flags.append("MACRO_RISK_OFF")

    rsi = compute_rsi(btc_df["Close"]) if "Close" in btc_df else 0.0
    computed = deriv_data.get("computed") or {}
    funding_rate = (deriv_data.get("funding") or {}).get("latest", 0.0)
    funding_z = float(computed.get("funding_z", 0.0))
    if funding_z == 0.0 and funding_rate:
        funding_z = funding_rate / 0.01
    if rsi > 75 and funding_z > 1.5:
        flags.append("LEVERAGE_HEAT")

    oi_delta = float(computed.get("oi_delta_pct", 0.0))
    if oi_delta > 5.0:
        flags.append("LEVERAGE_BUILD")

    if compute_volume_anomaly(btc_df.get("Volume")):
        flags.append("VOLUME_ANOMALY")

    fear_greed = compute_fear_greed(rsi, funding_z, oi_delta)

    return flags, rsi, funding_z, oi_delta, fear_greed
