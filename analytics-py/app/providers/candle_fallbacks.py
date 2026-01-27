from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd

from app.infra.http import get_json


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame()


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return _empty_df()
    df = df.copy()
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    return df.sort_index()


def fetch_finnhub_candles(
    symbol: str,
    interval: str = "15m",
    timeout: float = 10.0,
    days: int = 30,
    crypto: bool | None = None,
) -> pd.DataFrame:
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        return _empty_df()
    resolution = "15" if interval == "15m" else "60" if interval == "1h" else "15"
    to_ts = int(time.time())
    from_ts = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    if crypto is None:
        crypto = ":" in symbol and symbol.split(":")[0] in {"BINANCE", "COINBASE", "KRAKEN"}
    base = "https://finnhub.io/api/v1/crypto/candle" if crypto else "https://finnhub.io/api/v1/stock/candle"
    params = {
        "symbol": symbol,
        "resolution": resolution,
        "from": from_ts,
        "to": to_ts,
        "token": api_key,
    }
    try:
        data = get_json(base, params=params, timeout=timeout, retries=1)
    except Exception:
        return _empty_df()
    if not data or data.get("s") != "ok":
        return _empty_df()
    ts = data.get("t") or []
    close = data.get("c") or []
    volume = data.get("v") or []
    if not ts or not close:
        return _empty_df()
    df = pd.DataFrame(
        {
            "Close": pd.Series(close, dtype="float"),
            "Volume": pd.Series(volume, dtype="float") if volume else pd.Series([0.0] * len(close)),
        },
        index=pd.to_datetime(ts, unit="s", utc=True),
    )
    return _normalize_df(df)


def fetch_twelvedata_candles(
    symbol: str,
    interval: str = "15m",
    timeout: float = 10.0,
    outputsize: int = 3000,
) -> pd.DataFrame:
    api_key = os.getenv("TWELVEDATA_API_KEY")
    if not api_key:
        return _empty_df()
    interval_map = {"15m": "15min", "1h": "1h"}
    td_interval = interval_map.get(interval, "15min")
    params = {
        "symbol": symbol,
        "interval": td_interval,
        "outputsize": outputsize,
        "apikey": api_key,
    }
    try:
        data = get_json("https://api.twelvedata.com/time_series", params=params, timeout=timeout, retries=1)
    except Exception:
        return _empty_df()
    values = data.get("values") or []
    if not values:
        return _empty_df()
    tz_name = (data.get("meta") or {}).get("timezone") or data.get("timezone")
    rows = []
    for row in values:
        dt_str = row.get("datetime")
        if not dt_str:
            continue
        rows.append(
            {
                "dt": dt_str,
                "Close": float(row.get("close") or 0.0),
                "Volume": float(row.get("volume") or 0.0),
            }
        )
    if not rows:
        return _empty_df()
    df = pd.DataFrame(rows)
    dt = pd.to_datetime(df["dt"])
    if tz_name:
        try:
            dt = dt.dt.tz_localize(tz_name).dt.tz_convert("UTC")
        except Exception:
            dt = dt.dt.tz_localize("UTC")
    else:
        dt = dt.dt.tz_localize("UTC")
    df = df.drop(columns=["dt"])
    df.index = dt
    return _normalize_df(df)
