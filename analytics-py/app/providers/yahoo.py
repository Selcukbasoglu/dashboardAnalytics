from __future__ import annotations

import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from app.infra.http import get_json
from app.providers.base import ProviderResult


def _safe_float(v: Any) -> float:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def _pct_change(last: float, prev: float) -> float:
    if prev == 0:
        return 0.0
    return (last - prev) / prev * 100.0


def _fetch_chart(symbol: str, range_: str, interval: str, timeout: float):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "range": range_,
        "interval": interval,
        "includePrePost": "false",
        "events": "div,splits",
    }
    headers = {"User-Agent": "Mozilla/5.0"}
    data = get_json(url, params=params, headers=headers, timeout=min(timeout, 3.5), retries=0)
    result = (data.get("chart") or {}).get("result") or []
    if not result:
        return None
    node = result[0]
    timestamps = node.get("timestamp") or []
    quote = ((node.get("indicators") or {}).get("quote") or [{}])[0]
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []

    length = min(len(timestamps), len(opens), len(highs), len(lows), len(closes), len(volumes))
    if length == 0:
        return None

    rows = []
    for i in range(length):
        if closes[i] is None:
            continue
        ts = datetime.fromtimestamp(timestamps[i], tz=timezone.utc)
        rows.append((ts, opens[i], highs[i], lows[i], closes[i], volumes[i] or 0))

    if not rows:
        return None

    df = pd.DataFrame(rows, columns=["Datetime", "Open", "High", "Low", "Close", "Volume"]).set_index("Datetime")
    return {"df": df}


def get_yahoo_snapshot(timeout: float = 10.0):
    out = {
        "dxy": 0.0,
        "qqq": 0.0,
        "nasdaq": 0.0,
        "ftse": 0.0,
        "eurostoxx": 0.0,
        "oil": 0.0,
        "gold": 0.0,
        "silver": 0.0,
        "copper": 0.0,
        "bist": 0.0,
        "vix": 0.0,
        "btc": 0.0,
        "eth": 0.0,
        "btc_chg_24h": 0.0,
        "eth_chg_24h": 0.0,
        "dxy_chg_24h": 0.0,
        "qqq_chg_24h": 0.0,
        "nasdaq_chg_24h": 0.0,
        "ftse_chg_24h": 0.0,
        "eurostoxx_chg_24h": 0.0,
        "oil_chg_24h": 0.0,
        "gold_chg_24h": 0.0,
        "silver_chg_24h": 0.0,
        "copper_chg_24h": 0.0,
        "bist_chg_24h": 0.0,
    }

    mapping = {
        "DX-Y.NYB": ("dxy", "dxy_chg_24h"),
        "QQQ": ("qqq", "qqq_chg_24h"),
        "^IXIC": ("nasdaq", "nasdaq_chg_24h"),
        "^FTSE": ("ftse", "ftse_chg_24h"),
        "^STOXX50E": ("eurostoxx", "eurostoxx_chg_24h"),
        "CL=F": ("oil", "oil_chg_24h"),
        "GC=F": ("gold", "gold_chg_24h"),
        "SI=F": ("silver", "silver_chg_24h"),
        "HG=F": ("copper", "copper_chg_24h"),
        "XU100.IS": ("bist", "bist_chg_24h"),
        "^VIX": ("vix", None),
        "BTC-USD": ("btc", "btc_chg_24h"),
        "ETH-USD": ("eth", "eth_chg_24h"),
    }

    with ThreadPoolExecutor(max_workers=6) as exe:
        futures = {
            exe.submit(_fetch_chart, symbol, "5d", "1h", timeout): (symbol, key, chg_key)
            for symbol, (key, chg_key) in mapping.items()
        }
        for fut in as_completed(futures):
            symbol, key, chg_key = futures[fut]
            try:
                result = fut.result()
                if not result:
                    continue
                closes = result["df"]["Close"].dropna()
                if closes.empty:
                    continue
                last = closes.iloc[-1]
                out[key] = _safe_float(last)
                if chg_key:
                    prev = closes.iloc[-24] if len(closes) >= 24 else closes.iloc[0]
                    out[chg_key] = _pct_change(_safe_float(last), _safe_float(prev))
            except Exception:
                continue

    if out["eurostoxx"] == 0.0:
        fallback = _fetch_chart("^STOXX", "5d", "1h", timeout)
        if fallback:
            closes = fallback["df"]["Close"].dropna()
            if not closes.empty:
                last = closes.iloc[-1]
                out["eurostoxx"] = _safe_float(last)
                prev = closes.iloc[-24] if len(closes) >= 24 else closes.iloc[0]
                out["eurostoxx_chg_24h"] = _pct_change(_safe_float(last), _safe_float(prev))

    return out


def resample_candles(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    rule_map = {
        "1h": "1H",
        "3h": "3H",
        "6h": "6H",
    }
    rule = rule_map.get(interval)
    if not rule or df.empty:
        return df
    agg = {
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum",
    }
    return df.resample(rule).agg(agg).dropna()


PRIMARY_CANDLE_SYMBOLS = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "QQQ": "QQQ",
    "NASDAQ": "^IXIC",
    "FTSE": "^FTSE",
    "EUROSTOXX": "^STOXX50E",
    "OIL": "CL=F",
    "GOLD": "GC=F",
    "SILVER": "SI=F",
    "COPPER": "HG=F",
    "BIST": "XU100.IS",
    "DXY": "DX-Y.NYB",
    "AAPL": "AAPL",
    "MSFT": "MSFT",
    "AMZN": "AMZN",
    "GOOGL": "GOOGL",
    "META": "META",
    "NVDA": "NVDA",
    "TSLA": "TSLA",
    "MSTR": "MSTR",
    "COIN": "COIN",
    "XOM": "XOM",
    "CVX": "CVX",
    "COP": "COP",
    "OXY": "OXY",
    "SLB": "SLB",
    "EOG": "EOG",
    "MPC": "MPC",
    "PSX": "PSX",
    "VLO": "VLO",
    "ASML.AS": "ASML.AS",
    "SAP.DE": "SAP.DE",
    "005930.KS": "005930.KS",
    "6758.T": "6758.T",
    "SHOP.TO": "SHOP.TO",
    "ADYEN.AS": "ADYEN.AS",
    "NOKIA.HE": "NOKIA.HE",
    "0700.HK": "0700.HK",
    "9988.HK": "9988.HK",
    "SHEL": "SHEL",
    "TTE": "TTE",
    "BP": "BP",
    "EQNR": "EQNR",
    "PBR": "PBR",
    "ENB": "ENB",
    "SU.TO": "SU.TO",
    "CNQ.TO": "CNQ.TO",
    "REP.MC": "REP.MC",
    "JPM": "JPM",
    "BAC": "BAC",
    "WFC": "WFC",
    "C": "C",
    "GS": "GS",
    "MS": "MS",
    "BLK": "BLK",
    "SCHW": "SCHW",
    "AXP": "AXP",
    "HSBA.L": "HSBA.L",
    "UBSG.SW": "UBSG.SW",
    "BNP.PA": "BNP.PA",
    "DBK.DE": "DBK.DE",
    "INGA.AS": "INGA.AS",
    "8058.T": "8058.T",
    "SAN.MC": "SAN.MC",
    "BARC.L": "BARC.L",
    "ZURN.SW": "ZURN.SW",
    "CAT": "CAT",
    "DE": "DE",
    "BA": "BA",
    "GE": "GE",
    "HON": "HON",
    "UNP": "UNP",
    "UPS": "UPS",
    "LMT": "LMT",
    "RTX": "RTX",
    "SIE.DE": "SIE.DE",
    "AIR.PA": "AIR.PA",
    "DPW.DE": "DHL.DE",
    "VOLV-B.ST": "VOLV-B.ST",
    "7203.T": "7203.T",
    "7267.T": "7267.T",
    "CP.TO": "CP.TO",
    "6501.T": "6501.T",
    "SGRO.L": "SGRO.L",
    "LIN": "LIN",
    "APD": "APD",
    "SHW": "SHW",
    "ECL": "ECL",
    "DD": "DD",
    "DOW": "DOW",
    "NUE": "NUE",
    "FCX": "FCX",
    "NEM": "NEM",
    "BHP.AX": "BHP.AX",
    "RIO.AX": "RIO.AX",
    "GLEN.L": "GLEN.L",
    "ANTO.L": "ANTO.L",
    "BAS.DE": "BAS.DE",
    "SIKA.SW": "SIKA.SW",
    "AEM.TO": "AEM.TO",
    "NTR.TO": "NTR.TO",
    "IVN.AX": "IVN.AX",
    "NOC": "NOC",
    "GD": "GD",
    "LHX": "LHX",
    "HII": "HII",
    "TDG": "TDG",
    "AVAV": "AVAV",
    "KTOS": "KTOS",
    "BA.L": "BA.L",
    "RHM.DE": "RHM.DE",
    "HO.PA": "HO.PA",
    "LDO.MI": "LDO.MI",
    "SAAB-B.ST": "SAAB-B.ST",
    "SAF.PA": "SAF.PA",
    "HAG.DE": "HAG.DE",
    "AM.PA": "AM.PA",
    "ASELS.IS": "ASELS.IS",
    "OTKAR.IS": "OTKAR.IS",
    "SDTTR.IS": "SDTTR.IS",
    "ALTNY.IS": "ALTNY.IS",
    "ONRYT.IS": "ONRYT.IS",
    "PAPIL.IS": "PAPIL.IS",
    "PATEK.IS": "PATEK.IS",
    "KATMR.IS": "KATMR.IS",
    "TMSN.IS": "TMSN.IS",
    "CHKP": "CHKP",
    "CYBR": "CYBR",
    "NICE": "NICE",
    "ESLT": "ESLT",
    "IAI.TA": "IAI.TA",
    "ESLT.TA": "ESLT.TA",
    "NICE.TA": "NICE.TA",
    "MGDL.TA": "MGDL.TA",
    "FIBI.TA": "FIBI.TA",
}


def get_asset_candles(symbol: str, interval: str = "15m", timeout: float = 10.0) -> pd.DataFrame:
    range_map = {
        "15m": "5d",
    }
    range_ = range_map.get(interval, "5d")
    try:
        result = _fetch_chart(symbol, range_, interval, timeout)
        if (not result or result["df"].dropna().empty) and symbol == "^STOXX50E":
            result = _fetch_chart("^STOXX", range_, interval, timeout)
        if not result:
            return pd.DataFrame()
        return result["df"].dropna()
    except Exception:
        return pd.DataFrame()


def fetch_asset_candles(
    assets: dict[str, str], interval: str = "15m", timeout: float = 10.0, max_workers: int = 4
) -> dict[str, pd.DataFrame]:
    if not assets:
        return {}
    out: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = {exe.submit(get_asset_candles, symbol, interval, timeout): asset for asset, symbol in assets.items()}
        for fut in as_completed(futures):
            asset = futures[fut]
            try:
                df = fut.result()
            except Exception:
                continue
            if df is not None and not df.empty:
                out[asset] = df
    return out


def get_btc_candles(interval: str = "1h", period: str = "5d", timeout: float = 10.0) -> pd.DataFrame:
    try:
        df = get_asset_candles("BTC-USD", "15m", timeout)
        if df.empty:
            return df
        if interval == "15m":
            return df
        return resample_candles(df, interval)
    except Exception:
        return pd.DataFrame()


def fetch_yahoo_snapshot(timeout: float = 10.0) -> ProviderResult[dict]:
    start = time.time()
    try:
        data = get_yahoo_snapshot(timeout)
        return ProviderResult(
            ok=True,
            source="yahoo",
            data=data,
            latency_ms=int((time.time() - start) * 1000),
            cache_hit=False,
            error_code=None,
            error_msg=None,
            degraded_mode=False,
            last_good_age_s=None,
        )
    except Exception as exc:
        return ProviderResult(
            ok=False,
            source="yahoo",
            data=None,
            latency_ms=int((time.time() - start) * 1000),
            cache_hit=False,
            error_code="yahoo_exception",
            error_msg=str(exc),
            degraded_mode=True,
            last_good_age_s=None,
        )
