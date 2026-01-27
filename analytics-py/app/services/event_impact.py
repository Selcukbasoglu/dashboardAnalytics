from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Iterable

from app.infra.db import DB


WINDOWS = {"15m": 15, "1h": 60, "3h": 180, "6h": 360}


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_ts(ts_utc: str) -> datetime | None:
    try:
        return datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
    except Exception:
        return None


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
    try:
        return float(row["close"] or 0.0)
    except Exception:
        return None


def _returns_series(closes: list[float], step: int) -> list[float]:
    out: list[float] = []
    if step <= 0 or len(closes) <= step:
        return out
    for i in range(step, len(closes)):
        prev = closes[i - step]
        cur = closes[i]
        if not prev:
            continue
        out.append((cur - prev) / prev * 100.0)
    return out


def _sigma_for_window(db: DB, asset: str, minutes: int, lookback_days: int) -> float | None:
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(days=lookback_days))
    rows = db.fetchall(
        """
        SELECT close FROM price_bars
        WHERE asset = ? AND ts_utc >= ?
        ORDER BY ts_utc ASC
        """,
        (asset, cutoff),
    )
    closes = [float(r["close"]) for r in rows if r["close"] is not None]
    step = max(1, minutes // 15)
    rets = _returns_series(closes, step)
    if len(rets) < 20 and lookback_days > 7:
        return _sigma_for_window(db, asset, minutes, 7)
    if len(rets) < 5:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / max(1, len(rets) - 1)
    return math.sqrt(var) if var > 0 else None


def _target_asset(target: str) -> str:
    if target == "ALTS":
        return "ALTS"
    return target


def compute_realized_impacts(
    db: DB,
    cluster_ids: Iterable[str],
    ts_map: dict[str, str],
    targets: Iterable[str] = ("BTC", "ETH", "ALTS"),
    lookback_days: int = 30,
) -> list[tuple[str, str, str, float | None, float | None, str]]:
    rows = []
    now = _iso(datetime.now(timezone.utc))
    for cluster_id in cluster_ids:
        ts_utc = ts_map.get(cluster_id)
        if not ts_utc:
            continue
        for target in targets:
            asset = _target_asset(target)
            if target == "ALTS":
                asset = "ALTS"
                alt_row = db.fetchone(
                    "SELECT 1 FROM price_bars WHERE asset = ? LIMIT 1",
                    (asset,),
                )
                if not alt_row:
                    asset = "ETH"
            t0 = _parse_ts(ts_utc)
            if not t0:
                continue
            sigma_cache: dict[int, float | None] = {}
            for tf, minutes in WINDOWS.items():
                start = _price_at(db, asset, ts_utc)
                if start is None or start == 0:
                    continue
                end_ts = _iso(t0 + timedelta(minutes=minutes))
                end = _price_at(db, asset, end_ts)
                if end is None or end == 0:
                    continue
                realized_ret = (end - start) / start * 100.0
                if minutes not in sigma_cache:
                    sigma_cache[minutes] = _sigma_for_window(db, asset, minutes, lookback_days)
                sigma = sigma_cache[minutes]
                realized_z = realized_ret / sigma if sigma else None
                rows.append((cluster_id, target, tf, realized_ret, realized_z, now))
    return rows


def upsert_realized_impacts(db: DB, rows: list[tuple[str, str, str, float | None, float | None, str]]) -> int:
    if not rows:
        return 0
    db.upsert(
        "event_impact",
        ["cluster_id", "target", "tf", "realized_ret", "realized_z", "computed_at"],
        ["cluster_id", "target", "tf"],
        rows,
    )
    return len(rows)


def load_event_impacts(db: DB, cluster_ids: list[str], tf: str) -> dict[str, dict[str, float | None]]:
    if not cluster_ids:
        return {}
    placeholders = ",".join(["?"] * len(cluster_ids))
    rows = db.fetchall(
        f"""
        SELECT cluster_id, target, realized_ret, realized_z
        FROM event_impact
        WHERE tf = ? AND cluster_id IN ({placeholders})
        """,
        (tf, *cluster_ids),
    )
    out: dict[str, dict[str, dict[str, float | None]]] = {}
    for row in rows:
        cid = row["cluster_id"]
        tgt = row["target"]
        out.setdefault(tgt, {})[cid] = {
            "realized_ret": row["realized_ret"],
            "realized_z": row["realized_z"],
        }
    return out


def load_event_impacts_all(db: DB, cluster_ids: list[str]) -> dict[str, list[dict]]:
    if not cluster_ids:
        return {}
    placeholders = ",".join(["?"] * len(cluster_ids))
    rows = db.fetchall(
        f"""
        SELECT cluster_id, target, tf, realized_ret, realized_z, computed_at
        FROM event_impact
        WHERE cluster_id IN ({placeholders})
        """,
        tuple(cluster_ids),
    )
    out: dict[str, list[dict]] = {}
    for row in rows:
        out.setdefault(row["cluster_id"], []).append(
            {
                "target": row["target"],
                "tf": row["tf"],
                "realized_ret": row["realized_ret"],
                "realized_z": row["realized_z"],
                "computed_at": row["computed_at"],
            }
        )
    return out


def fetch_cluster_times(db: DB, lookback_days: int) -> dict[str, str]:
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(days=lookback_days))
    rows = db.fetchall(
        """
        SELECT event_id, cluster_id, ts_utc
        FROM events
        WHERE ts_utc >= ?
        ORDER BY ts_utc ASC
        """,
        (cutoff,),
    )
    out: dict[str, str] = {}
    for row in rows:
        cid = row["cluster_id"] or row["event_id"]
        if cid not in out:
            out[cid] = row["ts_utc"]
    return out
