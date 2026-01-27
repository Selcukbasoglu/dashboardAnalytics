from __future__ import annotations

from typing import Any

from app.infra.cache import now_iso


def normalize_coingecko_global(snapshot: dict[str, Any] | None) -> dict[str, Any] | None:
    if not snapshot:
        return None
    total_mcap = snapshot.get("total_mcap_usd")
    btc_dom = (snapshot.get("dominance") or {}).get("btc")
    total_vol = snapshot.get("total_vol_usd")
    if total_mcap is None and btc_dom is None and total_vol is None:
        return None
    return {
        "total_mcap_usd": total_mcap,
        "btc_dominance_pct": btc_dom,
        "total_vol_usd": total_vol,
        "tsISO": now_iso(),
        "source": "coingecko",
    }


def select_global_overview(
    primary: dict[str, Any] | None,
    secondary: dict[str, Any] | None,
) -> dict[str, Any] | None:
    for candidate in (primary, secondary):
        if _valid_overview(candidate):
            return candidate
    return None


def _valid_overview(candidate: dict[str, Any] | None) -> bool:
    if not candidate:
        return False
    total = candidate.get("total_mcap_usd")
    dom = candidate.get("btc_dominance_pct")
    try:
        return float(total) > 0 and float(dom) > 0
    except Exception:
        return False
