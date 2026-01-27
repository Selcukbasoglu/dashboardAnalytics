from __future__ import annotations


def compute_altcoin_total_value_ex_btc(total_mcap_usd: float | None, btc_dom_pct: float | None) -> float | None:
    if total_mcap_usd is None or btc_dom_pct is None:
        return None
    if total_mcap_usd <= 0 or btc_dom_pct <= 0:
        return None
    factor = 1.0 - (btc_dom_pct / 100.0)
    if factor <= 0:
        return None
    return total_mcap_usd * factor
