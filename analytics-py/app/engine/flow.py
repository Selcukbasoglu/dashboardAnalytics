from __future__ import annotations

from app.models import EventPoint, NewsItem


def clamp(val: float, low: float, high: float) -> float:
    return max(low, min(high, val))


def build_flow(coingecko: dict, yahoo: dict, news: list[NewsItem], event_points: list[EventPoint]):
    deltas = coingecko.get("deltas") or {}
    btc_d = deltas.get("btc_d", 0.0)
    usdt_d = deltas.get("usdt_d", 0.0)
    usdc_d = deltas.get("usdc_d", 0.0)
    total_vol = deltas.get("total_vol", 0.0)

    score = 50.0
    score += clamp(-btc_d * 2.0, -10, 10)
    score += clamp(- (usdt_d + usdc_d) * 3.0, -12, 12)
    score += clamp(total_vol / 1e9, -10, 10)

    tag_counts = {}
    for item in news:
        for tag in item.tags:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1

    risk_tags = tag_counts.get("War", 0) + tag_counts.get("Energy", 0) + tag_counts.get("Reg", 0)
    growth_tags = tag_counts.get("ETF", 0) + tag_counts.get("CEO", 0) + tag_counts.get("DataCenter", 0)

    score += clamp(growth_tags * 1.5, 0, 8)
    score -= clamp(risk_tags * 1.2, 0, 10)

    if event_points:
        if any(p.volume_z >= 2.0 for p in event_points):
            score += 4

    score = int(clamp(score, 0, 100))

    evidence = []
    evidence.append(f"BTC.D delta: {btc_d:.2f} | USDT.D delta: {usdt_d:.2f} | USDC.D delta: {usdc_d:.2f}")
    evidence.append(f"TotalVol delta (usd): {total_vol:,.0f}")
    evidence.append(f"News tags: War {tag_counts.get('War', 0)} | Energy {tag_counts.get('Energy', 0)} | ETF {tag_counts.get('ETF', 0)}")

    if event_points:
        top = max(event_points, key=lambda x: x.volume_z)
        evidence.append(f"Event-study volume_z: {top.volume_z:.2f} | price_move: {top.price_move_pct:.2f}%")

    watch_metrics = [
        "BTC.D",
        "USDT.D",
        "TotalVol",
        "DXY",
        "Funding",
    ]

    return score, evidence[:6], watch_metrics[:5]
