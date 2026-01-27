from __future__ import annotations

from datetime import datetime, timezone

from app.models import NewsItem
from app.services.portfolio_engine import compute_news_impact, build_optimizer, compute_fx_risk, PortfolioSettings


def _now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def test_news_direct_indirect_splits_sum_to_total():
    alias_map = {
        "symbols": {
            "ABC": {"aliases": ["Alpha"], "sector": "SECTOR1"},
        },
        "fx": {},
    }
    item = NewsItem(
        title="Alpha earnings beat",
        url="http://example.com/a",
        publishedAtISO=_now_iso(),
        relevance_score=100,
        quality_score=100,
        impact_channel=["büyüme"],
        event_type="EARNINGS",
        sector_impacts=[{"sector": "SECTOR1", "direction": "UP", "impact_score": 50}],
    )
    items, _ = compute_news_impact([item], alias_map, None, [], None)
    assert items, "news impact should produce item"
    out = items[0]
    total = out["impact_by_symbol"]["ABC"]
    direct = out["impact_by_symbol_direct"]["ABC"]
    indirect = out["impact_by_symbol_indirect"]["ABC"]
    assert abs(total - (direct + indirect)) < 1e-6


def test_hold_gate_no_news_items():
    settings = PortfolioSettings()
    recs = build_optimizer(
        [],
        {},
        {},
        {},
        [],
        settings,
        coverage_ratio=0.0,
        coverage_total=0,
        low_signal_ratio=0.0,
        fx_risk_proxy=0.0,
    )
    assert all(r.get("mode") == "HOLD" for r in recs)


def test_hold_gate_low_coverage():
    settings = PortfolioSettings()
    recs = build_optimizer(
        [],
        {},
        {},
        {},
        [],
        settings,
        coverage_ratio=0.10,
        coverage_total=10,
        low_signal_ratio=0.0,
        fx_risk_proxy=0.0,
    )
    assert all(r.get("mode") == "HOLD" for r in recs)


def test_effective_turnover_cap_scaled():
    settings = PortfolioSettings()
    holdings = [
        {"symbol": "ABC", "weight": 0.1, "asset_class": "BIST", "currency": "TRY", "mom_z_7d": 0.0, "vol_30d": 0.01}
    ]
    recs = build_optimizer(
        holdings,
        {},
        {},
        {},
        [],
        settings,
        coverage_ratio=0.5,
        coverage_total=10,
        low_signal_ratio=0.0,
        fx_risk_proxy=0.0,
    )
    daily = next(r for r in recs if r.get("period") == "daily")
    assert abs(daily["turnover_cap"] - (settings.turnover_daily * 0.5)) < 1e-9


def test_score_breakdown_present():
    settings = PortfolioSettings()
    holdings = [
        {"symbol": "ABC", "weight": 0.1, "asset_class": "BIST", "currency": "TRY", "mom_z_7d": 3.0, "vol_30d": 0.01},
    ]
    recs = build_optimizer(
        holdings,
        {"ABC": 0.8},
        {"ABC": 0.8},
        {"ABC": 0.0},
        [],
        settings,
        coverage_ratio=1.0,
        coverage_total=10,
        low_signal_ratio=0.0,
        fx_risk_proxy=0.0,
    )
    daily = next(r for r in recs if r.get("period") == "daily")
    if daily["actions"]:
        assert "score_breakdown" in daily["actions"][0]


def test_fx_risk_flag_threshold():
    allocation = {"by_currency": {"USD": 0.60}}
    usd_exposure, fx_proxy, flag = compute_fx_risk(allocation, threshold=0.50)
    assert usd_exposure == 0.60
    assert fx_proxy == 0.60
    assert flag is True
