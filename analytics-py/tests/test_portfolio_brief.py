from app.services.portfolio_brief import build_daily_brief


def test_daily_brief_schema_and_tsi():
    portfolio = {
        "holdings": [
            {
                "symbol": "BTC",
                "asset_class": "CRYPTO",
                "weight": 0.4,
                "mkt_value_base": 100,
                "data_status": "ok",
            },
            {
                "symbol": "AMD",
                "asset_class": "NASDAQ",
                "weight": 0.2,
                "mkt_value_base": 50,
                "data_status": "ok",
            },
        ],
        "allocation": {"by_asset_class": {"CRYPTO": 0.4, "NASDAQ": 0.2, "BIST": 0.4}},
        "risk": {"hhi": 0.2, "max_weight": 0.4, "vol_30d": 0.1, "var_95_1d": 0.02, "data_status": "ok"},
        "newsImpact": {
            "coverage": {"matched": 1, "total": 2},
            "summary": {"impact_by_symbol": {"BTC": 0.2}},
            "items": [
                {
                    "title": "BTC headline",
                    "impact_by_symbol": {"BTC": 0.2},
                    "match_debug": [{"method": "direct"}],
                    "event_type": "MACRO",
                    "impact_channel": ["risk_primi"],
                }
            ],
        },
        "recommendations": [
            {
                "period": "daily",
                "actions": [
                    {"symbol": "BTC", "action": "increase", "deltaWeight": 0.02, "reason": ["score=0.1"], "confidence": 70}
                ],
            }
        ],
        "debug_notes": [],
    }

    brief = build_daily_brief(portfolio, window="24h", period="daily", base="TRY")
    assert "TSİ" in brief["generatedAtTSI"]
    assert brief["newsImpactSummary"]["coverage"]["direct"] == 1
    assert brief["topHoldings"][0]["symbol"] == "BTC"
    assert brief["optimizerHints"]["period"] == "daily"
    assert "executiveSummary" in brief
