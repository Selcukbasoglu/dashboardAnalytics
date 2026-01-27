import os

from app.llm import debate_providers as dp


def test_openrouter_free_model_detect():
    assert dp._is_free_model("meta-llama/llama-3.3-70b-instruct:free") is True
    assert dp._is_free_model("meta-llama/llama-3.3-70b-instruct") is False


def test_openrouter_local_budget_exceeded(monkeypatch):
    monkeypatch.setenv("OPENROUTER_FREE_RPM_BUDGET", "1")
    monkeypatch.setenv("OPENROUTER_FREE_DAILY_BUDGET", "1")
    day = dp._today_key()
    minute = dp._minute_bucket()
    dp._free_daily_count_by_day[day] = 1
    dp._free_rpm_bucket[minute] = 1
    allowed, reason = dp._check_free_budget()
    assert allowed is False
    assert reason in ("daily_budget_exceeded", "rpm_budget_exceeded")
