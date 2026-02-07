from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import requests


GUARD_PREFIX = (
    "Turkce yaz. TSİ kullan. Yatırım tavsiyesi verme. "
    "SADECE verilen evidenceIndex'e dayan. "
    "Her trimSignals ve sectorFocus maddesi en az 1 evidence_id içermeli. "
    "evidenceIndex dışında kanıt kullanma. "
    "ÇIKTI SADECE strict JSON (markdown yok)."
)

# OpenRouter state (process-local)
_keyinfo_cache: dict[str, Any] = {"ts": 0.0, "data": None}
_free_daily_count_by_day: dict[str, int] = {}
_free_rpm_bucket: dict[int, int] = {}
_unavailable_until: dict[tuple[str, str], float] = {}
_gemini_unavailable_until: float = 0.0
STRICT_DEBATE_SCHEMA = os.getenv("DEBATE_SCHEMA_STRICT", "true").lower() in ("1", "true", "yes", "on")
OPENROUTER_JSON_MODE = os.getenv("OPENROUTER_JSON_MODE", "true").lower() in ("1", "true", "yes", "on")


def _normalize_openrouter_base(base: str) -> str:
    return base.rstrip("/")


def _is_free_model(model: str) -> bool:
    return model.endswith(":free")


def _today_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _minute_bucket() -> int:
    return int(time.time() // 60)


def _budget_limits() -> tuple[int, int]:
    rpm = int(os.getenv("OPENROUTER_FREE_RPM_BUDGET", "18") or 18)
    daily = int(os.getenv("OPENROUTER_FREE_DAILY_BUDGET", "45") or 45)
    return rpm, daily


def _check_free_budget() -> tuple[bool, str | None]:
    rpm_budget, daily_budget = _budget_limits()
    day = _today_key()
    minute = _minute_bucket()
    daily_count = _free_daily_count_by_day.get(day, 0)
    minute_count = _free_rpm_bucket.get(minute, 0)
    if daily_count >= daily_budget:
        return False, "daily_budget_exceeded"
    if minute_count >= rpm_budget:
        return False, "rpm_budget_exceeded"
    return True, None


def _record_free_usage() -> None:
    day = _today_key()
    minute = _minute_bucket()
    _free_daily_count_by_day[day] = _free_daily_count_by_day.get(day, 0) + 1
    _free_rpm_bucket[minute] = _free_rpm_bucket.get(minute, 0) + 1


def _unavailable_key(model: str) -> tuple[str, str]:
    return ("openrouter", model)


def _mark_unavailable(model: str) -> None:
    ttl = int(os.getenv("OPENROUTER_MODEL_UNAVAILABLE_TTL_SECONDS", "900") or 900)
    _unavailable_until[_unavailable_key(model)] = time.time() + ttl


def _is_unavailable(model: str) -> bool:
    until = _unavailable_until.get(_unavailable_key(model), 0.0)
    return time.time() < until


def _gemini_ttl_seconds() -> int:
    return int(os.getenv("GEMINI_MODEL_UNAVAILABLE_TTL_SECONDS", "900") or 900)


def _mark_gemini_unavailable() -> None:
    global _gemini_unavailable_until
    _gemini_unavailable_until = time.time() + _gemini_ttl_seconds()


def _gemini_is_unavailable() -> bool:
    return time.time() < _gemini_unavailable_until


def _keyinfo_cached() -> dict[str, Any] | None:
    ttl = int(os.getenv("OPENROUTER_KEYINFO_TTL_SECONDS", "600") or 600)
    ts = float(_keyinfo_cache.get("ts") or 0.0)
    if time.time() - ts < ttl:
        return _keyinfo_cache.get("data")
    return None


def _fetch_keyinfo(api_key: str, base: str, timeout: float) -> tuple[dict | None, str | None]:
    cached = _keyinfo_cached()
    if cached is not None:
        return cached, None
    try:
        res = requests.get(
            f"{_normalize_openrouter_base(base)}/key",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout,
        )
        if res.status_code == 401:
            body = (res.text or "")[:200]
            return None, f"invalid_key:{body}"
        if res.status_code == 402:
            body = (res.text or "")[:200]
            return None, f"insufficient_credits:{body}"
        if res.status_code >= 300:
            body = (res.text or "")[:200]
            return None, f"status:{res.status_code}:{body}"
        data = res.json()
        _keyinfo_cache["ts"] = time.time()
        _keyinfo_cache["data"] = data
        return data, None
    except Exception as exc:
        return None, type(exc).__name__


def get_openrouter_debug() -> dict[str, Any]:
    day = _today_key()
    minute = _minute_bucket()
    return {
        "free_daily_count": _free_daily_count_by_day.get(day, 0),
        "free_minute_count": _free_rpm_bucket.get(minute, 0),
        "keyinfo_cached": _keyinfo_cache.get("data"),
    }


def _ensure_guard(prompt: str) -> str:
    if "strict JSON" in prompt:
        return prompt
    return f"{GUARD_PREFIX}\n{prompt}"


def _extract_json(text: str) -> dict | None:
    if not text:
        return None
    text = text.strip()
    if "```" in text:
        for fence in ("```json", "```JSON", "```"):
            if fence in text:
                parts = text.split(fence, 1)[1]
                payload = parts.split("```", 1)[0].strip()
                try:
                    return json.loads(payload)
                except Exception:
                    pass
    try:
        return json.loads(text)
    except Exception:
        pass
    decoder = json.JSONDecoder()
    for i in range(len(text)):
        if text[i] not in "{[":
            continue
        try:
            obj, _ = decoder.raw_decode(text[i:])
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
    if "{" in text and "}" in text:
        start = text.find("{")
        end = text.rfind("}")
        if end > start:
            try:
                return json.loads(text[start:end + 1])
            except Exception:
                pass
    return None


def _validate_schema(obj: dict | None) -> tuple[bool, str | None]:
    if not obj:
        return False, "empty"
    for key in ("executiveSummary", "trimSignals", "sectorFocus"):
        if key not in obj:
            return False, f"missing_{key}"
    return True, None


def _validate_schema_strict(obj: dict | None) -> tuple[bool, str | None]:
    ok, reason = _validate_schema(obj)
    if not ok:
        return ok, reason
    if not isinstance(obj, dict):
        return False, "not_dict"
    exec_sum = obj.get("executiveSummary")
    if not isinstance(exec_sum, list):
        return False, "executiveSummary_type"
    if len(exec_sum) > 5:
        return False, "executiveSummary_len"
    for key in ("trimSignals", "sectorFocus"):
        items = obj.get(key)
        if not isinstance(items, list):
            return False, f"{key}_type"
        if len(items) > 3:
            return False, f"{key}_len"
        for item in items:
            if not isinstance(item, dict):
                return False, f"{key}_item_type"
            ids = item.get("evidence_ids")
            if not isinstance(ids, list) or not ids:
                return False, f"{key}_evidence_ids"
            if len(ids) > 3:
                return False, f"{key}_evidence_ids_len"
    watch = obj.get("watchMetrics", [])
    if not isinstance(watch, list):
        return False, "watchMetrics_type"
    if len(watch) > 5:
        return False, "watchMetrics_len"
    scenarios = obj.get("scenarios")
    if not isinstance(scenarios, dict):
        return False, "scenarios_type"
    for key in ("base", "risk"):
        items = scenarios.get(key)
        if not isinstance(items, list):
            return False, f"scenarios_{key}_type"
        if len(items) > 3:
            return False, f"scenarios_{key}_len"
    return True, None


def _validate_referee_schema(obj: dict | None) -> tuple[bool, str | None]:
    if not obj:
        return False, "empty"
    for key in ("winner", "confidence", "why", "winner_evidence_ids", "referee_insights", "risk_flags"):
        if key not in obj:
            return False, f"missing_{key}"
    return True, None


def _validate_referee_analyst_schema(obj: dict | None) -> tuple[bool, str | None]:
    if not obj:
        return False, "empty"
    for key in ("mode", "confidence", "final_recommendation", "audit_findings", "improvements", "risk_flags"):
        if key not in obj:
            return False, f"missing_{key}"
    return True, None


def _coerce_schema(obj: dict) -> dict:
    if not isinstance(obj, dict):
        return {}
    coerced = dict(obj)
    coerced.setdefault("generatedAtTSI", "")
    coerced.setdefault("dataStatus", "partial")
    coerced.setdefault("executiveSummary", [])
    coerced.setdefault("portfolioMode", "mixed")
    coerced.setdefault("trimSignals", [])
    coerced.setdefault("sectorFocus", [])
    coerced.setdefault("watchMetrics", [])
    coerced.setdefault("scenarios", {"base": [], "risk": []})
    coerced.setdefault("note", "Sinyal; yatırım tavsiyesi değildir")
    coerced.setdefault("missingData", [])
    coerced["_schema_repaired"] = True
    return coerced


def call_gemini(prompt: str, timeout: float) -> tuple[str, dict | None, str | None]:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return "skipped", None, "missing_key"
    if _gemini_is_unavailable():
        return "skipped", None, "gemini_unavailable_cached"
    primary = os.getenv("GEMINI_MODEL_PRIMARY") or os.getenv("GEMINI_MODEL") or "gemini-2.5-flash"
    fallback = os.getenv("GEMINI_MODEL_FALLBACK") or ""
    base = os.getenv("GEMINI_API_BASE", "https://generativelanguage.googleapis.com/v1beta/models")
    prompt = _ensure_guard(prompt)
    system_text = "Turkce yaz. TSİ kullan. Yatırım tavsiyesi verme. Sadece strict JSON ver."
    max_out = int(os.getenv("GEMINI_MAX_OUTPUT_TOKENS", "1200") or 1200)
    def _make_payload(include_system: bool) -> dict:
        payload = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": max_out,
            },
        }
        if include_system:
            payload["systemInstruction"] = {"parts": [{"text": system_text}]}
        return payload

    def _call(model_name: str, use_v1beta: bool, include_system: bool, attempt: int):
        url = f"{base}/{model_name}:generateContent"
        payload = _make_payload(include_system)
        if not use_v1beta and "/v1beta/models" in url:
            url = url.replace("/v1beta/models", "/v1/models")
            payload = _make_payload(False)
        res = requests.post(
            url,
            headers={"x-goog-api-key": api_key, "Content-Type": "application/json"},
            json=payload,
            timeout=timeout,
        )
        if res.status_code in (429, 503) and attempt == 0:
            time.sleep(0.2 + 0.3 * attempt)
            return None, f"retry:{res.status_code}"
        if res.status_code == 429:
            _mark_gemini_unavailable()
            body = (res.text or "")[:200]
            return None, f"status:429:{body}"
        if res.status_code >= 300:
            body = (res.text or "")[:200]
            return None, f"status:{res.status_code}:{body}"
        data = res.json()
        text = None
        if isinstance(data, dict):
            candidates = data.get("candidates") or []
            if candidates:
                parts = (candidates[0].get("content") or {}).get("parts") or []
                if parts:
                    text = "".join([p.get("text") or "" for p in parts if isinstance(p, dict)])
        raw_payload = text or (res.text or "")
        parsed = _extract_json(raw_payload)
        ok, reason = _validate_schema(parsed)
        if not ok:
            snippet = (raw_payload or "")[:200]
            return None, f"schema:{reason}:{snippet}"
        return parsed, None

    models = [primary]
    if fallback and fallback != primary:
        models.append(fallback)

    for model_name in models:
        for attempt in range(2):
            try:
                parsed, err = _call(model_name, True, True, attempt)
                if err and err.startswith("status:404"):
                    parsed, err = _call(model_name, False, False, attempt)
                if err and err.startswith("status:400") and "systemInstruction" in err:
                    parsed, err = _call(model_name, True, False, attempt)
                if err and err.startswith("retry:"):
                    continue
                if err:
                    if model_name != models[-1]:
                        break
                    return "fail", None, err
                return "ok", parsed, None
            except Exception as exc:
                return "fail", None, type(exc).__name__
    return "fail", None, "no_model_succeeded"


def _call_openrouter_model(
    prompt: str,
    timeout: float,
    model: str | None,
    relax_schema: bool = False,
    ignore_unavailable: bool = False,
) -> tuple[str, dict | None, str | None]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        return "skipped", None, "missing_openrouter_key"
    if not model:
        return "skipped", None, "missing_openrouter_model"
    if _is_unavailable(model) and not ignore_unavailable:
        return "skipped", None, "model_unavailable_cached"
    base = os.getenv("OPENROUTER_API_BASE", "https://openrouter.ai/api/v1")
    prompt = _ensure_guard(prompt)
    if _is_free_model(model):
        allowed, reason = _check_free_budget()
        if not allowed:
            return "skipped", None, f"local_budget_exceeded:{reason}"
    keyinfo, key_err = _fetch_keyinfo(api_key, base, timeout)
    if key_err == "invalid_key":
        return "fail", None, "invalid_key"
    if key_err == "insufficient_credits":
        _mark_unavailable(model)
        return "fail", None, "insufficient_credits"

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    referer = os.getenv("OPENROUTER_HTTP_REFERER")
    title = os.getenv("OPENROUTER_X_TITLE")
    if referer:
        headers["HTTP-Referer"] = referer
    if title:
        headers["X-Title"] = title

    payload = {
        "model": model,
        "temperature": 0.2,
        "max_tokens": 900,
        "messages": [
            {"role": "user", "content": prompt},
        ],
    }
    if OPENROUTER_JSON_MODE:
        payload["response_format"] = {"type": "json_object"}
    url = f"{_normalize_openrouter_base(base)}/chat/completions"
    for attempt in range(2):
        try:
            res = requests.post(url, headers=headers, json=payload, timeout=timeout)
            if res.status_code in (429, 503) and attempt == 0:
                time.sleep(0.2 + 0.3 * attempt)
                continue
            if res.status_code == 400 and attempt == 0 and OPENROUTER_JSON_MODE:
                payload.pop("response_format", None)
                res = requests.post(url, headers=headers, json=payload, timeout=timeout)
            if res.status_code in (401, 402):
                _mark_unavailable(model)
                body = (res.text or "")[:200]
                return "fail", None, f"status:{res.status_code}:{body}"
            if res.status_code == 429:
                _mark_unavailable(model)
                body = (res.text or "")[:200]
                return "fail", None, f"rate_limited:{body}"
            if res.status_code == 503:
                _mark_unavailable(model)
                body = (res.text or "")[:200]
                return "fail", None, f"no_provider:{body}"
            if res.status_code >= 300:
                body = (res.text or "")[:200]
                return "fail", None, f"status:{res.status_code}:{body}"
            data = res.json()
            content = (
                data.get("choices", [{}])[0].get("message", {}).get("content")
                if isinstance(data, dict)
                else None
            )
            if isinstance(content, list):
                content = "".join(
                    [p.get("text") or "" for p in content if isinstance(p, dict)]
                )
            raw_payload = content or ""
            if not raw_payload:
                raw_payload = res.text or ""
            parsed = _extract_json(raw_payload)
            if STRICT_DEBATE_SCHEMA:
                ok, reason = _validate_schema_strict(parsed)
            else:
                ok, reason = _validate_schema(parsed)
            if not ok:
                if relax_schema and isinstance(parsed, dict):
                    return "ok", _coerce_schema(parsed), None
                snippet = (raw_payload or "")[:200]
                return "fail", None, f"schema:{reason}:{snippet}"
            if _is_free_model(model):
                _record_free_usage()
            return "ok", parsed, None
        except Exception as exc:
            return "fail", None, type(exc).__name__
    return "fail", None, "openrouter_failed"


def call_openrouter(prompt: str, timeout: float, force: bool = False) -> tuple[str, dict | None, str | None]:
    candidates_env = os.getenv("OPENROUTER_MODEL_PRIMARY_CANDIDATES", "")
    if candidates_env:
        candidates = [m.strip() for m in candidates_env.split(",") if m.strip()]
    else:
        candidates = [os.getenv("OPENROUTER_MODEL_PRIMARY", "meta-llama/llama-3.3-70b-instruct:free")]
    last_err = "no_model_succeeded"
    for model in candidates:
        status, data, err = _call_openrouter_model(prompt, timeout, model, ignore_unavailable=force)
        if status == "ok":
            return status, data, err
        last_err = err or last_err
    return "fail", None, last_err


def call_openrouter_openai(prompt: str, timeout: float, force: bool = False) -> tuple[str, dict | None, str | None]:
    candidates_env = os.getenv("OPENROUTER_MODEL_SECONDARY_CANDIDATES", "")
    if candidates_env:
        candidates = [m.strip() for m in candidates_env.split(",") if m.strip()]
    else:
        candidates = [os.getenv("OPENROUTER_MODEL_SECONDARY", "openai/gpt-oss-120b:free")]
    last_err = "no_model_succeeded"
    for model in candidates:
        status, data, err = _call_openrouter_model(prompt, timeout, model, relax_schema=True, ignore_unavailable=force)
        if status == "ok":
            return status, data, err
        last_err = err or last_err
    return "fail", None, last_err


# Referee (OpenRouter, strict JSON)
def call_openrouter_referee(ref_ctx: dict, timeout_ms: int, mode: str = "judge") -> tuple[str, dict | None, str | None, dict]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    base = os.getenv("OPENROUTER_API_BASE", "https://openrouter.ai/api/v1")
    model = os.getenv("PORTFOLIO_DEBATE_REFEREE_MODEL", "google/gemini-2.0-flash-exp:free")
    if not api_key:
        return "skipped", None, "missing_openrouter_key", {"policy_blocked": False}
    if _is_unavailable(model):
        return "skipped", None, "model_unavailable_cached", {"policy_blocked": False}
    if _is_free_model(model):
        allowed, reason = _check_free_budget()
        if not allowed:
            return "skipped", None, f"local_budget_exceeded:{reason}", {"policy_blocked": False}

    temperature = float(os.getenv("PORTFOLIO_DEBATE_REFEREE_TEMPERATURE", "0.2") or 0.2)
    max_tokens = int(os.getenv("PORTFOLIO_DEBATE_REFEREE_MAX_TOKENS", "700") or 700)
    timeout = max(1.0, float(timeout_ms) / 1000.0)
    skip_on_policy = os.getenv("PORTFOLIO_DEBATE_REFEREE_SKIP_ON_POLICY_ERROR", "true").lower() in ("1", "true", "yes", "on")

    system_text = (
        "Turkce yaz. TSİ kullan. ÇIKTI SADECE JSON (markdown yok). "
        "Bu JSON parse edilecek; eksiksiz kapat, ekstra metin ekleme. "
        "Sadece verilen context’e dayan; dış bilgi yok. "
        "Kanıt varsa evidence_id ver; yoksa assumption=true kullan."
    )

    if mode == "judge":
        prompt = (
            "Sana constraintsSnapshot, newsContentProfile ve iki planın JSON çıktıları veriliyor.\n"
            "Görev:\n"
            "1) Hangisi daha iyi? winner: provider_a / provider_b / tie\n"
            "2) Kısa gerekçeler (3–7 madde)\n"
            "3) Kendi görüşlerin (max 6 madde)\n"
            "4) İstersen küçük “contrarian idea” öner (small bet), ama assumption/evidence şart\n"
            "5) Risk bayrakları (fx_risk, low_signal, concentration, turnover_risk)\n"
            "REQUIRED JSON SCHEMA:\n"
            "{\n"
            '  "winner":"provider_a|provider_b|tie",\n'
            '  "confidence":0-100,\n'
            '  "why":[{"text":"...", "evidence_ids":["..."], "assumption":false}],\n'
            '  "winner_evidence_ids":["..."],\n'
            '  "referee_insights":[{"text":"...", "evidence_ids":["..."], "assumption":true|false}],\n'
            '  "contrarian_idea":{\n'
            '    "text":"...",\n'
            '    "horizon":"daily|weekly|monthly",\n'
            '    "actions":[{"type":"trim|add|sectorFocus|hold","target":"...", "size_hint_pct":0.0}],\n'
            '    "evidence_ids":["..."],\n'
            '    "assumption":true|false\n'
            "  },\n"
            '  "risk_flags":{"fx_risk":true|false,"low_signal":true|false,"concentration":true|false,"turnover_risk":true|false}\n'
            "}\n"
            "CONTEXT_JSON:\n"
            f"{json.dumps(ref_ctx, sort_keys=True, ensure_ascii=False, separators=(',', ':'))}"
        )
    else:
        prompt = (
            "You are the referee in ANALYST mode.\n"
            "You will receive: constraintsSnapshot, newsContentProfile, primary_plan, provider_meta.\n"
            "Tasks:\n"
            "1) primary_plan’ı kısıtlar ve risk rejimi açısından denetle (turnover_cap, max_weight, crypto_max, low_signal, evidence_id eksikleri).\n"
            "2) Planı geliştir: 3-6 madde improvement suggestions.\n"
            "3) 1-2 adet küçük contrarian/hedge önerisi (assumption/evidence şart).\n"
            "4) final_recommendation: accept / revise / hold.\n"
            "REQUIRED JSON SCHEMA:\n"
            "{\n"
            '  "mode":"analyst_single_provider|analyst_low_disagreement",\n'
            '  "confidence":0-100,\n'
            '  "final_recommendation":{"action":"accept|revise|hold","summary":"..."},\n'
            '  "audit_findings":[{"issue":"...", "severity":"low|med|high", "evidence_ids":["..."], "assumption":true|false}],\n'
            '  "improvements":[{"text":"...", "evidence_ids":["..."], "assumption":true|false}],\n'
            '  "contrarian_idea":{"text":"...", "horizon":"daily|weekly|monthly", "actions":[{"type":"trim|add|sectorFocus|hold","target":"...", "size_hint_pct":0.0}], "evidence_ids":["..."], "assumption":true|false},\n'
            '  "risk_flags":{"fx_risk":true|false,"low_signal":true|false,"concentration":true|false,"turnover_risk":true|false}\n'
            "}\n"
            "CONTEXT_JSON:\n"
            f"{json.dumps(ref_ctx, sort_keys=True, ensure_ascii=False, separators=(',', ':'))}"
        )

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    referer = os.getenv("OPENROUTER_HTTP_REFERER")
    title = os.getenv("OPENROUTER_X_TITLE")
    if referer:
        headers["HTTP-Referer"] = referer
    if title:
        headers["X-Title"] = title
    payload = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system_text},
            {"role": "user", "content": prompt},
        ],
        "response_format": {"type": "json_object"},
    }
    url = f"{_normalize_openrouter_base(base)}/chat/completions"
    meta = {"referee_model_used": model, "policy_blocked": False}
    try:
        res = requests.post(url, headers=headers, json=payload, timeout=timeout)
        if res.status_code in (401, 402):
            _mark_unavailable(model)
            return "fail", None, f"status:{res.status_code}", meta
        if res.status_code == 429:
            _mark_unavailable(model)
            return "fail", None, "rate_limited", meta
        if res.status_code == 503:
            _mark_unavailable(model)
            return "fail", None, "no_provider", meta
        if res.status_code >= 300:
            body = (res.text or "")[:200]
            if "No endpoints found matching your data policy" in body:
                meta["policy_blocked"] = True
                if skip_on_policy:
                    return "skipped_policy", None, "policy_blocked", meta
                return "fail", None, "policy_blocked", meta
            return "fail", None, f"status:{res.status_code}:{body}", meta
        data = res.json()
        content = (
            data.get("choices", [{}])[0].get("message", {}).get("content")
            if isinstance(data, dict)
            else None
        )
        if isinstance(content, list):
            content = "".join([p.get("text") or "" for p in content if isinstance(p, dict)])
        raw_payload = content or (res.text or "")
        parsed = _extract_json(raw_payload)
        ok, reason = (_validate_referee_schema(parsed) if mode == "judge" else _validate_referee_analyst_schema(parsed))
        if not ok:
            snippet = (raw_payload or "")[:200]
            return "fail", None, f"schema:{reason}:{snippet}", meta
        if _is_free_model(model):
            _record_free_usage()
        meta["raw_snip"] = (raw_payload or "")[:200]
        if isinstance(data, dict) and isinstance(data.get("usage"), dict):
            meta["referee_tokens"] = data["usage"].get("total_tokens")
        return "ok", parsed, None, meta
    except Exception as exc:
        return "fail", None, type(exc).__name__, meta

# Provider name is OpenRouter (Llama free).
