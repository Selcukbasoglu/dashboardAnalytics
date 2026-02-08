from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from app.llm.gemini_client import SUMMARY_HEADERS

EVIDENCE_RE = re.compile(r"\[KANIT:[TL](?:\d+|\?)\]", flags=re.IGNORECASE)
SYMBOL_RE_TEMPLATE = r"\b{symbol}\b"
BULLET_PREFIX = ("-", "*", "•")

ACTION_HINTS = (
    "izle",
    "takip",
    "artir",
    "azalt",
    "koru",
    "hedge",
    "tetik",
    "risk",
    "volatil",
)

TEMPLATE_FLAGS = (
    "basligina dayali etki notu",
    "yetersiz veri",
    "lorem ipsum",
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _portfolio_symbols(payload: dict[str, Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    rows = (payload.get("holdings") or []) + (payload.get("holdings_full") or [])
    for row in rows:
        sym = str(row.get("symbol") or "").strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out


def _section_header_coverage(summary: str) -> tuple[float, list[str]]:
    lower = (summary or "").lower()
    present = [h for h in SUMMARY_HEADERS if h.lower() in lower]
    if not SUMMARY_HEADERS:
        return 0.0, []
    return len(present) / len(SUMMARY_HEADERS), present


def _evidence_density(summary: str, payload: dict[str, Any]) -> tuple[float, int, int]:
    count = len(EVIDENCE_RE.findall(summary or ""))
    top_news = payload.get("topNews") or []
    local_news = payload.get("localHeadlines") or []
    evidence_target = 2 if (len(top_news) + len(local_news)) <= 6 else 3
    score = min(1.0, count / max(1, evidence_target))
    return score, count, evidence_target


def _portfolio_grounding(summary: str, payload: dict[str, Any]) -> tuple[float, list[str]]:
    symbols = _portfolio_symbols(payload)
    if not symbols:
        return 1.0, []
    hits: list[str] = []
    for sym in symbols:
        if re.search(SYMBOL_RE_TEMPLATE.format(symbol=re.escape(sym)), summary or "", flags=re.IGNORECASE):
            hits.append(sym)
    target = min(3, len(symbols))
    return min(1.0, len(hits) / max(1, target)), hits


def _actionability(summary: str) -> tuple[float, int]:
    lines = [ln.strip().lower() for ln in (summary or "").splitlines()]
    count = 0
    for line in lines:
        if not line or line[0] not in BULLET_PREFIX:
            continue
        if any(h in line for h in ACTION_HINTS):
            count += 1
    return min(1.0, count / 2.0), count


def _assumption_hygiene(summary: str) -> tuple[float, int, int]:
    lines = [ln.strip().lower() for ln in (summary or "").splitlines() if ln.strip()]
    model_lines = [ln for ln in lines if "model gorusu" in ln]
    if not model_lines:
        return 0.0, 0, 0
    marked = sum(1 for ln in model_lines if "varsayim" in ln)
    return marked / len(model_lines), marked, len(model_lines)


def _non_template_score(summary: str) -> tuple[float, list[str]]:
    lower = (summary or "").lower()
    hits = [flag for flag in TEMPLATE_FLAGS if flag in lower]
    if not hits:
        return 1.0, []
    penalty = min(1.0, 0.55 * len(hits))
    return max(0.0, 1.0 - penalty), hits


def _length_score(summary: str) -> float:
    size = len((summary or "").strip())
    if size <= 0:
        return 0.0
    if size < 220:
        return max(0.0, size / 220.0)
    if size <= 3600:
        return 1.0
    overflow = min(1.0, (size - 3600) / 2400.0)
    return max(0.0, 1.0 - overflow)


def score_summary(summary: str, payload: dict[str, Any]) -> dict[str, Any]:
    header_score, headers_present = _section_header_coverage(summary)
    evidence_score, evidence_count, evidence_target = _evidence_density(summary, payload)
    grounding_score, symbol_hits = _portfolio_grounding(summary, payload)
    action_score, action_count = _actionability(summary)
    assumption_score, marked_varsayim, model_lines = _assumption_hygiene(summary)
    non_template_score, template_hits = _non_template_score(summary)
    length_score = _length_score(summary)

    weights = {
        "header_coverage": 0.20,
        "evidence_density": 0.20,
        "portfolio_grounding": 0.20,
        "assumption_hygiene": 0.15,
        "actionability": 0.10,
        "non_template": 0.10,
        "length_quality": 0.05,
    }

    scores = {
        "header_coverage": round(header_score, 4),
        "evidence_density": round(evidence_score, 4),
        "portfolio_grounding": round(grounding_score, 4),
        "assumption_hygiene": round(assumption_score, 4),
        "actionability": round(action_score, 4),
        "non_template": round(non_template_score, 4),
        "length_quality": round(length_score, 4),
    }

    overall = 0.0
    for key, weight in weights.items():
        overall += weight * _safe_float(scores.get(key), 0.0)
    scores["overall"] = round(overall, 4)

    debug = {
        "headers_present": headers_present,
        "evidence_count": evidence_count,
        "evidence_target": evidence_target,
        "portfolio_symbol_hits": symbol_hits,
        "actionable_bullet_count": action_count,
        "model_lines": model_lines,
        "varsayim_marked_lines": marked_varsayim,
        "template_hits": template_hits,
        "summary_chars": len((summary or "").strip()),
    }
    return {"scores": scores, "debug": debug}


def evaluate_case(
    case: dict[str, Any],
    summary: str,
    payload: dict[str, Any],
    expect_profile: str = "soft",
) -> dict[str, Any]:
    result = score_summary(summary, payload)
    scores = result["scores"]
    profiles = case.get("expect_profiles") or {}
    expect = (profiles.get(expect_profile) if isinstance(profiles, dict) else None) or case.get("expect") or {}

    violations: list[str] = []

    for key, threshold in (expect.get("min_scores") or {}).items():
        metric_value = _safe_float(scores.get(key), 0.0)
        min_value = _safe_float(threshold, 0.0)
        if metric_value < min_value:
            violations.append(f"score:{key}<{min_value:.2f} (got {metric_value:.2f})")

    summary_text = summary or ""
    lower = summary_text.lower()

    for token in expect.get("must_contain") or []:
        if str(token).lower() not in lower:
            violations.append(f"missing_text:{token}")

    for pattern in expect.get("must_regex") or []:
        try:
            if re.search(pattern, summary_text, flags=re.IGNORECASE) is None:
                violations.append(f"missing_regex:{pattern}")
        except re.error:
            violations.append(f"invalid_regex:{pattern}")

    for token in expect.get("forbid") or []:
        if str(token).lower() in lower:
            violations.append(f"forbidden_text:{token}")

    return {
        "case_id": case.get("id"),
        "scores": scores,
        "debug": result.get("debug") or {},
        "violations": violations,
        "passed": len(violations) == 0,
    }


def load_cases(path: str | Path) -> list[dict[str, Any]]:
    raw = Path(path)
    if not raw.exists():
        raise FileNotFoundError(f"eval dataset not found: {raw}")

    if raw.suffix.lower() == ".jsonl":
        out: list[dict[str, Any]] = []
        for line in raw.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            out.append(json.loads(stripped))
        return out

    data = json.loads(raw.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("dataset json must be a list of cases")
    return [x for x in data if isinstance(x, dict)]
