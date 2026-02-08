from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.evals.gemini_summary.scorer import evaluate_case, load_cases
from app.llm import gemini_client


DEFAULT_DATASET = Path(__file__).with_name("dataset_v1.json")
DEFAULT_REPORT_DIR = Path(__file__).parents[3] / "eval_reports"


def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _truncate(text: str, limit: int) -> str:
    raw = (text or "").strip()
    if len(raw) <= limit:
        return raw
    return raw[: max(0, limit - 3)].rstrip() + "..."


def _generate_summary(payload: dict[str, Any], mode: str, timeout: float) -> tuple[str, str | None]:
    if mode == "rule_based":
        prepared = gemini_client._prepare_payload(payload, budget_chars=gemini_client.MAX_PROMPT_PAYLOAD_CHARS)
        return gemini_client._build_rule_based_summary(prepared), None

    if mode == "live":
        summary, err = gemini_client.generate_portfolio_summary(payload, timeout=timeout)
        if summary:
            return summary, err
        return "", err or "live_generation_failed"

    raise ValueError(f"unsupported mode: {mode}")


def _report_path(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit)
    DEFAULT_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    return DEFAULT_REPORT_DIR / f"gemini_summary_eval_{_now_ts()}.json"


def run_eval(
    dataset_path: str,
    mode: str,
    timeout: float,
    case_limit: int,
    summary_preview_chars: int,
    profile: str,
) -> dict[str, Any]:
    cases = load_cases(dataset_path)
    if case_limit > 0:
        cases = cases[:case_limit]

    if not cases:
        raise ValueError("dataset is empty")

    results: list[dict[str, Any]] = []
    failed = 0

    for case in cases:
        payload = case.get("input_payload") or {}
        summary, generation_error = _generate_summary(payload, mode=mode, timeout=timeout)
        evaluated = evaluate_case(case, summary, payload, expect_profile=profile)
        if generation_error:
            evaluated.setdefault("debug", {})["generation_error"] = generation_error
        if not evaluated["passed"]:
            failed += 1

        evaluated["tags"] = case.get("tags") or []
        evaluated["mode"] = mode
        evaluated["summary_preview"] = _truncate(summary, summary_preview_chars)
        results.append(evaluated)

    overall_scores = [float(r.get("scores", {}).get("overall", 0.0) or 0.0) for r in results]
    avg_overall = (sum(overall_scores) / len(overall_scores)) if overall_scores else 0.0

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "dataset_path": str(dataset_path),
        "mode": mode,
        "profile": profile,
        "cases_total": len(results),
        "cases_passed": len(results) - failed,
        "cases_failed": failed,
        "pass_rate": round((len(results) - failed) / max(1, len(results)), 4),
        "overall_avg": round(avg_overall, 4),
        "results": results,
    }


def _print_summary(report: dict[str, Any], only_failures: bool) -> None:
    print("Gemini Summary Eval")
    print(f"- dataset: {report.get('dataset_path')}")
    print(f"- mode: {report.get('mode')}")
    print(f"- profile: {report.get('profile')}")
    print(
        f"- cases: {report.get('cases_passed')}/{report.get('cases_total')} passed "
        f"(pass_rate={report.get('pass_rate'):.2%}, overall_avg={report.get('overall_avg'):.2f})"
    )

    rows = report.get("results") or []
    if only_failures:
        rows = [r for r in rows if not r.get("passed")]

    for row in rows:
        state = "PASS" if row.get("passed") else "FAIL"
        overall = float((row.get("scores") or {}).get("overall") or 0.0)
        print(f"  [{state}] {row.get('case_id')} overall={overall:.2f}")
        if not row.get("passed"):
            for v in row.get("violations") or []:
                print(f"    - {v}")


def _export_langsmith_jsonl(path: str, report: dict[str, Any], dataset_name: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    with target.open("w", encoding="utf-8") as handle:
        for row in report.get("results") or []:
            line = {
                "dataset_name": dataset_name,
                "inputs": {
                    "case_id": row.get("case_id"),
                    "mode": report.get("mode"),
                    "profile": report.get("profile"),
                    "summary_preview": row.get("summary_preview"),
                },
                "outputs": {
                    "scores": row.get("scores") or {},
                    "passed": bool(row.get("passed")),
                    "violations": row.get("violations") or [],
                },
                "metadata": {
                    "tags": row.get("tags") or [],
                    "debug": row.get("debug") or {},
                    "generated_at_utc": report.get("generated_at_utc"),
                },
            }
            handle.write(json.dumps(line, ensure_ascii=True) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Gemini summary eval set")
    parser.add_argument("--dataset", default=str(DEFAULT_DATASET), help="Path to dataset JSON/JSONL")
    parser.add_argument("--mode", choices=["rule_based", "live"], default="rule_based")
    parser.add_argument("--profile", choices=["soft", "strict"], default="soft")
    parser.add_argument("--timeout", type=float, default=8.0, help="Timeout for live mode")
    parser.add_argument("--limit", type=int, default=0, help="Limit case count for quick smoke runs")
    parser.add_argument("--report", default="", help="Write JSON report to this path")
    parser.add_argument("--summary-preview-chars", type=int, default=260)
    parser.add_argument("--only-failures", action="store_true")
    parser.add_argument(
        "--export-langsmith-jsonl",
        default="",
        help="Export LangSmith-import-friendly JSONL (manual import in LangSmith UI)",
    )
    parser.add_argument("--langsmith-dataset", default="gemini-summary-evals-v1")
    args = parser.parse_args()

    report = run_eval(
        dataset_path=args.dataset,
        mode=args.mode,
        timeout=args.timeout,
        case_limit=args.limit,
        summary_preview_chars=max(80, args.summary_preview_chars),
        profile=args.profile,
    )

    out_path = _report_path(args.report)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")

    _print_summary(report, only_failures=bool(args.only_failures))
    print(f"- report: {out_path}")

    if args.export_langsmith_jsonl:
        _export_langsmith_jsonl(args.export_langsmith_jsonl, report, dataset_name=args.langsmith_dataset)
        print(f"- langsmith_jsonl: {args.export_langsmith_jsonl}")

    if report.get("cases_failed", 0) > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
