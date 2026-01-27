#!/usr/bin/env python3
import json
import sys
import urllib.request

BACKEND_URL = "http://localhost:8080/api/v1/health"
ANALYTICS_URL = "http://localhost:8001/health"

ENV_KEYS = [
    "FINNHUB_API_KEY",
    "TWELVEDATA_API_KEY",
    "OPENAI_API_KEY",
    "OPENAI_MODEL",
    "ENABLE_OPENAI_SUMMARY",
    "PY_INTEL_BASE_URL",
    "DATABASE_URL",
    "NEXT_PUBLIC_API_BASE",
]

def fetch_json(url: str):
    with urllib.request.urlopen(url, timeout=6) as f:
        return json.loads(f.read().decode("utf-8"))

def validate(resp, label: str):
    env = resp.get("env") or {}
    features = resp.get("features") or {}
    missing = [k for k in ENV_KEYS if k not in env]
    if missing:
        raise AssertionError(f"{label}: missing env keys in health response: {missing}")
    if "openai_summaries_enabled" in features:
        openai_expected = bool(env.get("ENABLE_OPENAI_SUMMARY")) and bool(env.get("OPENAI_API_KEY")) and bool(env.get("OPENAI_MODEL"))
        if features.get("openai_summaries_enabled") != openai_expected:
            raise AssertionError(f"{label}: openai_summaries_enabled mismatch (expected {openai_expected})")


def main():
    try:
        backend = fetch_json(BACKEND_URL)
        validate(backend, "backend")
    except Exception as exc:
        print(f"backend health check failed: {exc}")
        sys.exit(1)

    try:
        analytics = fetch_json(ANALYTICS_URL)
        validate(analytics, "analytics")
    except Exception as exc:
        print(f"analytics health check failed: {exc}")
        sys.exit(1)

    print("health smoke test ok")

if __name__ == "__main__":
    main()
