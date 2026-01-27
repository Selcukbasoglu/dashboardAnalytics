from __future__ import annotations

import hashlib
import json
from typing import Any


def stable_json_dumps(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def hash_block(payload: Any) -> str:
    raw = stable_json_dumps(payload)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def provider_metrics_summary(results: list[dict]) -> str:
    parts: list[str] = []
    for res in results:
        source = res.get("source", "unknown")
        ok = res.get("ok", False)
        latency = res.get("latency_ms", 0)
        cache_hit = res.get("cache_hit", False)
        degraded = res.get("degraded_mode", False)
        error_code = res.get("error_code")
        status = "ok" if ok else "err"
        seg = f"{source} {status} p95={latency}ms hit={1 if cache_hit else 0}"
        if degraded:
            seg += " degraded"
        if error_code:
            seg += f" {error_code}"
        parts.append(seg)
    return " | ".join(parts)
