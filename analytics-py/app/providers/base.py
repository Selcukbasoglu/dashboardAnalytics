from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar


T = TypeVar("T")


@dataclass
class ProviderResult(Generic[T]):
    ok: bool
    source: str
    data: T | None
    latency_ms: int
    cache_hit: bool
    error_code: str | None
    error_msg: str | None
    degraded_mode: bool = False
    last_good_age_s: int | None = None

    def debug_view(self) -> dict:
        return {
            "source": self.source,
            "ok": self.ok,
            "latency_ms": self.latency_ms,
            "cache_hit": self.cache_hit,
            "error_code": self.error_code or "",
            "error_msg": self.error_msg or "",
            "degraded_mode": self.degraded_mode,
            "last_good_age_s": self.last_good_age_s or 0,
        }
