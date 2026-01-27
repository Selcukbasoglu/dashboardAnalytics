import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
import yaml

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / ".env.local")
load_dotenv(dotenv_path=BASE_DIR / ".env")
load_dotenv(dotenv_path=BASE_DIR / "analytics-py" / ".env.local")
load_dotenv(dotenv_path=BASE_DIR / "analytics-py" / ".env")


@dataclass
class Settings:
    openai_api_key: str | None
    openai_model: str
    enable_openai_summary: bool
    request_timeout: float
    cache_ttl_seconds: int
    redis_url: str
    database_url: str
    retention_days: int
    news_ingest_interval_minutes: int
    impact_half_life_hours: float
    weights: dict
    news_rank_weights: dict
    news_rank_profiles: dict
    news_rank_profile: str | None
    news_rank_profile_auto: bool
    thresholds: dict
    min_hold_minutes: dict
    source_tiers: dict


def _load_yaml_config() -> dict:
    config_path = os.getenv("CONFIG_PATH", str(BASE_DIR / "analytics-py" / "config.yaml"))
    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}
    except FileNotFoundError:
        return {}


def load_settings() -> Settings:
    cfg = _load_yaml_config()
    return Settings(
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-5-mini"),
        enable_openai_summary=os.getenv("ENABLE_OPENAI_SUMMARY", "").lower() in ("1", "true", "yes", "on"),
        request_timeout=float(os.getenv("REQUEST_TIMEOUT", "12")),
        cache_ttl_seconds=int(os.getenv("CACHE_TTL_SECONDS", "30")),
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"),
        database_url=os.getenv("DATABASE_URL", cfg.get("database_url") or "sqlite:///./dev.db"),
        retention_days=int(os.getenv("RETENTION_DAYS", cfg.get("retention_days") or 8)),
        news_ingest_interval_minutes=int(
            os.getenv("NEWS_INGEST_INTERVAL_MINUTES", cfg.get("news_ingest_interval_minutes") or 30)
        ),
        impact_half_life_hours=float(os.getenv("IMPACT_HALF_LIFE_HOURS", cfg.get("impact_half_life_hours") or 12)),
        weights=cfg.get("weights") or {"market": 0.6, "news": 0.4},
        news_rank_weights=cfg.get("news_rank_weights")
        or {"relevance": 0.45, "quality": 0.30, "impact": 0.15, "scope": 0.10},
        news_rank_profiles=cfg.get("news_rank_profiles") or {},
        news_rank_profile=os.getenv("NEWS_RANK_PROFILE"),
        news_rank_profile_auto=os.getenv("NEWS_RANK_PROFILE_AUTO", "true").lower() in ("1", "true", "yes", "on"),
        thresholds=cfg.get("thresholds")
        or {"flip_hysteresis": 0.12, "neutral_band_pct": 0.0015, "min_confidence": 0.35},
        min_hold_minutes=cfg.get("min_hold_minutes")
        or {"15m": 20, "1h": 75, "3h": 200, "6h": 340},
        source_tiers=cfg.get("source_tiers")
        or {
            "primary": [],
            "tier1": [],
            "tier2": [],
            "social": [],
        },
    )
