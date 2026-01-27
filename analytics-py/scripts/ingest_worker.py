from __future__ import annotations

import logging
import os
import time

from app.config import load_settings
from app.infra.cache import init_cache
from app.infra.db import init_db
from app.models import IntelRequest
from app.services.intel_pipeline import IntelPipelineService


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ingest_worker")


def _parse_watchlist(raw: str) -> list[str]:
    if not raw:
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


def main() -> None:
    settings = load_settings()
    cache = init_cache(settings.redis_url, settings.cache_ttl_seconds)
    db = init_db(settings.database_url)
    pipeline = IntelPipelineService(settings, cache, db)

    interval_s = int(os.getenv("INGEST_INTERVAL_SECONDS", "600") or 600)
    timeframe = os.getenv("INGEST_TIMEFRAME", "1h") or "1h"
    news_span = os.getenv("INGEST_NEWS_TIMESPAN", "6h") or "6h"
    watchlist = _parse_watchlist(os.getenv("INGEST_WATCHLIST", ""))

    logger.info("ingest_worker starting: interval=%ss timeframe=%s newsTimespan=%s watch=%s", interval_s, timeframe, news_span, watchlist)
    while True:
        started = time.time()
        try:
            req = IntelRequest(timeframe=timeframe, newsTimespan=news_span, watchlist=watchlist)
            pipeline.run(req)
            elapsed = time.time() - started
            logger.info("ingest_worker run ok (%.2fs)", elapsed)
        except Exception as exc:
            logger.exception("ingest_worker run failed: %s", exc)
        time.sleep(max(10, interval_s))


if __name__ == "__main__":
    main()
