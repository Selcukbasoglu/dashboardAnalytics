from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from app.config import load_settings
from app.infra.db import init_db


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def seed_prices(db):
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=48)
    ts = start
    price = 45000.0
    rows = []
    while ts <= now:
        drift = random.uniform(-0.002, 0.002)
        price *= 1 + drift
        high = price * (1 + random.uniform(0.0, 0.0015))
        low = price * (1 - random.uniform(0.0, 0.0015))
        close = price
        rows.append(("BTC", _iso(ts), price, high, low, close, random.uniform(1200, 5200)))
        ts += timedelta(minutes=15)

    db.executemany(
        """
        INSERT OR REPLACE INTO price_bars (asset, ts_utc, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def seed_events(db):
    now = datetime.now(timezone.utc)
    rows = []
    asset_rows = []
    for i in range(20):
        ts = now - timedelta(hours=random.uniform(1, 48))
        cluster_id = f"cluster_{i:02d}"
        event_id = f"event_{i:02d}"
        impact = round(random.uniform(0.3, 0.9), 4)
        credibility = round(random.uniform(0.6, 0.95), 3)
        severity = round(random.uniform(0.4, 0.9), 3)
        direction = random.choice([-1, 1])
        rows.append(
            (
                event_id,
                _iso(ts),
                "reuters.com",
                "tier1",
                f"Mock headline {i}",
                "Mock body text",
                "https://example.com/mock",
                '["Macro","ETF"]',
                f"dedup_{i}",
                cluster_id,
                credibility,
                severity,
                impact,
                "MACRO",
                "kripto",
                direction,
            )
        )
        asset_rows.append((event_id, "BTC", 0.8))
        asset_rows.append((event_id, "ALTS", 0.5))

    db.executemany(
        """
        INSERT OR REPLACE INTO events (
            event_id, ts_utc, source, source_tier, headline, body, url, tags_json,
            dedup_hash, cluster_id, credibility_score, severity_score, impact_score,
            event_type, category, direction
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    db.executemany(
        """
        INSERT OR REPLACE INTO event_asset_map (event_id, asset_or_sector, relevance_score)
        VALUES (?, ?, ?)
        """,
        asset_rows,
    )


def main():
    random.seed(42)
    settings = load_settings()
    db = init_db(settings.database_url)
    seed_prices(db)
    seed_events(db)
    print("seeded mock data")


if __name__ == "__main__":
    main()
