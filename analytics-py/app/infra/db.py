from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional dependency
    psycopg = None
    dict_row = None


def _sqlite_path(url: str) -> str:
    if url.startswith("sqlite:///"):
        return url.replace("sqlite:///", "", 1)
    if url.startswith("sqlite://"):
        return url.replace("sqlite://", "", 1)
    return url


@dataclass
class DB:
    conn: object
    lock: threading.Lock
    dialect: str

    def _prepare(self, sql: str) -> str:
        if self.dialect == "postgres":
            return sql.replace("?", "%s")
        return sql

    def _row_to_dict(self, row):
        if row is None:
            return None
        if isinstance(row, dict):
            return row
        try:
            return dict(row)
        except Exception:
            return row

    def execute(self, sql: str, params: tuple | dict = ()) -> None:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(self._prepare(sql), params)
            self.conn.commit()

    def executemany(self, sql: str, seq: list[tuple]) -> None:
        if not seq:
            return
        with self.lock:
            cur = self.conn.cursor()
            cur.executemany(self._prepare(sql), seq)
            self.conn.commit()

    def fetchone(self, sql: str, params: tuple | dict = ()):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(self._prepare(sql), params)
            return self._row_to_dict(cur.fetchone())

    def fetchall(self, sql: str, params: tuple | dict = ()):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(self._prepare(sql), params)
            rows = cur.fetchall()
            return [self._row_to_dict(r) for r in rows]

    def insert_ignore(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        if not rows:
            return
        cols = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        if self.dialect == "sqlite":
            sql = f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({placeholders})"
        else:
            sql = f"{sql} ON CONFLICT DO NOTHING"
        self.executemany(sql, rows)

    def upsert(self, table: str, columns: list[str], conflict_cols: list[str], rows: list[tuple]) -> None:
        if not rows:
            return
        cols = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))
        if self.dialect == "sqlite":
            sql = f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})"
            self.executemany(sql, rows)
            return
        conflict = ", ".join(conflict_cols)
        updates = ", ".join([f"{col}=EXCLUDED.{col}" for col in columns if col not in conflict_cols])
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
        self.executemany(sql, rows)


def init_db(database_url: str) -> DB:
    parsed = urlparse(database_url)
    if parsed.scheme in ("", "sqlite"):
        path = _sqlite_path(database_url)
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        db = DB(conn=conn, lock=threading.Lock(), dialect="sqlite")
    elif parsed.scheme in ("postgres", "postgresql"):
        if psycopg is None:
            raise ValueError("psycopg is required for Postgres support.")
        conn = psycopg.connect(database_url, row_factory=dict_row)
        db = DB(conn=conn, lock=threading.Lock(), dialect="postgres")
    else:
        raise ValueError("Unsupported database scheme.")
    _create_schema(db)
    _migrate_impact_scale(db)
    return db


def _create_schema(db: DB) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            ts_utc TEXT NOT NULL,
            source TEXT,
            source_tier TEXT,
            headline TEXT,
            body TEXT,
            url TEXT,
            tags_json TEXT,
            dedup_hash TEXT,
            cluster_id TEXT,
            credibility_score REAL,
            severity_score REAL,
            impact_score REAL,
            event_type TEXT,
            category TEXT,
            direction INTEGER
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS event_asset_map (
            event_id TEXT NOT NULL,
            asset_or_sector TEXT NOT NULL,
            relevance_score REAL,
            PRIMARY KEY (event_id, asset_or_sector)
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS forecasts (
            forecast_id TEXT PRIMARY KEY,
            ts_utc TEXT NOT NULL,
            tf TEXT NOT NULL,
            target TEXT NOT NULL,
            direction TEXT NOT NULL,
            expected_move REAL,
            confidence REAL NOT NULL,
            drivers_json TEXT,
            rationale_text TEXT,
            model_version TEXT,
            created_by TEXT,
            expires_at_utc TEXT NOT NULL
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS forecast_scores (
            forecast_id TEXT PRIMARY KEY,
            realized_return REAL,
            hit INTEGER,
            brier_component REAL,
            scored_at_utc TEXT
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS price_bars (
            asset TEXT NOT NULL,
            ts_utc TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            PRIMARY KEY (asset, ts_utc)
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS event_impact (
            cluster_id TEXT NOT NULL,
            target TEXT NOT NULL,
            tf TEXT NOT NULL,
            realized_ret REAL,
            realized_z REAL,
            computed_at TEXT,
            PRIMARY KEY (cluster_id, target, tf)
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS kv_store (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )


def _migrate_impact_scale(db: DB) -> None:
    try:
        row = db.fetchone("SELECT value FROM kv_store WHERE key = ?", ("impact_scale_migrated",))
        if row and row.get("value") == "1":
            return
        max_row = db.fetchone("SELECT MAX(impact_score) as max_imp FROM events")
        if not max_row or max_row.get("max_imp") is None:
            return
        try:
            max_imp = float(max_row["max_imp"])
        except Exception:
            return
        if max_imp <= 1.5:
            db.execute("UPDATE events SET impact_score = impact_score * 100.0 WHERE impact_score <= 1.5")
        db.upsert("kv_store", ["key", "value"], ["key"], [("impact_scale_migrated", "1")])
    except Exception:
        return


def purge_old(db: DB, retention_days: int) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    cutoff_iso = cutoff.isoformat().replace("+00:00", "Z")
    db.execute("DELETE FROM events WHERE ts_utc < ?", (cutoff_iso,))
    db.execute("DELETE FROM price_bars WHERE ts_utc < ?", (cutoff_iso,))
    db.execute("DELETE FROM forecasts WHERE ts_utc < ?", (cutoff_iso,))
    db.execute("DELETE FROM forecast_scores WHERE scored_at_utc < ?", (cutoff_iso,))
