"""
pipeline/db.py — Database operations (PostgreSQL and SQLite backends).

Schema DDL, idempotent upsert, day_status writes.
"""

import logging
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import polars as pl

from pipeline.config import PipelineConfig

log = logging.getLogger(__name__)


# ── SQLite Backend ──────────────────────────────────────────────────────

class SQLiteBackend:
    """SQLite database backend for local development and testing."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path), timeout=60.0)
        self._create_schema()

    def _create_schema(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS candles_1m (
                symbol      TEXT    NOT NULL,
                ts          TEXT    NOT NULL,
                open        REAL    NOT NULL,
                high        REAL    NOT NULL,
                low         REAL    NOT NULL,
                close       REAL    NOT NULL,
                volume      INTEGER NOT NULL DEFAULT 0,
                had_trade   INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (symbol, ts)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_candles_symbol_ts
            ON candles_1m (symbol, ts)
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS day_status (
                symbol        TEXT NOT NULL,
                trade_date    TEXT NOT NULL,
                status        TEXT NOT NULL,
                reason        TEXT,
                minutes_count INTEGER,
                first_ts      TEXT,
                last_ts       TEXT,
                built_at      TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (symbol, trade_date)
            )
        """)
        self.conn.commit()
        log.info("SQLite schema initialized at %s", self.db_path)

    def upsert_candles(self, candles: pl.DataFrame):
        """Upsert candles into the database."""
        if candles.is_empty():
            return

        cursor = self.conn.cursor()

        # Select only the columns we need
        cols = ["symbol", "ts", "open", "high", "low", "close", "volume", "had_trade"]
        df = candles.select([c for c in cols if c in candles.columns])

        # Convert ts to ISO string for SQLite
        if "ts" in df.columns:
            df = df.with_columns(
                pl.col("ts").cast(pl.Utf8).alias("ts")
            )

        rows = df.to_pandas().values.tolist()

        cursor.executemany("""
            INSERT INTO candles_1m (symbol, ts, open, high, low, close, volume, had_trade)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, ts) DO UPDATE SET
                open=excluded.open, high=excluded.high, low=excluded.low,
                close=excluded.close, volume=excluded.volume, had_trade=excluded.had_trade
        """, rows)

        self.conn.commit()
        log.info("Upserted %d candles", len(rows))

    def upsert_day_status(
        self,
        symbol: str,
        trade_date: date,
        status: str,
        reason: Optional[str] = None,
        minutes_count: int = 0,
        first_ts: Optional[str] = None,
        last_ts: Optional[str] = None,
    ):
        """Upsert a day_status row."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO day_status (symbol, trade_date, status, reason, minutes_count, first_ts, last_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, trade_date) DO UPDATE SET
                status=excluded.status, reason=excluded.reason,
                minutes_count=excluded.minutes_count,
                first_ts=excluded.first_ts, last_ts=excluded.last_ts,
                built_at=datetime('now')
        """, (symbol, trade_date.isoformat(), status, reason, minutes_count, first_ts, last_ts))
        self.conn.commit()

    def get_candle_count(self) -> int:
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM candles_1m")
        return cursor.fetchone()[0]

    def get_day_status_summary(self) -> dict:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT status, COUNT(DISTINCT trade_date) as cnt
            FROM day_status
            GROUP BY status
        """)
        return {row[0]: row[1] for row in cursor.fetchall()}

    def batch_upsert_day_statuses(self, entries):
        """Batch upsert day_status rows. entries = list of (symbol, trade_date, status, reason)."""
        cursor = self.conn.cursor()
        rows = [(s, d.isoformat() if hasattr(d, 'isoformat') else str(d), st, r) for s, d, st, r in entries]
        cursor.executemany("""
            INSERT INTO day_status (symbol, trade_date, status, reason, minutes_count)
            VALUES (?, ?, ?, ?, 0)
            ON CONFLICT (symbol, trade_date) DO UPDATE SET
                status=excluded.status, reason=excluded.reason,
                built_at=datetime('now')
        """, rows)
        self.conn.commit()
        log.info("Batch upserted %d day_status entries", len(rows))

    def close(self):
        self.conn.close()


# ── PostgreSQL Backend ──────────────────────────────────────────────────

class PostgreSQLBackend:
    """PostgreSQL database backend for production."""

    def __init__(self, dsn: str):
        try:
            import psycopg
            self.conn = psycopg.connect(dsn)
        except ImportError:
            raise ImportError(
                "psycopg is required for PostgreSQL backend. "
                "Install it with: pip install 'psycopg[binary]'"
            )
        self._create_schema()

    def _create_schema(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS candles_1m (
                symbol      TEXT          NOT NULL,
                ts          TIMESTAMPTZ   NOT NULL,
                open        NUMERIC(18,6) NOT NULL,
                high        NUMERIC(18,6) NOT NULL,
                low         NUMERIC(18,6) NOT NULL,
                close       NUMERIC(18,6) NOT NULL,
                volume      BIGINT        NOT NULL DEFAULT 0,
                had_trade   BOOLEAN       NOT NULL DEFAULT TRUE,
                PRIMARY KEY (symbol, ts)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_candles_symbol_ts
            ON candles_1m (symbol, ts)
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS day_status (
                symbol        TEXT NOT NULL,
                trade_date    DATE NOT NULL,
                status        TEXT NOT NULL,
                reason        TEXT,
                minutes_count INTEGER,
                first_ts      TIMESTAMPTZ,
                last_ts       TIMESTAMPTZ,
                built_at      TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (symbol, trade_date)
            )
        """)
        self.conn.commit()
        log.info("PostgreSQL schema initialized")

    def upsert_candles(self, candles: pl.DataFrame):
        """
        Bulk-upsert candles into PostgreSQL using COPY + temp table.

        Strategy (avoids N round-trips through the SSH tunnel):
          1. COPY all rows into a temporary table (1 network round-trip).
          2. INSERT ... ON CONFLICT DO UPDATE from temp → real table (1 round-trip).
        """
        if candles.is_empty():
            return

        cols = ["symbol", "ts", "open", "high", "low", "close", "volume", "had_trade"]
        df = candles.select([c for c in cols if c in candles.columns])

        # Ensure ts is a plain string so COPY treats it as text
        df = df.with_columns(pl.col("ts").cast(pl.Utf8))

        cursor = self.conn.cursor()

        # Temp table matches the real table's structure (no PK needed here)
        cursor.execute("""
            CREATE TEMP TABLE _candles_staging (
                symbol    TEXT,
                ts        TEXT,
                open      DOUBLE PRECISION,
                high      DOUBLE PRECISION,
                low       DOUBLE PRECISION,
                close     DOUBLE PRECISION,
                volume    BIGINT,
                had_trade BOOLEAN
            ) ON COMMIT DROP
        """)

        # Stream rows via COPY (single round-trip regardless of row count)
        with cursor.copy(
            "COPY _candles_staging (symbol, ts, open, high, low, close, volume, had_trade) FROM STDIN"
        ) as copy:
            for row in df.iter_rows():
                copy.row(*row)

        # Upsert from staging → real table (one SQL statement)
        cursor.execute("""
            INSERT INTO candles_1m (symbol, ts, open, high, low, close, volume, had_trade)
            SELECT symbol, ts::timestamptz, open, high, low, close, volume, had_trade
            FROM _candles_staging
            ON CONFLICT (symbol, ts) DO UPDATE SET
                open      = EXCLUDED.open,
                high      = EXCLUDED.high,
                low       = EXCLUDED.low,
                close     = EXCLUDED.close,
                volume    = EXCLUDED.volume,
                had_trade = EXCLUDED.had_trade
        """)

        self.conn.commit()
        log.info("Bulk-upserted %d candles to PostgreSQL (COPY+temp)", df.height)

    def upsert_day_status(
        self,
        symbol: str,
        trade_date: date,
        status: str,
        reason: Optional[str] = None,
        minutes_count: int = 0,
        first_ts: Optional[str] = None,
        last_ts: Optional[str] = None,
    ):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO day_status (symbol, trade_date, status, reason, minutes_count, first_ts, last_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, trade_date) DO UPDATE SET
                status=EXCLUDED.status, reason=EXCLUDED.reason,
                minutes_count=EXCLUDED.minutes_count,
                first_ts=EXCLUDED.first_ts, last_ts=EXCLUDED.last_ts,
                built_at=now()
        """, (symbol, trade_date, status, reason, minutes_count, first_ts, last_ts))
        self.conn.commit()

    def get_candle_count(self) -> int:
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM candles_1m")
        return cursor.fetchone()[0]

    def get_day_status_summary(self) -> dict:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT status, COUNT(DISTINCT trade_date) as cnt
            FROM day_status
            GROUP BY status
        """)
        return {row[0]: row[1] for row in cursor.fetchall()}

    def batch_upsert_day_statuses(self, entries):
        """Batch upsert day_status rows. entries = list of (symbol, trade_date, status, reason)."""
        cursor = self.conn.cursor()
        # executemany sends all rows in a single prepared-statement batch
        cursor.executemany("""
            INSERT INTO day_status (symbol, trade_date, status, reason, minutes_count)
            VALUES (%s, %s, %s, %s, 0)
            ON CONFLICT (symbol, trade_date) DO UPDATE SET
                status=EXCLUDED.status, reason=EXCLUDED.reason,
                built_at=now()
        """, [(s, d, st, r) for s, d, st, r in entries])
        self.conn.commit()
        log.info("Batch upserted %d day_status entries", len(entries))

    def close(self):
        self.conn.close()


# ── Factory ─────────────────────────────────────────────────────────────

def create_backend(config: PipelineConfig):
    """Create the appropriate database backend based on config."""
    if config.db_backend == "postgresql":
        if not config.pg_dsn:
            raise ValueError("PG_DSN environment variable required for PostgreSQL backend")
        return PostgreSQLBackend(config.pg_dsn)
    else:
        return SQLiteBackend(config.sqlite_path)
