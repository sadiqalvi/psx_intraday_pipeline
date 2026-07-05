"""
pipeline/ingest.py — Read raw data (SQLite DBs or CSVs), stream, archive to Parquet.

Memory-safe: uses Polars lazy frames and processes per (symbol, trade_date) group.
"""

import logging
import sqlite3
from datetime import date
from pathlib import Path
from typing import List, Tuple, Optional

import polars as pl

from pipeline.config import PipelineConfig
from pipeline.timezone import normalize_timestamps_polars

log = logging.getLogger(__name__)


def discover_source_files(config: PipelineConfig) -> List[Path]:
    """
    Find all raw data files in LOCAL_HISTORICAL_DIR.
    Returns sorted list of paths.
    """
    src_dir = config.local_historical_dir
    if not src_dir.exists():
        log.error("LOCAL_HISTORICAL_DIR does not exist: %s", src_dir)
        return []

    if config.source_format == "sqlite":
        files = sorted(src_dir.glob("*.db"))
    else:
        files = sorted(src_dir.glob(config.raw_file_glob))

    log.info("Discovered %d source files in %s", len(files), src_dir)
    return files


def extract_date_from_filename(path: Path) -> Optional[date]:
    """
    Extract a date from a filename like psx_intraday_20260619.db.
    Returns None if no date can be parsed.
    """
    stem = path.stem  # e.g. "psx_intraday_20260619"
    # Try to find an 8-digit date at the end
    digits = "".join(c for c in stem if c.isdigit())
    if len(digits) >= 8:
        try:
            return date(int(digits[-8:-4]), int(digits[-4:-2]), int(digits[-2:]))
        except ValueError:
            pass
    return None


def read_sqlite_file(
    path: Path,
    config: PipelineConfig,
) -> pl.LazyFrame:
    """
    Read a single SQLite .db file into a Polars LazyFrame.
    Applies column mapping from config.
    """
    log.debug("Reading SQLite file: %s", path)

    conn = sqlite3.connect(str(path))
    try:
        # Read all data from the intraday_data table
        df = pl.read_database(
            "SELECT symbol, time, price, volume FROM intraday_data",
            connection=conn,
        )
    except Exception as e:
        log.error("Failed to read %s: %s", path, e)
        conn.close()
        return pl.LazyFrame()
    finally:
        conn.close()

    if df.is_empty():
        return df.lazy()

    # Rename columns to canonical names
    col_map = {
        config.columns.symbol: "symbol",
        config.columns.timestamp: "ts_raw",
        config.columns.price: "price",
        config.columns.volume: "volume",
    }
    df = df.rename({k: v for k, v in col_map.items() if k in df.columns and k != v})

    return df.lazy()


def read_csv_file(
    path: Path,
    config: PipelineConfig,
) -> pl.LazyFrame:
    """
    Read a single CSV file as a Polars LazyFrame (streaming).
    """
    log.debug("Reading CSV file: %s", path)
    lf = pl.scan_csv(
        str(path),
        schema_overrides={"price": pl.Float64, "volume": pl.Int64},
    )

    col_map = {
        config.columns.symbol: "symbol",
        config.columns.timestamp: "ts_raw",
        config.columns.price: "price",
        config.columns.volume: "volume",
    }
    lf = lf.rename({k: v for k, v in col_map.items() if k != v})

    return lf


def ingest_file(
    path: Path,
    config: PipelineConfig,
) -> pl.LazyFrame:
    """
    Ingest a single raw file (SQLite or CSV), normalize timestamps.
    Returns a LazyFrame with columns: symbol, ts_raw (UTC), ts_karachi, price, volume.
    """
    if config.source_format == "sqlite":
        lf = read_sqlite_file(path, config)
    else:
        lf = read_csv_file(path, config)

    if lf.collect().is_empty():
        return lf

    # Normalize timestamps
    lf = normalize_timestamps_polars(lf, "ts_raw", config.timestamp_format)

    return lf


def archive_to_parquet(
    df: pl.DataFrame,
    trade_date: date,
    archive_dir: Path,
):
    """
    Write an archival Parquet copy of the raw ticks, compressed with zstd.
    """
    archive_dir.mkdir(parents=True, exist_ok=True)
    out_path = archive_dir / f"{trade_date.isoformat()}.parquet"
    df.write_parquet(str(out_path), compression="zstd")
    log.info("Archived raw ticks to %s (%d rows)", out_path, len(df))


def ingest_all(
    config: PipelineConfig,
    target_dates: Optional[List[date]] = None,
) -> List[Tuple[date, pl.DataFrame]]:
    """
    Ingest all source files, return list of (trade_date, DataFrame) tuples.
    Each DataFrame has columns: symbol, ts_raw (UTC), ts_karachi, price, volume.

    If target_dates is provided, only process files matching those dates.
    """
    files = discover_source_files(config)
    results = []

    for path in files:
        file_date = extract_date_from_filename(path)
        if target_dates and file_date and file_date not in target_dates:
            continue

        log.info("Ingesting %s (date=%s)", path.name, file_date)
        lf = ingest_file(path, config)
        df = lf.collect()

        if df.is_empty():
            log.warning("Empty data from %s", path.name)
            continue

        # Determine actual trade dates from data
        if "ts_karachi" in df.columns:
            dates_in_data = (
                df.select(pl.col("ts_karachi").cast(pl.Date).alias("trade_date"))
                  .unique()
                  .to_series()
                  .to_list()
            )
        elif file_date:
            dates_in_data = [file_date]
        else:
            log.warning("Cannot determine date for %s — skipping", path.name)
            continue

        # Group by trade_date and process each
        for td in dates_in_data:
            if target_dates and td not in target_dates:
                continue

            if "ts_karachi" in df.columns:
                day_df = df.filter(
                    pl.col("ts_karachi").cast(pl.Date) == td
                )
            else:
                day_df = df

            if day_df.is_empty():
                continue

            # Archive raw ticks
            archive_to_parquet(day_df, td, config.archive_dir)

            results.append((td, day_df))
            log.info("  → %s: %d ticks", td, len(day_df))

    log.info("Ingested %d day-chunks from %d files", len(results), len(files))
    return results
