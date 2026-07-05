"""
pipeline/dedup_days.py — Whole-day replication audit.

Runs BEFORE aggregation writes anything (BUILD_SPEC §4.4).

- Row-level duplicates: handled in clean.py
- Whole-day replication: this module
  1. Build cleaned candles for each copy independently
  2. Compare on (ts, open, high, low, close, volume)
  3. Identical → collapse, log to reports/dedup_log.csv
  4. Different → CONFLICT status, exclude from load, log diff
"""

import csv
import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import polars as pl

log = logging.getLogger(__name__)


def find_duplicate_days(
    day_chunks: List[Tuple[date, pl.DataFrame]],
) -> Dict[date, List[pl.DataFrame]]:
    """
    Group ingested day-chunks by date.
    Returns dict mapping date → list of DataFrames (one per copy).
    Only dates with >1 copy are returned.
    """
    by_date: Dict[date, List[pl.DataFrame]] = {}
    for d, df in day_chunks:
        by_date.setdefault(d, []).append(df)

    # Return only dates with duplicates
    return {d: dfs for d, dfs in by_date.items() if len(dfs) > 1}


def _candle_fingerprint(df: pl.DataFrame) -> pl.DataFrame:
    """
    Build a fingerprint of 1m candles from raw ticks for comparison.
    Uses the same aggregation logic as aggregate.py but independently.
    """
    if df.is_empty() or "ts_karachi" not in df.columns:
        return pl.DataFrame()

    # Truncate to minute
    agg = (
        df.with_columns(
            pl.col("ts_karachi").dt.truncate("1m").alias("minute")
        )
        .group_by(["symbol", "minute"])
        .agg([
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
        ])
        .sort(["symbol", "minute"])
    )

    return agg


def compare_copies(
    copies: List[pl.DataFrame],
) -> Tuple[bool, Optional[pl.DataFrame]]:
    """
    Compare multiple copies of a day's data.

    Returns
    -------
    (identical, diff_df)
    identical : True if all copies produce the same candles
    diff_df   : DataFrame showing disagreements (None if identical)
    """
    fingerprints = [_candle_fingerprint(c) for c in copies]

    # Filter out empty fingerprints
    fingerprints = [f for f in fingerprints if not f.is_empty()]
    if len(fingerprints) <= 1:
        return True, None

    # Compare the first against each subsequent
    base = fingerprints[0]
    for i, other in enumerate(fingerprints[1:], 1):
        # Check schema match
        if base.columns != other.columns:
            log.warning("Copy schemas differ: %s vs %s", base.columns, other.columns)
            return False, other

        # Check row count
        if len(base) != len(other):
            log.info("Copy row counts differ: %d vs %d", len(base), len(other))
            return False, other

        # Compare values
        try:
            joined = base.join(
                other,
                on=["symbol", "minute"],
                how="full",
                suffix="_copy",
            )
            # Check for rows where values differ
            diffs = joined.filter(
                (pl.col("open") != pl.col("open_copy"))
                | (pl.col("high") != pl.col("high_copy"))
                | (pl.col("low") != pl.col("low_copy"))
                | (pl.col("close") != pl.col("close_copy"))
                | (pl.col("volume") != pl.col("volume_copy"))
            )

            if diffs.height > 0:
                return False, diffs
        except Exception as e:
            log.error("Error comparing copies: %s", e)
            return False, None

    return True, None


def audit_duplicates(
    day_chunks: List[Tuple[date, pl.DataFrame]],
    reports_dir: Path,
) -> Tuple[List[Tuple[date, pl.DataFrame]], List[date]]:
    """
    Run the whole-day dedup audit.

    Returns
    -------
    (deduplicated_chunks, conflict_dates)
    deduplicated_chunks : list of (date, df) with duplicates collapsed
    conflict_dates      : list of dates marked as CONFLICT
    """
    dups = find_duplicate_days(day_chunks)

    if not dups:
        log.info("No duplicate days found")
        return day_chunks, []

    log.info("Found %d dates with duplicate ingestions", len(dups))

    dedup_log_path = reports_dir / "dedup_log.csv"
    reports_dir.mkdir(parents=True, exist_ok=True)

    conflict_dates = []
    collapsed_dates = set()

    with open(dedup_log_path, "a", newline="") as f:
        writer = csv.writer(f)
        if f.tell() == 0:
            writer.writerow(["trade_date", "n_copies", "action", "detail"])

        for d, copies in dups.items():
            identical, diff_df = compare_copies(copies)

            if identical:
                writer.writerow([d.isoformat(), len(copies), "collapsed_identical", ""])
                collapsed_dates.add(d)
                log.info("  %s: %d copies → collapsed (identical)", d, len(copies))
            else:
                detail = ""
                if diff_df is not None:
                    detail = f"{diff_df.height} minute(s) disagree"
                writer.writerow([d.isoformat(), len(copies), "CONFLICT", detail])
                conflict_dates.append(d)
                log.warning("  %s: %d copies → CONFLICT (%s)", d, len(copies), detail)

    # Build deduplicated result
    seen_dates = set()
    result = []
    conflict_set = set(conflict_dates)

    for d, df in day_chunks:
        if d in conflict_set:
            continue  # Exclude conflicts
        if d in collapsed_dates:
            if d in seen_dates:
                continue  # Skip duplicate copies
            seen_dates.add(d)
        result.append((d, df))

    log.info(
        "Dedup audit: %d collapsed, %d conflicts, %d chunks remaining",
        len(collapsed_dates), len(conflict_dates), len(result),
    )

    return result, conflict_dates
