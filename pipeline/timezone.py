"""
pipeline/timezone.py — Timezone handling for PSX pipeline.

Asia/Karachi is UTC+5 with no DST.  Still use the named zone so any future
government change is picked up automatically.

Key rules (from BUILD_SPEC §4.2):
  - epoch → UTC → tz_convert("Asia/Karachi")
  - naive datetime string with NO offset → depends on config:
      * "naive_utc"   → localize as UTC, convert to Karachi
      * "naive_local" → localize as Asia/Karachi directly
  - explicit offset → convert to Asia/Karachi and assert +05:00
  - Bucketing is in Karachi local time
  - Storage is UTC (TIMESTAMPTZ)
"""

import logging
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import polars as pl

log = logging.getLogger(__name__)

KARACHI = ZoneInfo("Asia/Karachi")
UTC = ZoneInfo("UTC")

# ── helpers ──────────────────────────────────────────────────────────────

def to_karachi(dt: datetime) -> datetime:
    """Convert any aware datetime to Asia/Karachi."""
    return dt.astimezone(KARACHI)


def to_utc(dt: datetime) -> datetime:
    """Convert any aware datetime to UTC."""
    return dt.astimezone(UTC)


# ── Polars column-level transforms ──────────────────────────────────────

def normalize_timestamps_polars(
    lf: pl.LazyFrame,
    ts_col: str,
    fmt: str,
) -> pl.LazyFrame:
    """
    Normalize a timestamp column in a Polars LazyFrame.

    Parameters
    ----------
    lf : pl.LazyFrame
    ts_col : str   — name of the column holding the raw timestamp
    fmt : str      — one of: epoch_s, epoch_ms, naive_utc, naive_local, iso, auto

    Returns
    -------
    LazyFrame with `ts_col` replaced by a tz-aware Datetime(UTC) column,
    plus a new column `ts_karachi` (Datetime Asia/Karachi) for bucketing.
    """
    if fmt == "auto":
        fmt = _auto_detect_format(lf, ts_col)
        log.info("Auto-detected timestamp format: %s", fmt)

    if fmt == "epoch_s":
        lf = lf.with_columns(
            pl.from_epoch(pl.col(ts_col).cast(pl.Int64), time_unit="s")
              .alias(ts_col)
        )
    elif fmt == "epoch_ms":
        lf = lf.with_columns(
            pl.from_epoch(pl.col(ts_col).cast(pl.Int64), time_unit="ms")
              .alias(ts_col)
        )
    elif fmt == "naive_utc":
        # Already a datetime string in UTC — parse then stamp as UTC
        lf = lf.with_columns(
            pl.col(ts_col)
              .cast(pl.Utf8)
              .str.to_datetime(format="%Y-%m-%d %H:%M:%S", strict=False)
              .dt.replace_time_zone("UTC")
              .alias(ts_col)
        )
    elif fmt == "naive_local":
        # Already a datetime string in Karachi wall-clock
        lf = lf.with_columns(
            pl.col(ts_col)
              .cast(pl.Utf8)
              .str.to_datetime(format="%Y-%m-%d %H:%M:%S", strict=False)
              .dt.replace_time_zone("Asia/Karachi")
              .alias(ts_col)
        )
    elif fmt == "iso":
        # ISO 8601 with offset
        lf = lf.with_columns(
            pl.col(ts_col)
              .cast(pl.Utf8)
              .str.to_datetime(strict=False)
              .dt.convert_time_zone("UTC")
              .alias(ts_col)
        )
    else:
        raise ValueError(f"Unknown timestamp_format: {fmt!r}")

    # Ensure column is UTC if not already
    lf = lf.with_columns(
        pl.col(ts_col).dt.convert_time_zone("UTC").alias(ts_col)
    )

    # Add Karachi column for bucketing
    lf = lf.with_columns(
        pl.col(ts_col).dt.convert_time_zone("Asia/Karachi").alias("ts_karachi")
    )

    return lf


def _auto_detect_format(lf: pl.LazyFrame, ts_col: str) -> str:
    """
    Peek at the first non-null value to guess the format.
    """
    sample = lf.select(pl.col(ts_col)).head(5).collect()
    if sample.is_empty():
        return "naive_utc"

    val = sample[ts_col][0]
    if val is None:
        return "naive_utc"

    # Numeric? → epoch
    if isinstance(val, (int, float)):
        if val > 1e12:
            return "epoch_ms"
        return "epoch_s"

    s = str(val)
    if "+" in s or "Z" in s:
        return "iso"
    if "T" in s:
        return "iso"
    # Plain datetime string — assume UTC (our data confirmed this)
    return "naive_utc"


# ── Startup self-test ───────────────────────────────────────────────────

def run_self_test():
    """
    Assert that a known epoch maps to the expected Karachi wall-clock.
    Call this at pipeline startup; fail loudly if wrong.
    """
    # 2026-06-19 04:17:00 UTC  →  2026-06-19 09:17:00 PKT
    epoch = 1781842620  # 2026-06-19 04:17:00 UTC
    dt_utc = datetime.fromtimestamp(epoch, tz=timezone.utc)
    dt_kar = to_karachi(dt_utc)

    expected_hour = 9
    expected_minute = 17
    assert dt_kar.hour == expected_hour and dt_kar.minute == expected_minute, (
        f"Timezone self-test FAILED: epoch {epoch} → "
        f"Karachi {dt_kar.strftime('%H:%M')} "
        f"(expected {expected_hour:02d}:{expected_minute:02d})"
    )

    # Also verify offset is +05:00
    offset = dt_kar.utcoffset()
    assert offset == timedelta(hours=5), (
        f"Timezone self-test FAILED: Karachi offset = {offset} (expected +05:00)"
    )

    log.info("[OK] Timezone self-test passed: epoch %d -> Karachi %s", epoch, dt_kar)
    print(f"[OK] Timezone self-test passed: epoch {epoch} -> Karachi {dt_kar}")
