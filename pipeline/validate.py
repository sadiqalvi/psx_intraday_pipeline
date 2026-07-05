"""
pipeline/validate.py — Candle-level and day-level validation.

Candle-level (every candle):
  - high >= max(open, close) and low <= min(open, close) and high >= low
  - open/high/low/close all > 0
  - volume >= 0
  - ts is exactly on a minute boundary (seconds == 0)
  - Spike guard: abs(close - prev_close)/prev_close > 20% → FLAG

Day-level:
  - populated-minute count within expected band
  - timestamps strictly increasing, no dupes
  - no multi-hour internal hole across liquid symbols → flag
"""

import csv
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import List, Tuple

import polars as pl

from pipeline.config import PipelineConfig

log = logging.getLogger(__name__)


def validate_candles(
    candles: pl.DataFrame,
    trade_date: date,
    config: PipelineConfig,
) -> Tuple[pl.DataFrame, pl.DataFrame, dict]:
    """
    Validate candle data for a single day.

    Returns
    -------
    (valid_candles, quarantined_candles, day_stats)
    """
    if candles.is_empty():
        return candles, pl.DataFrame(), {
            "trade_date": str(trade_date),
            "status": "MISSING",
            "reason": "no_candles",
            "minutes_count": 0,
        }

    checks = []
    quarantine_reasons = []

    # ── Candle-level checks ──────────────────────────────────────────

    # OHLC sanity: high >= max(open, close)
    candles = candles.with_columns([
        (
            (pl.col("high") >= pl.max_horizontal("open", "close"))
            & (pl.col("low") <= pl.min_horizontal("open", "close"))
            & (pl.col("high") >= pl.col("low"))
        ).alias("ohlc_valid"),

        # Positive prices
        (
            (pl.col("open") > 0)
            & (pl.col("high") > 0)
            & (pl.col("low") > 0)
            & (pl.col("close") > 0)
        ).alias("price_valid"),

        # Non-negative volume
        (pl.col("volume") >= 0).alias("volume_valid"),
    ])

    # Minute boundary check (seconds component should be 0)
    candles = candles.with_columns(
        (pl.col("ts").dt.second() == 0).alias("minute_boundary")
    )

    # Combined validity
    candles = candles.with_columns(
        (
            pl.col("ohlc_valid")
            & pl.col("price_valid")
            & pl.col("volume_valid")
            & pl.col("minute_boundary")
        ).alias("is_valid")
    )

    # Spike guard: >20% move from previous close
    spike_pct = config.thresholds.spike_pct
    candles = candles.sort(["symbol", "ts"])

    candles = candles.with_columns(
        pl.col("close")
          .shift(1)
          .over("symbol")
          .alias("prev_close")
    )

    candles = candles.with_columns(
        pl.when(pl.col("prev_close").is_not_null() & (pl.col("prev_close") > 0))
        .then(
            ((pl.col("close") - pl.col("prev_close")).abs() / pl.col("prev_close")) > spike_pct
        )
        .otherwise(pl.lit(False))
        .alias("spike_flag")
    )

    # Separate quarantined candles (invalid but NOT spikes — spikes are flagged only)
    quarantined = candles.filter(~pl.col("is_valid"))
    valid = candles.filter(pl.col("is_valid"))

    n_ohlc_fail = candles.filter(~pl.col("ohlc_valid")).height
    n_price_fail = candles.filter(~pl.col("price_valid")).height
    n_vol_fail = candles.filter(~pl.col("volume_valid")).height
    n_boundary_fail = candles.filter(~pl.col("minute_boundary")).height
    n_spikes = candles.filter(pl.col("spike_flag")).height

    # Clean up helper columns from valid candles
    drop_cols = ["ohlc_valid", "price_valid", "volume_valid", "minute_boundary",
                 "is_valid", "prev_close"]
    valid = valid.drop([c for c in drop_cols if c in valid.columns])
    quarantined_out = quarantined.drop([c for c in drop_cols if c in quarantined.columns])

    # ── Day-level checks ─────────────────────────────────────────────

    n_symbols = valid.select("symbol").n_unique() if not valid.is_empty() else 0
    n_minutes = valid.height

    # Check populated minutes vs threshold
    min_minutes = config.thresholds.min_session_minutes
    status = "COMPLETE"
    reason = None

    if n_minutes == 0:
        status = "MISSING"
        reason = "no_valid_candles"
    elif n_minutes < min_minutes:
        # Check if there's enough data across the market
        # Use a per-symbol check: at least some symbols should have good coverage
        symbol_counts = valid.group_by("symbol").agg(pl.count().alias("cnt"))
        if symbol_counts.is_empty() or symbol_counts["cnt"].max() < 10:
            status = "MISSING"
            reason = f"insufficient_minutes ({n_minutes} < {min_minutes})"

    # Check for duplicate timestamps within a symbol
    if not valid.is_empty():
        dup_check = (
            valid.group_by(["symbol", "ts"])
            .agg(pl.count().alias("cnt"))
            .filter(pl.col("cnt") > 1)
        )
        if dup_check.height > 0:
            log.warning("%s: %d duplicate (symbol, ts) pairs found", trade_date, dup_check.height)

    # Check for multi-hour gaps
    gap_flag = False
    if not valid.is_empty() and n_symbols > 0:
        # Check the most liquid symbol for internal gaps
        symbol_counts = valid.group_by("symbol").agg(pl.count().alias("cnt")).sort("cnt", descending=True)
        top_symbol = symbol_counts["symbol"][0]
        top_data = valid.filter(pl.col("symbol") == top_symbol).sort("ts")

        if top_data.height > 1:
            gaps = top_data.with_columns(
                (pl.col("ts").diff()).alias("gap")
            ).filter(
                pl.col("gap") > timedelta(hours=2)
            )
            if gaps.height > 0:
                gap_flag = True
                log.warning("%s: multi-hour gap detected in %s", trade_date, top_symbol)

    day_stats = {
        "trade_date": str(trade_date),
        "status": status,
        "reason": reason,
        "minutes_count": n_minutes,
        "symbols_count": n_symbols,
        "quarantined": quarantined.height,
        "spikes_flagged": n_spikes,
        "ohlc_failures": n_ohlc_fail,
        "price_failures": n_price_fail,
        "volume_failures": n_vol_fail,
        "boundary_failures": n_boundary_fail,
        "gap_flag": gap_flag,
    }

    log.info(
        "Validated %s: %s — %d candles, %d quarantined, %d spikes, gap=%s",
        trade_date, status, n_minutes, quarantined.height, n_spikes, gap_flag,
    )

    return valid, quarantined_out, day_stats


def write_validation_report(
    day_stats: dict,
    reports_dir: Path,
):
    """Append validation results to reports/validation_report.csv."""
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / "validation_report.csv"

    write_header = not report_path.exists()

    with open(report_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=day_stats.keys())
        if write_header:
            writer.writeheader()
        writer.writerow(day_stats)


def write_quarantine(
    quarantined: pl.DataFrame,
    trade_date: date,
    reports_dir: Path,
):
    """Append quarantined candles to reports/quarantine.csv."""
    if quarantined.is_empty():
        return

    reports_dir.mkdir(parents=True, exist_ok=True)
    quarantine_path = reports_dir / "quarantine.csv"

    # Add trade_date column
    q = quarantined.with_columns(
        pl.lit(str(trade_date)).alias("quarantine_date")
    )

    if quarantine_path.exists():
        q.write_csv(str(quarantine_path), include_header=False)
    else:
        q.write_csv(str(quarantine_path))

    log.info("Quarantined %d candles for %s", quarantined.height, trade_date)
