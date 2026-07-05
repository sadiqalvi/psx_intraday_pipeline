"""
pipeline/clean.py — Clean raw ticks.

Order of operations (BUILD_SPEC §4.5):
  1. Type coercion: price→float, volume→int, timestamp→tz-aware datetime
  2. Drop invalid rows: price ≤ 0, volume < 0, null timestamp
  3. De-duplicate exact-duplicate trades (symbol, ts, price, volume)
  4. Filter to session hours (09:30–15:30 PKT, configurable)
  5. Sort by timestamp within each (symbol, day)
  6. Outlier flag: mark trades with >20% move vs prior trade's price
"""

import logging
from datetime import date, time, datetime
from typing import Tuple

import polars as pl

from pipeline.config import PipelineConfig

log = logging.getLogger(__name__)


def get_session_window(config: PipelineConfig, trade_date: date) -> Tuple[time, time]:
    """
    Get the session start/end for a given date, respecting overrides.
    """
    for ov in config.session.overrides:
        if ov.from_date <= trade_date <= ov.to_date:
            return ov.start, ov.end
    return config.session.start, config.session.end


def clean_ticks(
    df: pl.DataFrame,
    trade_date: date,
    config: PipelineConfig,
) -> Tuple[pl.DataFrame, dict]:
    """
    Clean raw tick data for a single day.

    Parameters
    ----------
    df : pl.DataFrame with columns: symbol, ts_raw (UTC datetime), ts_karachi, price, volume
    trade_date : the trading date
    config : PipelineConfig

    Returns
    -------
    (cleaned_df, stats) where stats is a dict of cleaning metrics.
    """
    stats = {"input_rows": len(df), "trade_date": str(trade_date)}

    if df.is_empty():
        stats["output_rows"] = 0
        return df, stats

    # 1. Type coercion
    df = df.with_columns([
        pl.col("price").cast(pl.Float64, strict=False),
        pl.col("volume").cast(pl.Int64, strict=False),
    ])

    # Count nulls introduced by coercion
    coerce_nulls = df.filter(
        pl.col("price").is_null() | pl.col("volume").is_null() | pl.col("ts_raw").is_null()
    ).height
    stats["coercion_failures"] = coerce_nulls

    # 2. Drop invalid rows
    before = len(df)
    df = df.filter(
        pl.col("price").is_not_null()
        & pl.col("volume").is_not_null()
        & pl.col("ts_raw").is_not_null()
        & (pl.col("price") > 0)
        & (pl.col("volume") >= 0)
    )
    stats["invalid_dropped"] = before - len(df)

    if df.is_empty():
        stats["output_rows"] = 0
        return df, stats

    # 3. De-duplicate exact-duplicate trades
    before = len(df)
    df = df.unique(subset=["symbol", "ts_raw", "price", "volume"])
    stats["duplicates_dropped"] = before - len(df)

    # 4. Filter to session hours (using Karachi time)
    df = df.with_columns(
        pl.col("ts_karachi").dt.time().alias("trade_time")
    )
    
    before = len(df)
    is_friday = trade_date.weekday() == 4

    if is_friday and config.session.friday:
        # Friday split session
        condition = pl.lit(False)
        for window in config.session.friday:
            w_start = pl.time(window.start.hour, window.start.minute)
            w_end = pl.time(window.end.hour, window.end.minute)
            condition = condition | (
                (pl.col("trade_time") >= w_start) & (pl.col("trade_time") <= w_end)
            )
        df = df.filter(condition)
    else:
        # Standard session
        session_start, session_end = get_session_window(config, trade_date)
        start_time = pl.time(session_start.hour, session_start.minute)
        end_time = pl.time(session_end.hour, session_end.minute)
        df = df.filter(
            (pl.col("trade_time") >= start_time) & (pl.col("trade_time") <= end_time)
        )
    stats["out_of_session_dropped"] = before - len(df)

    # Drop the helper column
    df = df.drop("trade_time")

    if df.is_empty():
        stats["output_rows"] = 0
        return df, stats

    # 5. Sort by timestamp within each (symbol, day)
    df = df.sort(["symbol", "ts_raw"])

    # 6. Outlier flag: mark trades where price moves >20% from previous trade
    spike_pct = config.thresholds.spike_pct

    df = df.with_columns(
        pl.col("price")
          .shift(1)
          .over("symbol")
          .alias("prev_price")
    )

    df = df.with_columns(
        pl.when(pl.col("prev_price").is_not_null())
        .then(
            ((pl.col("price") - pl.col("prev_price")).abs() / pl.col("prev_price")) > spike_pct
        )
        .otherwise(pl.lit(False))
        .alias("spike_flag")
    )

    stats["spikes_flagged"] = df.filter(pl.col("spike_flag")).height
    stats["output_rows"] = len(df)

    # Drop prev_price helper (keep spike_flag for reporting)
    df = df.drop("prev_price")

    log.info(
        "Cleaned %s: %d→%d rows (-%d invalid, -%d dupes, -%d off-session, %d spikes)",
        trade_date,
        stats["input_rows"],
        stats["output_rows"],
        stats["invalid_dropped"],
        stats["duplicates_dropped"],
        stats["out_of_session_dropped"],
        stats["spikes_flagged"],
    )

    return df, stats
