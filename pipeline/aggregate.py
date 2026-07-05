"""
pipeline/aggregate.py — Aggregate cleaned ticks into 1-minute OHLCV candles.

Per (symbol, trade_date), resample to 1-minute buckets.
Sparse storage: only minutes with ≥1 trade produce a row.
"""

import logging
from datetime import date

import polars as pl

log = logging.getLogger(__name__)


def aggregate_to_1m(
    df: pl.DataFrame,
    trade_date: date,
) -> pl.DataFrame:
    """
    Aggregate cleaned ticks into 1-minute OHLCV candles.

    Parameters
    ----------
    df : pl.DataFrame
        Cleaned tick data with columns:
        symbol, ts_raw (UTC), ts_karachi (Asia/Karachi), price, volume, spike_flag
    trade_date : date
        The trading date (for logging).

    Returns
    -------
    pl.DataFrame with columns:
        symbol, ts (UTC, minute bucket start), open, high, low, close, volume, had_trade
    """
    if df.is_empty():
        return pl.DataFrame(schema={
            "symbol": pl.Utf8,
            "ts": pl.Datetime("us", "UTC"),
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "had_trade": pl.Boolean,
        })

    # Truncate Karachi time to minute boundary for grouping
    df = df.with_columns(
        pl.col("ts_karachi").dt.truncate("1m").alias("minute_karachi")
    )

    # Also compute the corresponding UTC minute bucket
    df = df.with_columns(
        pl.col("ts_raw").dt.truncate("1m").alias("minute_utc")
    )

    # Group by symbol and minute, compute OHLCV
    candles = (
        df.group_by(["symbol", "minute_utc", "minute_karachi"])
        .agg([
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
        ])
        .sort(["symbol", "minute_utc"])
    )

    # Add had_trade flag
    candles = candles.with_columns(
        pl.lit(True).alias("had_trade")
    )

    # Rename to final schema
    candles = candles.rename({"minute_utc": "ts"})

    # Select final columns
    candles = candles.select([
        "symbol", "ts", "open", "high", "low", "close", "volume", "had_trade",
        "minute_karachi",  # keep for CSV output / display
    ])

    log.info(
        "Aggregated %s: %d ticks → %d candles across %d symbols",
        trade_date,
        len(df),
        len(candles),
        candles.select("symbol").n_unique(),
    )

    return candles


def aggregate_batch(
    day_chunks: list,
) -> list:
    """
    Aggregate a batch of (date, cleaned_df) tuples.

    Returns list of (date, candles_df) tuples.
    """
    results = []
    for trade_date, df in day_chunks:
        candles = aggregate_to_1m(df, trade_date)
        if not candles.is_empty():
            results.append((trade_date, candles))
    return results
