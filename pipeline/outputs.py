"""
pipeline/outputs.py — CSV writer and report writer.

Outputs:
  - csv_out/{symbol}/{symbol}_{YYYY-MM-DD}.csv  — per-symbol/day candle CSVs
  - reports/coverage.csv — every expected trading day + its status
"""

import csv
import logging
from datetime import date
from pathlib import Path
from typing import Dict, List

import polars as pl

log = logging.getLogger(__name__)


def write_candle_csvs(
    candles: pl.DataFrame,
    trade_date: date,
    csv_out_dir: Path,
):
    """
    Write per-symbol candle CSVs for a single day.
    Output: csv_out/{symbol}/{symbol}_{YYYY-MM-DD}.csv
    """
    if candles.is_empty():
        return

    symbols = candles.select("symbol").unique().to_series().to_list()

    for symbol in symbols:
        sym_dir = csv_out_dir / symbol
        sym_dir.mkdir(parents=True, exist_ok=True)

        sym_candles = candles.filter(pl.col("symbol") == symbol)

        # Select display columns (use Karachi time for human-readable CSV)
        display_cols = []
        for col in ["symbol", "minute_karachi", "ts", "open", "high", "low", "close", "volume", "had_trade"]:
            if col in sym_candles.columns:
                display_cols.append(col)

        out_path = sym_dir / f"{symbol}_{trade_date.isoformat()}.csv"
        sym_candles.select(display_cols).write_csv(str(out_path))

    log.debug("Wrote CSVs for %d symbols on %s", len(symbols), trade_date)


def write_coverage_report(
    expected_days: List[date],
    day_statuses: Dict[date, dict],
    reports_dir: Path,
):
    """
    Write the coverage report: every expected trading day + its status.
    """
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / "coverage.csv"

    with open(report_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["trade_date", "day_of_week", "status", "reason", "minutes_count", "symbols_count"])

        for d in sorted(expected_days):
            dow = d.strftime("%A")
            stats = day_statuses.get(d, {})
            status = stats.get("status", "MISSING")
            reason = stats.get("reason", "absent")
            minutes = stats.get("minutes_count", 0)
            symbols = stats.get("symbols_count", 0)

            writer.writerow([d.isoformat(), dow, status, reason, minutes, symbols])

    # Summary
    total = len(expected_days)
    complete = sum(1 for d in expected_days if day_statuses.get(d, {}).get("status") == "COMPLETE")
    missing = sum(1 for d in expected_days if day_statuses.get(d, {}).get("status", "MISSING") == "MISSING")
    conflict = sum(1 for d in expected_days if day_statuses.get(d, {}).get("status") == "CONFLICT")

    log.info(
        "Coverage report: %d expected days — %d COMPLETE, %d MISSING, %d CONFLICT",
        total, complete, missing, conflict,
    )

    return {"total": total, "complete": complete, "missing": missing, "conflict": conflict}


def print_summary(
    coverage: dict,
    n_candles: int,
    n_symbols: int,
    date_range: tuple,
):
    """Print a human-readable summary to stdout."""
    print("\n" + "=" * 60)
    print("🎉 PIPELINE COMPLETE")
    print("=" * 60)
    print(f"Date range           : {date_range[0]} → {date_range[1]}")
    print(f"Expected trading days: {coverage['total']}")
    print(f"  COMPLETE           : {coverage['complete']}")
    print(f"  MISSING            : {coverage['missing']}")
    print(f"  CONFLICT           : {coverage['conflict']}")
    print(f"Total candles        : {n_candles:,}")
    print(f"Symbols              : {n_symbols}")
    print("=" * 60)
