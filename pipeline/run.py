"""
pipeline/run.py — Orchestrator for both backfill and daily update.

Calls the same stages in both cases; only the date range and source differ.
"""

import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional
from zoneinfo import ZoneInfo

import polars as pl

from pipeline.config import PipelineConfig, load_config
from pipeline.timezone import run_self_test
from pipeline.ingest import ingest_all, discover_source_files, extract_date_from_filename
from pipeline.github_sync import sync_recent, sync_date
from pipeline.clean import clean_ticks
from pipeline.dedup_days import audit_duplicates
from pipeline.aggregate import aggregate_to_1m
from pipeline.validate import validate_candles, write_validation_report, write_quarantine
from pipeline.db import create_backend
from pipeline.outputs import write_candle_csvs, write_coverage_report, print_summary

log = logging.getLogger(__name__)


def run_backfill(config: PipelineConfig):
    """
    One-time historical build.

    Processes every date that has actual data — no calendar pre-filter.
    Weekend scrapes and holiday carry-overs are rejected naturally by the
    validate_candles min_session_minutes threshold (< 60 populated minutes
    → MISSING). This removes the need to maintain a holidays list.

    Steps:
      1. Timezone self-test.
      2. Sync last-week data from GitHub.
      3. Ingest all raw files and group ticks by trade date.
      4. For each date with data: clean → aggregate → validate → write.
      5. Coverage report + summary.
    """
    print("🚀 Starting PSX Candle Pipeline — BACKFILL")
    print(f"   Historical data: {config.local_historical_dir}")
    print(f"   DB backend: {config.db_backend}")
    print()

    # Step 1: Timezone self-test
    run_self_test()

    # Step 2: Sync last-week data from GitHub (optional — skip if no token)
    if config.github_token:
        print("📥 Syncing last week's data from GitHub...")
        sync_recent(
            config.github_repo,
            config.local_historical_dir,
            days_back=7,
            token=config.github_token,
        )

    # Step 3: Determine date range from filenames
    source_files = discover_source_files(config)
    file_dates = sorted(set(
        d for f in source_files
        if (d := extract_date_from_filename(f)) is not None
    ))
    if not file_dates:
        print("No source files found!")
        return

    min_date = file_dates[0]
    max_date = file_dates[-1]
    print(f"   File date range: {min_date} → {max_date} ({len(file_dates)} files)")

    # Step 3b: Ingest all raw files
    print("Ingesting raw data files...")
    day_chunks = ingest_all(config)

    if not day_chunks:
        print("No data found to process!")
        return
    print(f"   Day-chunks ingested: {len(day_chunks)}")

    # Step 4: Merge all chunks by trade date.
    # Multiple files can contribute ticks for the same day (e.g. a Sunday
    # scrape contains Saturday + Friday data). Merge into one pool per date
    # and let clean_ticks handle row-level deduplication.
    print("Merging day-chunks by trade date...")
    data_by_date = {}
    for d, df in day_chunks:
        data_by_date.setdefault(d, []).append(df)

    dup_dates = {d: len(dfs) for d, dfs in data_by_date.items() if len(dfs) > 1}
    if dup_dates:
        print(f"   {len(dup_dates)} dates have data from multiple files (will merge + dedup ticks)")

    # Process every date that has data — no calendar pre-filter.
    # HOWEVER: filter to only dates within the file date range. Each .db file
    # contains residual rows from old dates (2022, 2023, etc.) left over from
    # the scraper's internal SQLite. We only want dates between min_date and
    # max_date (the actual dates of our downloaded files).
    # validate_candles will still mark weekends/holidays as MISSING naturally.
    all_dates = sorted(d for d in data_by_date.keys() if min_date <= d <= max_date)
    skipped = len(data_by_date) - len(all_dates)
    if skipped:
        print(f"   Skipped {skipped} out-of-range dates (stale scraper residuals)")
    print(f"   Dates with data: {len(all_dates)}")
    print("Processing days: clean → aggregate → validate → write")

    db = create_backend(config)
    day_statuses = {}
    all_symbols = set()
    total_candles = 0
    pending_day_statuses = []  # batch DB writes

    for i, trade_date in enumerate(all_dates):
        progress = f"[{i+1}/{len(all_dates)}]"

        # Combine all DataFrames for this date (in case of multiple sources)
        dfs = data_by_date[trade_date]
        df = pl.concat(dfs) if len(dfs) > 1 else dfs[0]

        # Clean
        cleaned, clean_stats = clean_ticks(df, trade_date, config)

        if cleaned.is_empty():
            day_statuses[trade_date] = {
                "status": "MISSING",
                "reason": "corrupt",
                "minutes_count": 0,
                "symbols_count": 0,
            }
            pending_day_statuses.append(("*", trade_date, "MISSING", "corrupt"))
            print(f"  {progress} {trade_date}: MISSING (corrupt — no valid ticks after cleaning)")
            continue

        # Aggregate
        candles = aggregate_to_1m(cleaned, trade_date)

        # Validate — min_session_minutes threshold naturally filters out
        # weekends, holidays, and low-data carry-over days
        valid_candles, quarantined, day_stats = validate_candles(
            candles, trade_date, config,
        )

        write_validation_report(day_stats, config.reports_dir)
        write_quarantine(quarantined, trade_date, config.reports_dir)

        day_statuses[trade_date] = day_stats

        if day_stats["status"] == "MISSING":
            pending_day_statuses.append(("*", trade_date, "MISSING", day_stats.get("reason")))
            dow = trade_date.strftime("%a")
            print(f"  {progress} {trade_date} ({dow}): MISSING ({day_stats.get('reason')})")
            continue

        # Write outputs
        db.upsert_candles(valid_candles)
        write_candle_csvs(valid_candles, trade_date, config.csv_out_dir)

        # Update day_status per symbol
        symbols = valid_candles.select("symbol").unique().to_series().to_list()
        for sym in symbols:
            sym_candles = valid_candles.filter(pl.col("symbol") == sym)
            ts_col = sym_candles.select("ts").to_series()
            db.upsert_day_status(
                sym, trade_date, "COMPLETE", None,
                minutes_count=sym_candles.height,
                first_ts=str(ts_col.min()),
                last_ts=str(ts_col.max()),
            )
            all_symbols.add(sym)

        total_candles += valid_candles.height
        dow = trade_date.strftime("%a")
        print(f"  {progress} {trade_date} ({dow}): COMPLETE ({valid_candles.height} candles, {len(symbols)} symbols)")

    # Flush all pending MISSING entries in one batch
    if pending_day_statuses:
        print(f"\n   Flushing {len(pending_day_statuses)} MISSING day_status entries...")
        db.batch_upsert_day_statuses(pending_day_statuses)

    # Step 5: Coverage report
    print("\nWriting coverage report...")
    coverage = write_coverage_report(all_dates, day_statuses, config.reports_dir)

    print_summary(coverage, total_candles, len(all_symbols), (min_date, max_date))

    db.close()
    print("\nBackfill complete!")


def run_daily_update(config: PipelineConfig, target_date: Optional[date] = None):
    """
    Daily update job (BUILD_SPEC §5.2).

    1. Load config, timezone self-test.
    2. Sync today's data from GitHub.
    3. Archive raw → parquet.
    4. Dedup check.
    5. Classify: COMPLETE / MISSING / CONFLICT.
    6. Clean → aggregate → validate → upsert.
    7. Append to reports.
    8. Exit non-zero on failure.
    """
    if target_date is None:
        # Today in Asia/Karachi
        target_date = datetime.now(ZoneInfo("Asia/Karachi")).date()

    print(f"🚀 PSX Candle Pipeline — DAILY UPDATE for {target_date}")

    # Step 1: Timezone self-test
    run_self_test()

    # Step 2: Sync from GitHub
    print("📥 Syncing data from GitHub...")
    downloaded = sync_date(
        target_date,
        config.github_repo,
        config.local_historical_dir,
        config.github_token,
    )

    if not downloaded:
        print(f"⚠️  No data found for {target_date} on GitHub")
        # Record as MISSING
        db = create_backend(config)
        db.upsert_day_status("*", target_date, "MISSING", "scrape_failed")
        db.close()
        sys.exit(1)

    # Step 3-6: Ingest, clean, aggregate, validate, upsert
    print("⚙️  Processing...")
    day_chunks = ingest_all(config, target_dates=[target_date])

    if not day_chunks:
        print(f"❌ No data could be ingested for {target_date}")
        db = create_backend(config)
        db.upsert_day_status("*", target_date, "MISSING", "ingest_failed")
        db.close()
        sys.exit(1)

    # We don't check for conflict here because we merge all chunks for the target date.
    # Process
    db = create_backend(config)
    success = False

    for d, df in day_chunks:
        if d != target_date:
            continue

        cleaned, _ = clean_ticks(df, d, config)
        if cleaned.is_empty():
            db.upsert_day_status("*", d, "MISSING", "corrupt")
            continue

        candles = aggregate_to_1m(cleaned, d)
        valid, quarantined, day_stats = validate_candles(candles, d, config)

        write_validation_report(day_stats, config.reports_dir)
        write_quarantine(quarantined, d, config.reports_dir)

        if day_stats["status"] != "COMPLETE":
            db.upsert_day_status("*", d, day_stats["status"], day_stats.get("reason"))
            continue

        db.upsert_candles(valid)
        write_candle_csvs(valid, d, config.csv_out_dir)

        symbols = valid.select("symbol").unique().to_series().to_list()
        for sym in symbols:
            sym_candles = valid.filter(pl.col("symbol") == sym)
            ts_col = sym_candles.select("ts").to_series()
            db.upsert_day_status(
                sym, d, "COMPLETE", None,
                minutes_count=sym_candles.height,
                first_ts=str(ts_col.min()),
                last_ts=str(ts_col.max()),
            )

        print(f"✅ {d}: COMPLETE ({valid.height} candles, {len(symbols)} symbols)")
        success = True

    db.close()

    if not success:
        print(f"❌ Failed to process {target_date}")
        sys.exit(1)

    print(f"\n✅ Daily update complete for {target_date}!")
