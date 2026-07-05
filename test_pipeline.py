"""Quick test: run the timezone self-test and process a single .db file."""
import sys
sys.path.insert(0, ".")

from pipeline.timezone import run_self_test
from pipeline.config import load_config
from pipeline.ingest import ingest_file, extract_date_from_filename
from pipeline.clean import clean_ticks
from pipeline.aggregate import aggregate_to_1m
from pipeline.validate import validate_candles
from pathlib import Path

print("=" * 60)
print("STEP 1: Timezone self-test")
print("=" * 60)
run_self_test()

print("\n" + "=" * 60)
print("STEP 2: Load config")
print("=" * 60)
config = load_config()
print(f"  Historical dir: {config.local_historical_dir}")
print(f"  DB backend: {config.db_backend}")
print(f"  Timezone: {config.timezone}")
print(f"  Session: {config.session.start} - {config.session.end}")

print("\n" + "=" * 60)
print("STEP 3: Ingest a single file")
print("=" * 60)
test_file = Path(config.local_historical_dir) / "psx_intraday_20260619.db"
file_date = extract_date_from_filename(test_file)
print(f"  File: {test_file.name}, Date: {file_date}")

lf = ingest_file(test_file, config)
df = lf.collect()
print(f"  Rows ingested: {len(df)}")
print(f"  Columns: {df.columns}")
print(f"  Sample (first 3 rows):")
print(df.head(3))

print("\n" + "=" * 60)
print("STEP 4: Clean ticks")
print("=" * 60)
cleaned, stats = clean_ticks(df, file_date, config)
print(f"  Stats: {stats}")
print(f"  Cleaned rows: {len(cleaned)}")

print("\n" + "=" * 60)
print("STEP 5: Aggregate to 1m candles")
print("=" * 60)
candles = aggregate_to_1m(cleaned, file_date)
print(f"  Candles: {len(candles)}")
print(f"  Symbols: {candles.select('symbol').n_unique()}")
print(f"  Sample:")
print(candles.head(5))

print("\n" + "=" * 60)
print("STEP 6: Validate")
print("=" * 60)
valid, quarantined, day_stats = validate_candles(candles, file_date, config)
print(f"  Valid: {len(valid)}")
print(f"  Quarantined: {len(quarantined)}")
print(f"  Status: {day_stats['status']}")
print(f"  Day stats: {day_stats}")

print("\n✅ All stages passed for single file test!")
