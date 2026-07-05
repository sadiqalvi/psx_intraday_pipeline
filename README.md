# PSX Candle Pipeline

A Python pipeline that processes raw PSX (Pakistan Stock Exchange) intraday trade data into **1-minute OHLCV candles**, validates them, and loads them into a database for backtesting.

## Features

- **Historical backfill**: Processes 244+ daily SQLite databases of tick-level trade data
- **Daily updates**: Automated at 17:00 PKT via GitHub Actions
- **1-minute OHLCV candles**: Sparse storage — only minutes with trades get a row
- **Dual DB support**: PostgreSQL (production) and SQLite (local development)
- **Full validation**: OHLC sanity checks, spike detection, day-level completeness
- **Idempotent**: Safe to re-run — upserts on `(symbol, ts)` never create duplicates
- **Timezone-correct**: All bucketing in Asia/Karachi, storage in UTC

## Quick Start

```bash
# 1. Clone and install dependencies
cd psx-candle-pipeline
pip install -r requirements.txt

# 2. Create your .env from the template
cp .env.example .env
# Edit .env with your paths

# 3. Run the backfill
python scripts/backfill.py

# 4. Check results
ls reports/coverage.csv       # day-by-day status
ls csv_out/OGDC/              # sample candles
```

## Configuration

### .env (environment variables)

| Variable | Required | Description |
|----------|----------|-------------|
| `LOCAL_HISTORICAL_DIR` | ✅ | Path to directory containing per-day `.db` files |
| `DB_BACKEND` | ✅ | `sqlite` or `postgresql` |
| `SQLITE_PATH` | If sqlite | Path for the output SQLite database |
| `PG_DSN` | If postgresql | PostgreSQL connection string |
| `GITHUB_REPO` | For daily | GitHub repo for daily data (`owner/repo`) |
| `GITHUB_TOKEN` | If private | GitHub personal access token |

### config.yaml

Session hours, spike thresholds, column mapping, and holidays are all configured here — no code changes needed for Ramadan or holiday adjustments.

## Pipeline Stages

1. **Ingest** — Read raw SQLite/CSV files, normalize timestamps, archive to Parquet
2. **Clean** — Type coercion, dedup, session filtering (09:30–15:30 PKT), outlier flagging
3. **Dedup** — Whole-day replication audit (identical → collapse, different → CONFLICT)
4. **Aggregate** — Ticks → 1-minute OHLCV candles (sparse)
5. **Validate** — Candle-level + day-level checks, quarantine failures
6. **Write** — Upsert to DB, write CSV + Parquet

## Read-Time Shaping (Strategy Support)

The **one sparse 1-minute table is the single source of truth**. Higher timeframes and fills are derived on read:

### Intraday Strategies

Fetch 1m candles (or roll up to 5m/15m from 1m) within a single COMPLETE day. For illiquid symbols, optionally forward-fill tradeless minutes with flat bars (`open=high=low=close=prev_close`, `volume=0`, `had_trade=False`).

```sql
-- Example: 5-minute candles for OGDC on a given day
SELECT
    symbol,
    date_trunc('minute', ts) - (EXTRACT(MINUTE FROM ts)::int % 5) * INTERVAL '1 minute' AS ts_5m,
    (array_agg(open ORDER BY ts))[1] AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    (array_agg(close ORDER BY ts DESC))[1] AS close,
    SUM(volume) AS volume
FROM candles_1m
WHERE symbol = 'OGDC' AND ts::date = '2026-06-19'
GROUP BY symbol, ts_5m
ORDER BY ts_5m;
```

### Swing Strategies

Roll 1m up to daily bars, but **consult `day_status` first**. Crossing a MISSING/CONFLICT day → carry position at last known close, resume on next COMPLETE day.

### Fill Rules

| Situation | Action |
|-----------|--------|
| Tradeless MINUTE inside a COMPLETE day | Flat-fill on read (optional) |
| Entire MISSING/CONFLICT DAY | Leave empty — no fabrication |
| Substitute daily OHLCV for missing day | **NEVER** — forbidden |

`had_trade` (minute level) + `day_status` (day level) keep real bars, filled minutes, and missing days permanently distinguishable.

## Output Files

```
reports/
├── coverage.csv           # Every expected trading day + status
├── validation_report.csv  # Per-day validation results
├── quarantine.csv         # Invalid candles quarantined for review
└── dedup_log.csv          # Duplicate day handling log

csv_out/
└── {SYMBOL}/
    └── {SYMBOL}_{YYYY-MM-DD}.csv

archive/
└── {YYYY-MM-DD}.parquet   # Raw tick archive (zstd compressed)
```

## Database Schema

```sql
-- 1-minute candles (primary key ensures no duplicates)
candles_1m (symbol TEXT, ts TIMESTAMPTZ, open, high, low, close, volume, had_trade)
  PRIMARY KEY (symbol, ts)

-- Day-level status tracking
day_status (symbol TEXT, trade_date DATE, status TEXT, reason TEXT, ...)
  PRIMARY KEY (symbol, trade_date)
```
