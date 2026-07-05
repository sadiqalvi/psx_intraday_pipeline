#!/usr/bin/env python3
"""
scripts/daily_update.py — Daily update job (runs at 17:00 Asia/Karachi).

Usage:
    python scripts/daily_update.py
    python scripts/daily_update.py --date 2026-06-19
    python scripts/daily_update.py --env .env.prod
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.config import load_config
from pipeline.run import run_daily_update


def main():
    parser = argparse.ArgumentParser(description="PSX Candle Pipeline — Daily Update")
    parser.add_argument("--env", type=str, default=None, help="Path to .env file")
    parser.add_argument("--date", type=str, default=None, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Load config
    config = load_config(env_path=args.env)

    # Parse target date
    target = None
    if args.date:
        target = date.fromisoformat(args.date)

    # Run daily update
    run_daily_update(config, target_date=target)


if __name__ == "__main__":
    main()
