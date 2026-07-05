#!/usr/bin/env python3
"""
scripts/backfill.py — One-time historical build entry point.

Usage:
    python scripts/backfill.py
    python scripts/backfill.py --env .env.prod
"""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.config import load_config
from pipeline.run import run_backfill


def main():
    parser = argparse.ArgumentParser(description="PSX Candle Pipeline — Historical Backfill")
    parser.add_argument("--env", type=str, default=None, help="Path to .env file")
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

    # Run backfill
    run_backfill(config)


if __name__ == "__main__":
    main()
