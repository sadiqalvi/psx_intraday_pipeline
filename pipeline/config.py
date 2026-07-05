"""
pipeline/config.py — Load .env + config.yaml, validate, expose typed config.
"""

import os
import sys
from pathlib import Path
from dataclasses import dataclass, field
from datetime import time, date
from typing import List, Optional

import yaml
from dotenv import load_dotenv


# ── Locate project root (directory containing config.yaml) ──────────────
def _find_project_root() -> Path:
    """Walk up from this file until we find config.yaml."""
    d = Path(__file__).resolve().parent.parent
    for _ in range(5):
        if (d / "config.yaml").exists():
            return d
        d = d.parent
    raise FileNotFoundError("Cannot find config.yaml in any parent directory")


PROJECT_ROOT = _find_project_root()


# ── Dataclasses ─────────────────────────────────────────────────────────
@dataclass
class TimeWindow:
    start: time
    end: time


@dataclass
class SessionOverride:
    from_date: date
    to_date: date
    start: time
    end: time


@dataclass
class SessionConfig:
    start: time
    end: time
    days: List[str]
    friday: List[TimeWindow] = field(default_factory=list)
    overrides: List[SessionOverride] = field(default_factory=list)


@dataclass
class ThresholdConfig:
    spike_pct: float
    min_session_minutes: int


@dataclass
class ColumnMap:
    symbol: str
    price: str
    volume: str
    timestamp: str


@dataclass
class PipelineConfig:
    # Environment
    local_historical_dir: Path
    github_repo: str
    github_branch: str
    github_data_path: str
    github_token: Optional[str]
    pg_dsn: Optional[str]
    db_backend: str          # "postgresql" or "sqlite"
    sqlite_path: Path
    raw_file_glob: str

    # config.yaml
    timezone: str
    session: SessionConfig
    thresholds: ThresholdConfig
    columns: ColumnMap
    timestamp_format: str    # auto | epoch_s | epoch_ms | iso | naive_local | naive_utc
    file_layout: str         # one_per_day | single_file | one_per_symbol
    source_format: str       # sqlite | csv

    # Derived paths
    project_root: Path = field(default_factory=lambda: PROJECT_ROOT)

    @property
    def archive_dir(self) -> Path:
        return self.project_root / "archive"

    @property
    def csv_out_dir(self) -> Path:
        return self.project_root / "csv_out"

    @property
    def reports_dir(self) -> Path:
        return self.project_root / "reports"

    def ensure_dirs(self):
        """Create output directories if they don't exist."""
        for d in [self.archive_dir, self.csv_out_dir, self.reports_dir]:
            d.mkdir(parents=True, exist_ok=True)


def _parse_time(s: str) -> time:
    parts = s.split(":")
    return time(int(parts[0]), int(parts[1]))


def _parse_date(s: str) -> date:
    return date.fromisoformat(s)


def load_config(env_path: Optional[str] = None) -> PipelineConfig:
    """
    Load configuration from .env and config.yaml.
    Returns a fully validated PipelineConfig.
    """
    # Load .env
    env_file = Path(env_path) if env_path else PROJECT_ROOT / ".env"
    if env_file.exists():
        load_dotenv(env_file)

    # Load config.yaml
    yaml_path = PROJECT_ROOT / "config.yaml"
    if not yaml_path.exists():
        raise FileNotFoundError(f"config.yaml not found at {yaml_path}")

    with open(yaml_path) as f:
        cfg = yaml.safe_load(f)

    # Parse session config
    sess = cfg["session"]
    overrides = []
    for ov in sess.get("overrides", []) or []:
        overrides.append(SessionOverride(
            from_date=_parse_date(ov["from"]),
            to_date=_parse_date(ov["to"]),
            start=_parse_time(ov["start"]),
            end=_parse_time(ov["end"]),
        ))

    friday_windows = []
    for fw in sess.get("friday", []) or []:
        friday_windows.append(TimeWindow(
            start=_parse_time(fw["start"]),
            end=_parse_time(fw["end"]),
        ))

    session = SessionConfig(
        start=_parse_time(sess["start"]),
        end=_parse_time(sess["end"]),
        days=sess["days"],
        friday=friday_windows,
        overrides=overrides,
    )

    # Parse thresholds
    thr = cfg["thresholds"]
    thresholds = ThresholdConfig(
        spike_pct=float(thr["spike_pct"]),
        min_session_minutes=int(thr["min_session_minutes"]),
    )

    # Parse column map
    cols = cfg["columns"]
    columns = ColumnMap(
        symbol=cols["symbol"],
        price=cols["price"],
        volume=cols["volume"],
        timestamp=cols["timestamp"],
    )

    # Environment variables with sensible defaults
    db_backend = os.environ.get("DB_BACKEND", "sqlite").lower()
    local_hist = os.environ.get("LOCAL_HISTORICAL_DIR", "")
    sqlite_path = os.environ.get("SQLITE_PATH", str(PROJECT_ROOT / "psx_candles.db"))

    config = PipelineConfig(
        local_historical_dir=Path(local_hist) if local_hist else PROJECT_ROOT / "data",
        github_repo=os.environ.get("GITHUB_REPO", "ahmerhkhan/psx-intraday"),
        github_branch=os.environ.get("GITHUB_BRANCH", "main"),
        github_data_path=os.environ.get("GITHUB_DATA_PATH", "data/raw/"),
        github_token=os.environ.get("GITHUB_TOKEN"),
        pg_dsn=os.environ.get("PG_DSN"),
        db_backend=db_backend,
        sqlite_path=Path(sqlite_path),
        raw_file_glob=os.environ.get("RAW_FILE_GLOB", "*.csv"),
        timezone=cfg.get("timezone", "Asia/Karachi"),
        session=session,
        thresholds=thresholds,
        columns=columns,
        timestamp_format=cfg.get("timestamp_format", "auto"),
        file_layout=cfg.get("file_layout", "one_per_day"),
        source_format=cfg.get("source_format", "sqlite"),
    )

    config.ensure_dirs()
    return config
