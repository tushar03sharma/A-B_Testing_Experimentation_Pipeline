from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
REPORT_DIR = DATA_DIR / "reports"
SQL_DIR = ROOT_DIR / "sql"


@dataclass(frozen=True)
class Settings:
    experiment_key: str = "homepage_checkout_redesign"
    warehouse_path: Path = WAREHOUSE_DIR / "experiments.duckdb"
    raw_events_path: Path = RAW_DIR / "events.jsonl"
    users_path: Path = RAW_DIR / "users.csv"
    summary_report_path: Path = REPORT_DIR / "experiment_summary.md"
    summary_csv_path: Path = REPORT_DIR / "experiment_summary.csv"
    daily_csv_path: Path = REPORT_DIR / "experiment_daily_metrics.csv"
    lookback_days: int = 7


def ensure_directories() -> None:
    for path in (DATA_DIR, RAW_DIR, WAREHOUSE_DIR, REPORT_DIR, SQL_DIR):
        path.mkdir(parents=True, exist_ok=True)
