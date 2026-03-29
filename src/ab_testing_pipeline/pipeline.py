from __future__ import annotations

from pathlib import Path

import duckdb

from .config import SQL_DIR, Settings, ensure_directories


SQL_FILES = [
    "staging_events.sql",
    "experiment_exposures.sql",
    "experiment_conversions.sql",
    "daily_metrics.sql",
    "summary_metrics.sql",
]


def _read_sql(filename: str, settings: Settings) -> str:
    template = (SQL_DIR / filename).read_text()
    return template.format(
        raw_events=settings.raw_events_path.as_posix(),
        lookback_days=settings.lookback_days,
    )


def _connect(path: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(path))


def build_warehouse(settings: Settings | None = None) -> None:
    settings = settings or Settings()
    ensure_directories()
    with _connect(settings.warehouse_path) as connection:
        for sql_file in SQL_FILES:
            connection.execute(_read_sql(sql_file, settings))
