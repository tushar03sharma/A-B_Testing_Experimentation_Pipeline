from __future__ import annotations

import duckdb

from .config import Settings


def _fetch_one(connection: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(connection.execute(query).fetchone()[0])


def run_quality_checks(settings: Settings | None = None) -> list[str]:
    settings = settings or Settings()
    failures: list[str] = []

    with duckdb.connect(str(settings.warehouse_path)) as connection:
        staged_events = _fetch_one(connection, "SELECT COUNT(*) FROM stg_events")
        if staged_events == 0:
            failures.append("stg_events is empty")

        null_exposures = _fetch_one(
            connection,
            """
            SELECT COUNT(*)
            FROM experiment_exposures
            WHERE experiment_key IS NULL OR variant IS NULL OR exposure_ts IS NULL
            """,
        )
        if null_exposures > 0:
            failures.append("experiment_exposures contains null experiment fields")

        duplicate_exposures = _fetch_one(
            connection,
            """
            SELECT COUNT(*)
            FROM (
              SELECT experiment_key, user_id, COUNT(*) AS row_count
              FROM experiment_exposures
              GROUP BY 1, 2
              HAVING COUNT(*) > 1
            )
            """,
        )
        if duplicate_exposures > 0:
            failures.append("duplicate user exposures detected")

        variant_count = _fetch_one(
            connection,
            "SELECT COUNT(DISTINCT variant) FROM experiment_exposures",
        )
        if variant_count < 2:
            failures.append("expected both control and treatment variants")

        invalid_converters = _fetch_one(
            connection,
            """
            SELECT COUNT(*)
            FROM experiment_metrics_summary
            WHERE converters > exposed_users
            """,
        )
        if invalid_converters > 0:
            failures.append("converters exceed exposed users")

    return failures
