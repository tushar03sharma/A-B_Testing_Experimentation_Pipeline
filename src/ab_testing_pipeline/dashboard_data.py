from __future__ import annotations

from collections.abc import Sequence

import duckdb
import pandas as pd

from .analysis import _proportion_test
from .config import Settings


def _where_clause(
    countries: Sequence[str] | None = None,
    devices: Sequence[str] | None = None,
) -> tuple[str, list[str]]:
    clauses: list[str] = []
    params: list[str] = []

    if countries:
        placeholders = ", ".join("?" for _ in countries)
        clauses.append(f"country IN ({placeholders})")
        params.extend(countries)

    if devices:
        placeholders = ", ".join("?" for _ in devices)
        clauses.append(f"device_type IN ({placeholders})")
        params.extend(devices)

    if not clauses:
        return "", params
    return f"WHERE {' AND '.join(clauses)}", params


def _connect(settings: Settings) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(settings.warehouse_path), read_only=True)


def load_filter_options(settings: Settings | None = None) -> dict[str, list[str]]:
    settings = settings or Settings()
    with _connect(settings) as connection:
        countries = connection.execute(
            "SELECT DISTINCT country FROM experiment_conversions ORDER BY country"
        ).fetchdf()["country"].tolist()
        devices = connection.execute(
            "SELECT DISTINCT device_type FROM experiment_conversions ORDER BY device_type"
        ).fetchdf()["device_type"].tolist()
    return {"countries": countries, "devices": devices}


def load_filtered_summary(
    settings: Settings | None = None,
    countries: Sequence[str] | None = None,
    devices: Sequence[str] | None = None,
) -> pd.DataFrame:
    settings = settings or Settings()
    where_clause, params = _where_clause(countries, devices)

    query = f"""
    WITH variant_metrics AS (
      SELECT
        variant,
        COUNT(*) AS exposed_users,
        SUM(CASE WHEN conversion_ts IS NOT NULL THEN 1 ELSE 0 END) AS converters,
        ROUND(AVG(CASE WHEN conversion_ts IS NOT NULL THEN 1.0 ELSE 0.0 END), 4) AS conversion_rate,
        ROUND(SUM(revenue), 2) AS total_revenue,
        ROUND(AVG(revenue), 2) AS revenue_per_user
      FROM experiment_conversions
      {where_clause}
      GROUP BY 1
    ),
    control_metrics AS (
      SELECT
        conversion_rate AS control_conversion_rate,
        revenue_per_user AS control_revenue_per_user
      FROM variant_metrics
      WHERE variant = 'control'
    )
    SELECT
      '{settings.experiment_key}' AS experiment_key,
      metrics.variant,
      metrics.exposed_users,
      metrics.converters,
      metrics.conversion_rate,
      metrics.total_revenue,
      metrics.revenue_per_user,
      ROUND(metrics.conversion_rate - control.control_conversion_rate, 4) AS absolute_lift,
      ROUND(
        CASE
          WHEN control.control_conversion_rate = 0 THEN NULL
          ELSE ((metrics.conversion_rate / control.control_conversion_rate) - 1) * 100
        END,
        2
      ) AS relative_lift_pct,
      ROUND(metrics.revenue_per_user - control.control_revenue_per_user, 2) AS rpu_lift
    FROM variant_metrics AS metrics
    LEFT JOIN control_metrics AS control
      ON TRUE
    ORDER BY metrics.variant
    """

    with _connect(settings) as connection:
        return connection.execute(query, params).df()


def load_daily_metrics(
    settings: Settings | None = None,
    countries: Sequence[str] | None = None,
    devices: Sequence[str] | None = None,
) -> pd.DataFrame:
    settings = settings or Settings()
    where_clause, params = _where_clause(countries, devices)

    query = f"""
    SELECT
      CAST(exposure_ts AS DATE) AS exposure_date,
      variant,
      COUNT(*) AS exposed_users,
      SUM(CASE WHEN conversion_ts IS NOT NULL THEN 1 ELSE 0 END) AS converters,
      ROUND(AVG(CASE WHEN conversion_ts IS NOT NULL THEN 1.0 ELSE 0.0 END), 4) AS conversion_rate,
      ROUND(SUM(revenue), 2) AS total_revenue,
      ROUND(AVG(revenue), 2) AS revenue_per_user
    FROM experiment_conversions
    {where_clause}
    GROUP BY 1, 2
    ORDER BY 1, 2
    """

    with _connect(settings) as connection:
        return connection.execute(query, params).df()


def load_segment_metrics(
    dimension: str,
    settings: Settings | None = None,
    countries: Sequence[str] | None = None,
    devices: Sequence[str] | None = None,
) -> pd.DataFrame:
    settings = settings or Settings()
    valid_dimensions = {"country", "device_type"}
    if dimension not in valid_dimensions:
        raise ValueError(f"dimension must be one of {sorted(valid_dimensions)}")

    where_clause, params = _where_clause(countries, devices)
    query = f"""
    SELECT
      {dimension} AS segment,
      variant,
      COUNT(*) AS exposed_users,
      SUM(CASE WHEN conversion_ts IS NOT NULL THEN 1 ELSE 0 END) AS converters,
      ROUND(AVG(CASE WHEN conversion_ts IS NOT NULL THEN 1.0 ELSE 0.0 END), 4) AS conversion_rate,
      ROUND(AVG(revenue), 2) AS revenue_per_user
    FROM experiment_conversions
    {where_clause}
    GROUP BY 1, 2
    ORDER BY 1, 2
    """

    with _connect(settings) as connection:
        return connection.execute(query, params).df()


def build_experiment_snapshot(summary_df: pd.DataFrame) -> dict[str, float | int | str | None]:
    empty_snapshot = {
        "winner": "No data",
        "exposed_users": 0,
        "control_rate": None,
        "treatment_rate": None,
        "uplift_pp": None,
        "relative_lift_pct": None,
        "control_rpu": None,
        "treatment_rpu": None,
        "rpu_lift": None,
        "z_score": None,
        "p_value": None,
        "decision": "No data matched the selected filters.",
    }
    if summary_df.empty:
        return empty_snapshot

    control = summary_df.loc[summary_df["variant"] == "control"]
    treatment = summary_df.loc[summary_df["variant"] == "treatment"]
    if control.empty or treatment.empty:
        return empty_snapshot

    control_row = control.iloc[0]
    treatment_row = treatment.iloc[0]
    z_score, p_value = _proportion_test(
        int(control_row["converters"]),
        int(control_row["exposed_users"]),
        int(treatment_row["converters"]),
        int(treatment_row["exposed_users"]),
    )

    uplift_pp = round((float(treatment_row["conversion_rate"]) - float(control_row["conversion_rate"])) * 100, 2)
    relative_lift_pct = round(float(treatment_row["relative_lift_pct"]), 2)
    rpu_lift = round(float(treatment_row["revenue_per_user"]) - float(control_row["revenue_per_user"]), 2)
    exposed_users = int(control_row["exposed_users"]) + int(treatment_row["exposed_users"])

    winner = "Inconclusive"
    decision = "Treatment is directionally positive, but the result is not yet statistically significant."
    if p_value is not None and p_value < 0.05:
        if uplift_pp > 0:
            winner = "Treatment wins"
            decision = "Treatment outperforms control with statistically significant conversion lift."
        elif uplift_pp < 0:
            winner = "Control wins"
            decision = "Control performs better, so the treatment should not be shipped as-is."

    return {
        "winner": winner,
        "exposed_users": exposed_users,
        "control_rate": float(control_row["conversion_rate"]),
        "treatment_rate": float(treatment_row["conversion_rate"]),
        "uplift_pp": uplift_pp,
        "relative_lift_pct": relative_lift_pct,
        "control_rpu": float(control_row["revenue_per_user"]),
        "treatment_rpu": float(treatment_row["revenue_per_user"]),
        "rpu_lift": rpu_lift,
        "z_score": z_score,
        "p_value": p_value,
        "decision": decision,
    }
