from __future__ import annotations

import math

import duckdb
import pandas as pd

from .config import Settings, ensure_directories


def _proportion_test(
    control_converters: int,
    control_users: int,
    treatment_converters: int,
    treatment_users: int,
) -> tuple[float | None, float | None]:
    if min(control_users, treatment_users) == 0:
        return None, None

    p1 = control_converters / control_users
    p2 = treatment_converters / treatment_users
    pooled = (control_converters + treatment_converters) / (control_users + treatment_users)
    variance = pooled * (1 - pooled) * ((1 / control_users) + (1 / treatment_users))
    if variance <= 0:
        return None, None

    z_score = (p2 - p1) / math.sqrt(variance)
    p_value = math.erfc(abs(z_score) / math.sqrt(2))
    return round(z_score, 4), round(p_value, 6)


def export_reports(settings: Settings | None = None) -> dict[str, float | None]:
    settings = settings or Settings()
    ensure_directories()

    with duckdb.connect(str(settings.warehouse_path)) as connection:
        summary_df = connection.execute(
            "SELECT * FROM experiment_metrics_summary ORDER BY variant"
        ).df()
        daily_df = connection.execute(
            "SELECT * FROM experiment_metrics_daily ORDER BY exposure_date, variant"
        ).df()

    control = summary_df.loc[summary_df["variant"] == "control"]
    treatment = summary_df.loc[summary_df["variant"] == "treatment"]

    stats = {"z_score": None, "p_value": None}
    if not control.empty and not treatment.empty:
        stats["z_score"], stats["p_value"] = _proportion_test(
            int(control.iloc[0]["converters"]),
            int(control.iloc[0]["exposed_users"]),
            int(treatment.iloc[0]["converters"]),
            int(treatment.iloc[0]["exposed_users"]),
        )

    enriched = summary_df.copy()
    enriched["z_score"] = stats["z_score"]
    enriched["p_value"] = stats["p_value"]
    enriched.to_csv(settings.summary_csv_path, index=False)
    daily_df.to_csv(settings.daily_csv_path, index=False)

    control_rate = None if control.empty else float(control.iloc[0]["conversion_rate"])
    treatment_rate = None if treatment.empty else float(treatment.iloc[0]["conversion_rate"])
    uplift = None
    if control_rate not in (None, 0) and treatment_rate is not None:
        uplift = round((treatment_rate - control_rate) * 100, 2)

    report_lines = [
        "# Experiment Summary",
        "",
        f"- Experiment: `{settings.experiment_key}`",
        f"- Lookback window: `{settings.lookback_days}` days",
        f"- Control conversion rate: `{control_rate}`",
        f"- Treatment conversion rate: `{treatment_rate}`",
        f"- Absolute uplift (pp): `{uplift}`",
        f"- Z-score: `{stats['z_score']}`",
        f"- P-value: `{stats['p_value']}`",
        "",
        "## Variant Metrics",
        "",
        "```text",
        enriched.to_string(index=False),
        "```",
    ]
    settings.summary_report_path.write_text("\n".join(report_lines))
    return stats
