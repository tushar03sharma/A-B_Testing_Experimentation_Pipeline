import pandas as pd

from ab_testing_pipeline.dashboard_data import build_experiment_snapshot


def test_build_experiment_snapshot_identifies_treatment_win() -> None:
    summary_df = pd.DataFrame(
        [
            {
                "variant": "control",
                "exposed_users": 1000,
                "converters": 90,
                "conversion_rate": 0.09,
                "revenue_per_user": 8.2,
                "relative_lift_pct": 0.0,
            },
            {
                "variant": "treatment",
                "exposed_users": 1000,
                "converters": 130,
                "conversion_rate": 0.13,
                "revenue_per_user": 10.1,
                "relative_lift_pct": 44.44,
            },
        ]
    )

    snapshot = build_experiment_snapshot(summary_df)

    assert snapshot["winner"] == "Treatment wins"
    assert snapshot["p_value"] is not None
    assert snapshot["p_value"] < 0.05
    assert snapshot["uplift_pp"] == 4.0
