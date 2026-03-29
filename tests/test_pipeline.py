from ab_testing_pipeline.analysis import _proportion_test


def test_proportion_test_returns_lower_p_value_for_clear_lift() -> None:
    z_score, p_value = _proportion_test(
        control_converters=80,
        control_users=1000,
        treatment_converters=125,
        treatment_users=1000,
    )

    assert z_score is not None
    assert p_value is not None
    assert z_score > 0
    assert p_value < 0.01
