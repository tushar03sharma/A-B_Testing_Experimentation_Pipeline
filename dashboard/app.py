from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from ab_testing_pipeline.config import Settings
from ab_testing_pipeline.dashboard_data import (
    build_experiment_snapshot,
    load_daily_metrics,
    load_filter_options,
    load_filtered_summary,
    load_segment_metrics,
)


st.set_page_config(
    page_title="Experiment Dashboard",
    page_icon="",
    layout="wide",
)

st.markdown(
    """
    <style>
      .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
      }
      div[data-testid="metric-container"] {
        background: linear-gradient(180deg, #f7fbf8 0%, #eef6f0 100%);
        border: 1px solid #d7e6da;
        padding: 12px 16px;
        border-radius: 16px;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

settings = Settings()


@st.cache_data(show_spinner=False)
def get_filter_options() -> dict[str, list[str]]:
    return load_filter_options(settings=settings)


@st.cache_data(show_spinner=False)
def get_summary(countries: tuple[str, ...], devices: tuple[str, ...]) -> pd.DataFrame:
    return load_filtered_summary(settings=settings, countries=countries, devices=devices)


@st.cache_data(show_spinner=False)
def get_daily(countries: tuple[str, ...], devices: tuple[str, ...]) -> pd.DataFrame:
    return load_daily_metrics(settings=settings, countries=countries, devices=devices)


@st.cache_data(show_spinner=False)
def get_segments(
    dimension: str,
    countries: tuple[str, ...],
    devices: tuple[str, ...],
) -> pd.DataFrame:
    return load_segment_metrics(
        dimension=dimension,
        settings=settings,
        countries=countries,
        devices=devices,
    )


def format_percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%"


def format_currency(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.2f}"


if not settings.warehouse_path.exists():
    st.error("Warehouse not found. Run `python -m ab_testing_pipeline.cli run-all` first.")
    st.stop()

options = get_filter_options()

with st.sidebar:
    st.title("Filters")
    selected_countries = st.multiselect(
        "Country",
        options=options["countries"],
        default=options["countries"],
    )
    selected_devices = st.multiselect(
        "Device",
        options=options["devices"],
        default=options["devices"],
    )
    segment_dimension = st.selectbox(
        "Segment view",
        options=["country", "device_type"],
        format_func=lambda value: "Country" if value == "country" else "Device",
    )
    st.caption("The dashboard reads from the DuckDB warehouse and recomputes metrics for the selected slice.")

countries = tuple(selected_countries)
devices = tuple(selected_devices)

summary_df = get_summary(countries, devices)
daily_df = get_daily(countries, devices)
segment_df = get_segments(segment_dimension, countries, devices)
snapshot = build_experiment_snapshot(summary_df)

st.title("A/B Testing Experiment Dashboard")
st.caption(
    "Experiment: homepage_checkout_redesign | Source: DuckDB warehouse | Window: 7-day conversion lookback"
)

if summary_df.empty:
    st.warning("No data matched the selected filters.")
    st.stop()

if snapshot["winner"] == "Treatment wins":
    st.success(snapshot["decision"])
elif snapshot["winner"] == "Control wins":
    st.error(snapshot["decision"])
else:
    st.info(snapshot["decision"])

metric_1, metric_2, metric_3, metric_4 = st.columns(4)
metric_1.metric("Winner", str(snapshot["winner"]), f"{snapshot['exposed_users']:,} exposed users")
metric_2.metric(
    "Treatment Conversion",
    format_percent(snapshot["treatment_rate"]),
    f"{snapshot['uplift_pp']:+.2f} pp vs control" if snapshot["uplift_pp"] is not None else None,
)
metric_3.metric(
    "P-value",
    f"{snapshot['p_value']:.4f}" if snapshot["p_value"] is not None else "n/a",
    f"z = {snapshot['z_score']:.2f}" if snapshot["z_score"] is not None else None,
)
metric_4.metric(
    "Treatment Revenue / User",
    format_currency(snapshot["treatment_rpu"]),
    f"{snapshot['rpu_lift']:+.2f} vs control" if snapshot["rpu_lift"] is not None else None,
)

trend_col, traffic_col = st.columns(2)

with trend_col:
    conversion_chart = px.line(
        daily_df,
        x="exposure_date",
        y="conversion_rate",
        color="variant",
        markers=True,
        color_discrete_map={"control": "#355c7d", "treatment": "#2a9d8f"},
        title="Daily Conversion Rate",
        labels={"exposure_date": "Exposure Date", "conversion_rate": "Conversion Rate", "variant": "Variant"},
    )
    conversion_chart.update_layout(yaxis_tickformat=".1%")
    st.plotly_chart(conversion_chart, use_container_width=True)

with traffic_col:
    traffic_chart = px.bar(
        daily_df,
        x="exposure_date",
        y="exposed_users",
        color="variant",
        barmode="group",
        color_discrete_map={"control": "#355c7d", "treatment": "#e76f51"},
        title="Daily Exposed Users",
        labels={"exposure_date": "Exposure Date", "exposed_users": "Exposed Users", "variant": "Variant"},
    )
    st.plotly_chart(traffic_chart, use_container_width=True)

segment_col, table_col = st.columns([1.2, 1])

with segment_col:
    segment_chart = px.bar(
        segment_df,
        x="segment",
        y="conversion_rate",
        color="variant",
        barmode="group",
        color_discrete_map={"control": "#355c7d", "treatment": "#2a9d8f"},
        title="Segment Conversion Breakdown",
        labels={"segment": "Segment", "conversion_rate": "Conversion Rate", "variant": "Variant"},
    )
    segment_chart.update_layout(yaxis_tickformat=".1%")
    st.plotly_chart(segment_chart, use_container_width=True)

with table_col:
    summary_table = summary_df.copy()
    summary_table["conversion_rate"] = summary_table["conversion_rate"].map(lambda value: f"{value * 100:.2f}%")
    st.subheader("Variant Summary")
    st.dataframe(summary_table, use_container_width=True, hide_index=True)

st.subheader("Segment Detail")
segment_table = segment_df.copy()
segment_table["conversion_rate"] = segment_table["conversion_rate"].map(lambda value: f"{value * 100:.2f}%")
segment_table["revenue_per_user"] = segment_table["revenue_per_user"].map(lambda value: f"${value:,.2f}")
st.dataframe(segment_table, use_container_width=True, hide_index=True)
