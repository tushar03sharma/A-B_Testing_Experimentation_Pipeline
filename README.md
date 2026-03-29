# A/B Testing Experimentation Pipeline

A portfolio-ready data engineering project that simulates an experimentation platform for product teams. The pipeline generates realistic event data, loads it into DuckDB, builds layered warehouse tables with SQL, and publishes experiment metrics with statistical significance checks.

## What This Project Shows

- ETL design with raw, staging, and analytics layers
- SQL-based experiment modeling
- Python orchestration for data generation, warehouse builds, and reporting
- Data quality checks for pipeline reliability
- Experiment analysis with uplift and p-value reporting

## Project Layout

```text
.
├── sql/
├── src/ab_testing_pipeline/
├── tests/
├── data/
├── Makefile
└── pyproject.toml
```

## Dataset Story

The project simulates a product experiment called `homepage_checkout_redesign` with `control` and `treatment` variants. Users generate exposure, browsing, and checkout events. The pipeline measures conversion lift and revenue impact for the treatment group.

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
python -m ab_testing_pipeline.cli run-all
```

Outputs are created in `data/raw`, `data/warehouse`, and `data/reports`.

## Resume Highlights

- Built an end-to-end experimentation pipeline using Python, SQL, and DuckDB
- Modeled raw events into analytics-ready tables for A/B test measurement
- Automated conversion, uplift, and significance reporting for product experiments
- Added data quality checks to catch nulls, duplicate exposures, and missing variants
