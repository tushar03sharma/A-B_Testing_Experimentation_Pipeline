from __future__ import annotations

import typer

from .analysis import export_reports
from .config import Settings, ensure_directories
from .data_generation import generate_data
from .pipeline import build_warehouse
from .quality import run_quality_checks


app = typer.Typer(no_args_is_help=True)


@app.command()
def generate(num_users: int = 8000, seed: int = 7) -> None:
    ensure_directories()
    settings = Settings()
    _, events = generate_data(settings=settings, num_users=num_users, seed=seed)
    typer.echo(f"Generated {len(events):,} events in {settings.raw_events_path}")


@app.command()
def build() -> None:
    settings = Settings()
    build_warehouse(settings=settings)
    typer.echo(f"Built warehouse at {settings.warehouse_path}")


@app.command()
def analyze() -> None:
    settings = Settings()
    stats = export_reports(settings=settings)
    typer.echo(
        f"Analysis complete. z_score={stats['z_score']} p_value={stats['p_value']}"
    )


@app.command()
def quality() -> None:
    settings = Settings()
    failures = run_quality_checks(settings=settings)
    if failures:
        for failure in failures:
            typer.echo(f"FAILED: {failure}")
        raise typer.Exit(code=1)
    typer.echo("All data quality checks passed.")


@app.command("run-all")
def run_all(num_users: int = 8000, seed: int = 7) -> None:
    generate(num_users=num_users, seed=seed)
    build()
    analyze()
    quality()


def run() -> None:
    app()


if __name__ == "__main__":
    run()
