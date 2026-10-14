from __future__ import annotations

import logging

import requests
import typer

from luxtrade.logging_setup import setup_logging

from .config import load_settings, resolve_storage_dirs
from .metadata.classifications import import_metadata
from .pipeline.ingest import ingest_sources
from .sources.catalogue import discover_matching_files
from .sources.downloader import download_sources
from .storage.database import init_database, inspect_status

app = typer.Typer(
    add_completion=False, no_args_is_help=True, help="Luxembourg Comext offline ingestion CLI"
)
logger = logging.getLogger(__name__)


@app.callback()
def configure_logging(ctx: typer.Context) -> None:
    """Configure console and file logging before executing any CLI command."""
    settings = load_settings()
    setup_logging(level=settings.get("logging", {}).get("level", "DEBUG"))
    logger.debug("Executing command=%s", ctx.invoked_subcommand)


@app.command("init-db")
def init_db() -> None:
    settings = load_settings()
    db_path = resolve_storage_dirs(settings)[-1]
    init_database(db_path)
    typer.echo(f"Initialized DuckDB database at {db_path}")


@app.command("discover")
def discover() -> None:
    settings = load_settings()
    results = discover_matching_files(settings)
    if not results:
        typer.echo("No matching remote files found.")
        return
    for entry in results:
        period = entry.get("period") or "-"
        size = entry.get("size") or "-"
        typer.echo(f"{entry['source_name']:<18} {entry['name']:<60} {period:<8} {size}")


@app.command("download")
def download_command(
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Refresh the listing and validate URLs without downloading files."
    ),
) -> None:
    settings = load_settings()
    try:
        items = download_sources(settings, dry_run=dry_run)
    except (requests.RequestException, ValueError) as exc:
        typer.echo(f"Download check failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    if dry_run:
        typer.echo(f"Validated downloads: {len(items)}")
        for item in items:
            typer.echo(f"{item['source_name']} -> {item['url']}")
        return
    typer.echo(f"Downloaded {len(items)} file(s).")


@app.command("import-metadata")
def import_metadata_command() -> None:
    settings = load_settings()
    imported = import_metadata(settings)
    typer.echo(
        f"Imported metadata resources: {', '.join(sorted(imported)) if imported else 'none'}"
    )


@app.command("ingest")
def ingest() -> None:
    logger.info("Starting ingest command; loading settings")
    settings = load_settings()
    logger.debug("Ingest implementation=%s", ingest_sources.__module__)
    stats = ingest_sources(settings)
    typer.echo(
        f"Saved {stats['rows']} new rows; completed {stats['files']} source file(s), "
        f"resumed {stats['resumed']}, skipped {stats['skipped']}. "
        f"Database total: {stats['total_rows']} rows."
    )


@app.command("run")
def run_pipeline() -> None:
    settings = load_settings()
    discover_matching_files(settings)
    download_sources(settings, dry_run=False)
    import_metadata(settings)
    stats = ingest_sources(settings)
    typer.echo(f"Run complete: {stats['rows']} rows imported from {stats['files']} source file(s).")


@app.command("validate")
def validate() -> None:
    settings = load_settings()
    issues = []
    db_path = resolve_storage_dirs(settings)[-1]
    if not db_path.exists():
        issues.append(f"Database not initialized: {db_path}")
    if not issues:
        typer.echo("Validation passed.")
    else:
        typer.echo("Validation issues detected:")
        for issue in issues:
            typer.echo(f"- {issue}")
        raise typer.Exit(code=1)


@app.command("status")
def status() -> None:
    settings = load_settings()
    summary = inspect_status(settings)
    typer.echo(summary)


if __name__ == "__main__":
    app()
