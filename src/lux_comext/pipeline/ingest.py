from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from luxtrade.logging_setup import timed

from ..archive.extractor import iter_extracted_archives
from ..config import resolve_storage_dirs
from ..metadata.importer import import_codelists
from ..parsing.reader import normalize_record
from ..storage.database import init_database
from ..storage.ingestion import (
    FACT_SCHEMA,
    commit_batch,
    connection,
    export_source,
    ingestion_lock,
    record_failure,
    start_source,
)

logger = logging.getLogger(__name__)


def _iter_data_files(root: Path) -> list[Path]:
    return sorted(root.rglob("*.dat")) + sorted(root.rglob("*.csv"))


def _decoded_lines(handle):
    # csv consumes complete logical records, including quoted multiline values.
    # The binary handle therefore gives an exact, seekable offset after each row.
    for line in iter(handle.readline, b""):
        yield line.decode("utf-8")


def _trade_record(row: dict, path: Path, checkpoint: dict, preserve_columns: bool) -> dict | None:
    if None in row:
        checkpoint["missing_declarant"] += 1
        return None
    normalized = normalize_record(row)
    if not normalized.get("declarant"):
        checkpoint["missing_declarant"] += 1
        return None
    if normalized["declarant"] != "LU":
        checkpoint["non_luxembourg"] += 1
        return None
    if normalized.get("flow") not in {1, 2}:
        checkpoint["invalid_flow"] += 1
        return None
    period = normalized.get("period")
    if period is None or not 100001 <= period <= 999912 or not 1 <= period % 100 <= 12:
        checkpoint["invalid_period"] += 1
        return None
    record = {name: normalized.get(name) for name in FACT_SCHEMA.names}
    try:
        record["sup_quantity"] = int(normalized.get("sup_quantity") or "")
    except (TypeError, ValueError):
        record["sup_quantity"] = None
    record.update(
        source_archive=path.name,
        source_row_number=checkpoint["parsed_rows"],
        extra_source_columns=json.dumps(row, ensure_ascii=False) if preserve_columns else None,
        ingestion_source_key=checkpoint["source_key"],
    )
    return record


def _ingest_file(
    path: Path, database_path: Path, processed_dir: Path,
    batch_size: int, commit_interval: float, preserve_columns: bool,
) -> dict:
    signature = json.dumps({"parser_version": 1, "preserve_source_columns": preserve_columns})
    checkpoint = start_source(database_path, path, signature)
    if checkpoint["status"] == "completed":
        logger.info("Skipping completed source=%s saved_rows=%d", path, checkpoint["retained_rows"])
        return {"rows": 0, "files": 0, "skipped": 1, "resumed": 0, "partitions": 0}
    previous_rows = checkpoint["retained_rows"]
    resumed = int(checkpoint["byte_offset"] > 0)
    logger.info(
        "%s source=%s byte_offset=%d scanned=%d saved=%d",
        "Resuming" if resumed else "Parsing", path, checkpoint["byte_offset"],
        checkpoint["parsed_rows"], previous_rows,
    )
    started = perf_counter()

    def save_batch(records: list[dict], byte_offset: int, at_eof: bool = False) -> None:
        stat = path.stat()
        if stat.st_size != checkpoint["file_size"] or stat.st_mtime_ns != checkpoint["modified_ns"]:
            raise ValueError(f"Source changed while parsing: {path}; saved batches are unchanged")
        checkpoint["byte_offset"] = byte_offset
        checkpoint["at_eof"] = at_eof
        commit_batch(database_path, checkpoint, records)

    try:
        if not checkpoint["at_eof"]:
            with path.open("rb") as handle:
                reader = csv.DictReader(_decoded_lines(handle), strict=True)
                fieldnames = reader.fieldnames
                if fieldnames:
                    fieldnames[0] = fieldnames[0].lstrip("\ufeff")
                if checkpoint["byte_offset"]:
                    handle.seek(checkpoint["byte_offset"])
                    reader = csv.DictReader(
                        _decoded_lines(handle), fieldnames=fieldnames, strict=True
                    )
                records = []
                batch_scanned = 0
                last_commit = last_progress = perf_counter()
                for row in reader:
                    checkpoint["parsed_rows"] += 1
                    batch_scanned += 1
                    record = _trade_record(row, path, checkpoint, preserve_columns)
                    if record is not None:
                        records.append(record)
                        checkpoint["retained_rows"] += 1
                    now = perf_counter()
                    if checkpoint["parsed_rows"] % 100_000 == 0 or now - last_progress >= 10:
                        logger.info(
                            "Parsing %s: scanned=%d retained=%d elapsed=%.1fs",
                            path.name, checkpoint["parsed_rows"], checkpoint["retained_rows"],
                            now - started,
                        )
                        last_progress = now
                    if batch_scanned >= batch_size or now - last_commit >= commit_interval:
                        save_batch(records, handle.tell())
                        logger.info(
                            "Committed source=%s batch_rows=%d scanned=%d saved=%d byte_offset=%d",
                            path.name, len(records), checkpoint["parsed_rows"],
                            checkpoint["retained_rows"], checkpoint["byte_offset"],
                        )
                        records.clear()
                        batch_scanned = 0
                        last_commit = perf_counter()
                save_batch(records, handle.tell(), at_eof=True)
        logger.info(
            "Parsing complete: source=%s parsed=%d retained=%d missing_declarant=%d "
            "non_luxembourg=%d invalid_flow=%d invalid_period=%d",
            path.name, checkpoint["parsed_rows"], checkpoint["retained_rows"],
            checkpoint["missing_declarant"], checkpoint["non_luxembourg"],
            checkpoint["invalid_flow"], checkpoint["invalid_period"],
        )
        files = export_source(database_path, processed_dir, checkpoint)
        logger.info(
            "Finished source=%s saved=%d partitions=%d elapsed=%.1fs",
            path, checkpoint["retained_rows"], len(files), perf_counter() - started,
        )
        return {
            "rows": checkpoint["retained_rows"] - previous_rows, "files": 1,
            "skipped": 0, "resumed": resumed, "partitions": len(files),
        }
    except BaseException as exc:
        try:
            record_failure(database_path, checkpoint["source_key"], exc)
        except Exception:
            logger.exception("Could not record failure for source=%s", path)
        logger.error(
            "Source interrupted: %s; committed batches are saved. Rerun ingest to resume.", path
        )
        raise


def _ingest_sources(settings: dict) -> dict:
    raw_dir, _, processed_dir, metadata_dir, database_path = resolve_storage_dirs(settings)
    processing = settings.get("processing", {})
    batch_size = int(processing.get("batch_size", 200_000))
    commit_interval = float(processing.get("commit_interval_seconds", 10))
    if batch_size < 1 or commit_interval <= 0:
        raise ValueError("processing.batch_size and commit_interval_seconds must be positive")
    preserve_columns = processing.get("preserve_source_columns", True)
    logger.info(
        "Ingest started: database=%s raw=%s batch_size=%d commit_interval=%.1fs",
        database_path, raw_dir, batch_size, commit_interval,
    )
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    init_database(database_path)
    run_id = str(uuid4())
    with connection(database_path) as conn:
        conn.execute(
            "UPDATE ingestion_log SET status = 'interrupted', finished_at = current_timestamp "
            "WHERE source_name = 'ingest' AND status = 'running'"
        )
        conn.execute(
            "INSERT INTO ingestion_log (run_id, source_name, status, started_at) "
            "VALUES (?, 'ingest', 'running', current_timestamp)", [run_id]
        )
    stats = {"rows": 0, "files": 0, "skipped": 0, "resumed": 0, "partitions": 0}
    seen = set()

    def ingest_candidates(candidates) -> None:
        for candidate in candidates:
            candidate = candidate.resolve()
            if candidate in seen or candidate.name.startswith("."):
                continue
            seen.add(candidate)
            result = _ingest_file(
                candidate, database_path, processed_dir,
                batch_size, commit_interval, preserve_columns,
            )
            for key in stats:
                stats[key] += result[key]

    try:
        # Export and checkpoint one archive's data before extracting the next.
        for entries in iter_extracted_archives(raw_dir):
            ingest_candidates(
                path for path in entries
                if path.is_file() and path.suffix.lower() in {".csv", ".dat"}
            )
        logger.info("Scanning existing data files in %s", raw_dir)
        ingest_candidates(_iter_data_files(raw_dir))
        logger.info("Importing metadata codelists from %s", metadata_dir)
        imported = import_codelists(database_path, metadata_dir)
        logger.info("Metadata import complete: %s", imported)
        with connection(database_path) as conn:
            stats["total_rows"] = conn.execute("SELECT count(*) FROM comext_monthly").fetchone()[0]
            conn.execute(
                "UPDATE ingestion_log SET status = 'completed', finished_at = current_timestamp "
                "WHERE run_id = ?", [run_id]
            )
    except BaseException as exc:
        try:
            with connection(database_path) as conn:
                conn.execute(
                    "UPDATE ingestion_log SET status = 'failed', error_message = ?, "
                    "finished_at = current_timestamp WHERE run_id = ?",
                    [str(exc) or type(exc).__name__, run_id],
                )
        except Exception:
            logger.exception("Could not record failed ingestion run=%s", run_id)
        raise
    logger.info("Ingest complete: %s", stats)
    return stats


@timed
def ingest_sources(settings: dict) -> dict:
    database_path = resolve_storage_dirs(settings)[-1]
    with ingestion_lock(database_path):
        return _ingest_sources(settings)
