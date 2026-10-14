from __future__ import annotations

import hashlib
import json
import logging
import os
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pyarrow as pa

logger = logging.getLogger(__name__)

FACT_SCHEMA = pa.schema(
    [
        ("period", pa.int32()),
        ("declarant", pa.string()),
        ("partner", pa.string()),
        ("product_nc", pa.string()),
        ("flow", pa.int32()),
        ("value_in_euros", pa.int64()),
        ("quantity_in_kg", pa.int64()),
        ("sup_quantity", pa.int64()),
        ("source_archive", pa.string()),
        ("source_row_number", pa.int64()),
        ("classification_version", pa.string()),
        ("extra_source_columns", pa.string()),
        ("ingestion_source_key", pa.string()),
    ]
)
FACT_COLUMNS = ", ".join(FACT_SCHEMA.names[:-1])


@contextmanager
def ingestion_lock(database_path: Path):
    """Allow one ingest writer; the OS releases this lock even after a hard stop."""
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with database_path.with_suffix(".ingest.lock").open("a+b") as handle:
        if handle.seek(0, 2) == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            def acquire():
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)

            def release():
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            def acquire():
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

            def release():
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        try:
            acquire()
        except OSError as exc:
            raise RuntimeError(f"Another ingest process is already using {database_path}") from exc
        try:
            yield
        finally:
            handle.seek(0)
            release()


@contextmanager
def connection(database_path: Path):
    """Release DuckDB's file lock between batches and while parsing/extracting."""
    conn = duckdb.connect(str(database_path))
    try:
        yield conn
    finally:
        conn.close()


def initialize_ingestion(conn: duckdb.DuckDBPyConnection) -> None:
    """Migrate the old view once, then keep it backed by durable database rows."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS comext_records (
            period INTEGER, declarant VARCHAR, partner VARCHAR, product_nc VARCHAR,
            flow INTEGER, value_in_euros BIGINT, quantity_in_kg BIGINT, sup_quantity BIGINT,
            source_archive VARCHAR, source_row_number BIGINT, classification_version VARCHAR,
            extra_source_columns JSON, ingestion_source_key VARCHAR,
            PRIMARY KEY (ingestion_source_key, source_row_number)
        );
        CREATE TABLE IF NOT EXISTS ingestion_checkpoint (
            source_key VARCHAR PRIMARY KEY, source_path VARCHAR UNIQUE,
            file_size BIGINT, modified_ns BIGINT, parser_signature VARCHAR,
            byte_offset BIGINT DEFAULT 0, parsed_rows BIGINT DEFAULT 0,
            retained_rows BIGINT DEFAULT 0, missing_declarant BIGINT DEFAULT 0,
            non_luxembourg BIGINT DEFAULT 0, invalid_flow BIGINT DEFAULT 0,
            invalid_period BIGINT DEFAULT 0, at_eof BOOLEAN DEFAULT FALSE,
            status VARCHAR DEFAULT 'parsing', error_message VARCHAR,
            parquet_files JSON DEFAULT '[]', updated_at TIMESTAMP DEFAULT current_timestamp
        );
        """
    )
    existing = conn.execute(
        "SELECT sql FROM duckdb_views() WHERE view_name = 'comext_monthly' AND internal = FALSE"
    ).fetchone()
    if existing and "comext_records" not in existing[0]:
        columns = {
            item[0] for item in conn.execute("SELECT * FROM comext_monthly LIMIT 0").description
        }
        expressions = []
        for field in FACT_SCHEMA:
            if field.name == "ingestion_source_key":
                expressions.append("'__legacy__'")
            elif field.name == "source_row_number":
                expressions.append("row_number() OVER ()")
            elif field.name == "extra_source_columns":
                expressions.append("to_json(old_row)")
            elif field.name in columns:
                sql_type = "BIGINT" if pa.types.is_integer(field.type) else "VARCHAR"
                expressions.append(f'TRY_CAST("{field.name}" AS {sql_type})')
            else:
                expressions.append("NULL")
        conn.execute(
            "INSERT INTO comext_records SELECT "
            + ", ".join(expressions)
            + " FROM comext_monthly old_row"
        )
    conn.execute(
        f"CREATE OR REPLACE VIEW comext_monthly AS SELECT {FACT_COLUMNS} FROM comext_records"
    )


def source_key(path: Path) -> str:
    return hashlib.sha256(str(path.resolve()).encode("utf-8")).hexdigest()


def start_source(database_path: Path, path: Path, parser_signature: str) -> dict:
    stat = path.stat()
    key = source_key(path)
    with connection(database_path) as conn:
        cursor = conn.execute("SELECT * FROM ingestion_checkpoint WHERE source_key = ?", [key])
        values = cursor.fetchone()
        if values:
            checkpoint = dict(zip([item[0] for item in cursor.description], values))
            if (
                checkpoint["file_size"] != stat.st_size
                or checkpoint["modified_ns"] != stat.st_mtime_ns
                or checkpoint["parser_signature"] != parser_signature
            ):
                raise ValueError(
                    f"Source or parser settings changed since its checkpoint: {path}. "
                    "Restore the original file/settings, or ingest the revised source into "
                    "a separate database to avoid mixing versions. Saved rows are unchanged."
                )
            return checkpoint
        conn.execute("BEGIN TRANSACTION")
        try:
            # Replace legacy rows for this source as it starts its tracked import.
            conn.execute(
                "DELETE FROM comext_records WHERE ingestion_source_key = '__legacy__' "
                "AND source_archive = ?",
                [path.name],
            )
            conn.execute(
                "INSERT INTO ingestion_checkpoint "
                "(source_key, source_path, file_size, modified_ns, parser_signature) "
                "VALUES (?, ?, ?, ?, ?)",
                [key, str(path.resolve()), stat.st_size, stat.st_mtime_ns, parser_signature],
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        cursor = conn.execute("SELECT * FROM ingestion_checkpoint WHERE source_key = ?", [key])
        return dict(zip([item[0] for item in cursor.description], cursor.fetchone()))


def commit_batch(database_path: Path, checkpoint: dict, records: list[dict]) -> None:
    """Commit observations and the corresponding CSV position in one transaction."""
    with connection(database_path) as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            if records:
                conn.register("batch_rows", pa.Table.from_pylist(records, schema=FACT_SCHEMA))
                conn.execute("INSERT INTO comext_records SELECT * FROM batch_rows")
            conn.execute(
                """
                UPDATE ingestion_checkpoint SET byte_offset = ?, parsed_rows = ?,
                    retained_rows = ?, missing_declarant = ?, non_luxembourg = ?,
                    invalid_flow = ?, invalid_period = ?, at_eof = ?, status = ?,
                    error_message = NULL, updated_at = current_timestamp
                WHERE source_key = ?
                """,
                [
                    checkpoint["byte_offset"], checkpoint["parsed_rows"],
                    checkpoint["retained_rows"], checkpoint["missing_declarant"],
                    checkpoint["non_luxembourg"], checkpoint["invalid_flow"],
                    checkpoint["invalid_period"], checkpoint["at_eof"],
                    "exporting" if checkpoint["at_eof"] else "parsing",
                    checkpoint["source_key"],
                ],
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise


def record_failure(database_path: Path, key: str, error: BaseException) -> None:
    with connection(database_path) as conn:
        conn.execute(
            "UPDATE ingestion_checkpoint SET status = 'failed', error_message = ?, "
            "updated_at = current_timestamp WHERE source_key = ?",
            [str(error) or type(error).__name__, key],
        )


def export_source(database_path: Path, processed_dir: Path, checkpoint: dict) -> list[str]:
    """Retryable exports; committed database rows remain authoritative on failure."""
    key = checkpoint["source_key"]
    with connection(database_path) as conn:
        periods = conn.execute(
            "SELECT DISTINCT period FROM comext_records WHERE ingestion_source_key = ? "
            "ORDER BY period", [key]
        ).fetchall()
    files = []
    for (period,) in periods:
        year, month = divmod(period, 100)
        target = processed_dir / f"year={year}" / f"month={month:02d}" / f"source-{key}.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(".parquet.tmp")
        logger.info(
            "Exporting source=%s period=%s partition=%s", checkpoint["source_path"], period, target
        )
        # COPY streams from DuckDB without loading a whole source into Python memory.
        escaped_target = str(temporary).replace("'", "''")
        with connection(database_path) as conn:
            conn.execute(
                f"COPY (SELECT {FACT_COLUMNS} FROM comext_records "
                f"WHERE ingestion_source_key = '{key}' AND period = {int(period)}) "
                f"TO '{escaped_target}' (FORMAT PARQUET)"
            )
        temporary.replace(target)
        files.append(str(target))
    with connection(database_path) as conn:
        conn.execute(
            "UPDATE ingestion_checkpoint SET status = 'completed', parquet_files = ?, "
            "error_message = NULL, updated_at = current_timestamp WHERE source_key = ?",
            [json.dumps(files), key],
        )
    return files
