from __future__ import annotations

from pathlib import Path

import duckdb

from ..config import resolve_storage_dirs
from .ingestion import connection, initialize_ingestion


def _create_metadata_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_archive (
            source_archive_id INTEGER,
            source_path VARCHAR,
            source_url VARCHAR,
            filename VARCHAR,
            reference_period VARCHAR,
            file_size BIGINT,
            source_modified_at TIMESTAMP,
            checksum VARCHAR,
            local_path VARCHAR,
            downloaded_at TIMESTAMP,
            parsed_record_count BIGINT,
            imported_record_count BIGINT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion_log (
            run_id VARCHAR,
            source_name VARCHAR,
            status VARCHAR,
            error_message VARCHAR,
            processed_periods VARCHAR,
            started_at TIMESTAMP,
            finished_at TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS partner_dim (
            partner_code VARCHAR,
            label_en VARCHAR,
            label_fr VARCHAR,
            label_de VARCHAR,
            is_standard_code BOOLEAN,
            source VARCHAR,
            note VARCHAR,
            ec_corporate_code VARCHAR,
            ec_corporate_uri VARCHAR,
            codelist_version VARCHAR
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS product_dim (
            product_code VARCHAR,
            label_en VARCHAR,
            label_fr VARCHAR,
            label_de VARCHAR,
            level INTEGER,
            parent_code VARCHAR,
            chapter VARCHAR,
            is_not_elsewhere_specified BOOLEAN,
            classification_id VARCHAR,
            classification_version VARCHAR
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS product_correspondence (
            source_classification VARCHAR,
            source_version VARCHAR,
            source_product_code VARCHAR,
            target_classification VARCHAR,
            target_version VARCHAR,
            target_product_code VARCHAR,
            relationship_type VARCHAR,
            source_metadata_file VARCHAR
        );
        """
    )


def init_database(database_path: str | Path) -> None:
    target = Path(database_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with connection(target) as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            _create_metadata_tables(conn)
            initialize_ingestion(conn)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise


def inspect_status(settings: dict) -> str:
    database_path = resolve_storage_dirs(settings)[-1]
    if not database_path.exists():
        return f"Database not initialized: {database_path}"
    with connection(database_path) as conn:
        tables = conn.execute(
            "SELECT COUNT(*) FROM duckdb_tables() WHERE internal = FALSE"
        ).fetchone()[0]
        views = conn.execute(
            "SELECT COUNT(*) FROM duckdb_views() WHERE internal = FALSE"
        ).fetchone()[0]
        rows = conn.execute("SELECT COUNT(*) FROM comext_monthly").fetchone()[0]
        has_checkpoints = conn.execute(
            "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'ingestion_checkpoint'"
        ).fetchone()[0]
        summary = []
        if has_checkpoints:
            summary = conn.execute(
                "SELECT status, count(*), sum(parsed_rows), sum(retained_rows) "
                "FROM ingestion_checkpoint GROUP BY status ORDER BY status"
            ).fetchall()
    progress = ""
    if summary:
        progress = "\n" + "\n".join(
            f"{status}: files={count} scanned={scanned} saved={saved}"
            for status, count, scanned, saved in summary
        )
    return f"Tables: {tables} | Views: {views} | Rows: {rows} | Database: {database_path}{progress}"
