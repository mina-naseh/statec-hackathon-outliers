import csv
import gzip
import json
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from time import monotonic, sleep

import duckdb
import py7zr
import pytest
from typer.testing import CliRunner

from lux_comext import cli
from lux_comext.pipeline import ingest as pipeline
from lux_comext.storage import ingestion as storage
from lux_comext.storage.database import init_database, inspect_status


@pytest.fixture
def config(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    return {
        "storage": {
            "raw_dir": str(raw_dir),
            "processed_dir": str(tmp_path / "processed"),
            "metadata_dir": str(tmp_path / "metadata"),
            "database_path": str(tmp_path / "comext.duckdb"),
        },
        "processing": {"batch_size": 2, "commit_interval_seconds": 3600},
    }


def write_source(config, rows, name="trade.csv"):
    path = Path(config["storage"]["raw_dir"]) / name
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["REPORTER", "FLOW", "PERIOD", "VALUE_IN_EUROS", "PRODUCT_NC", "NOTE"])
        writer.writerows(rows)
    return path


def query(config, sql):
    conn = duckdb.connect(config["storage"]["database_path"])
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def test_batches_visible_before_source_finishes_and_rerun_skips(config, monkeypatch):
    write_source(config, [
        ["LU", 1, 202501, 9007199254740993, "01012100", "first\nmultiline"],
        ["LU", 2, 202502, "", "01012101", "second"],
        ["LU", 1, 202501, 3, "01012102", "third"],
    ])
    original = pipeline.normalize_record
    calls = 0

    def observe(row):
        nonlocal calls
        calls += 1
        if calls == 3:
            assert query(config, "SELECT count(*) FROM comext_monthly") == [(2,)]
            assert query(
                config, "SELECT parsed_rows, retained_rows FROM ingestion_checkpoint"
            ) == [(2, 2)]
        return original(row)

    monkeypatch.setattr(pipeline, "normalize_record", observe)
    stats = pipeline.ingest_sources(config)
    assert stats["rows"] == stats["total_rows"] == 3
    assert stats["partitions"] == 2
    assert query(
        config, "SELECT value_in_euros FROM comext_monthly ORDER BY source_row_number"
    ) == [
        (9007199254740993,), (None,), (3,),
    ]
    extras = query(
        config, "SELECT extra_source_columns FROM comext_monthly ORDER BY source_row_number"
    )
    assert json.loads(extras[0][0])["NOTE"] == "first\nmultiline"
    parquet_path = config["storage"]["processed_dir"].replace("\\", "/") + "/**/*.parquet"
    assert query(config, f"SELECT count(*) FROM read_parquet('{parquet_path}')") == [(3,)]
    second = pipeline.ingest_sources(config)
    assert second["rows"] == 0
    assert second["skipped"] == 1
    assert second["total_rows"] == 3
    assert calls == 3
    init_database(config["storage"]["database_path"])
    assert query(config, "SELECT count(*) FROM comext_monthly") == [(3,)]
    assert "completed: files=1 scanned=3 saved=3" in inspect_status(config)


@pytest.mark.parametrize("after_commit", [False, True])
def test_resume_after_interruption_does_not_duplicate_or_reparse_committed_rows(
    config, monkeypatch, after_commit
):
    source = write_source(config, [
        ["LU", 1, 202501, 1, "01", "first\nmultiline"],
        ["DE", 1, 202501, 2, "02", "filtered"],
        ["LU", 1, 202502, 3, "03", "third"],
        ["LU", 2, 202502, 4, "04", "fourth"],
    ])
    if after_commit:
        original = pipeline.commit_batch

        def interrupt(*args):
            original(*args)
            raise KeyboardInterrupt("simulated stop after durable commit")

        monkeypatch.setattr(pipeline, "commit_batch", interrupt)
    else:
        original = pipeline.normalize_record

        def interrupt(row):
            if row["PRODUCT_NC"] == "03":
                raise KeyboardInterrupt("simulated stop before next batch")
            return original(row)

        monkeypatch.setattr(pipeline, "normalize_record", interrupt)
    with pytest.raises(KeyboardInterrupt):
        pipeline.ingest_sources(config)
    assert query(config, "SELECT count(*) FROM comext_monthly") == [(1,)]
    checkpoint = query(
        config, "SELECT parsed_rows, byte_offset, status FROM ingestion_checkpoint"
    )[0]
    assert checkpoint[0] == 2
    assert 0 < checkpoint[1] < source.stat().st_size
    assert checkpoint[2] == "failed"
    monkeypatch.undo()
    normalized = pipeline.normalize_record
    seen = []

    def record_resumed_row(row):
        seen.append(row["PRODUCT_NC"])
        return normalized(row)

    monkeypatch.setattr(pipeline, "normalize_record", record_resumed_row)
    stats = pipeline.ingest_sources(config)
    assert seen == ["03", "04"]
    assert stats["resumed"] == 1
    assert stats["rows"] == 2
    assert stats["total_rows"] == 3
    assert query(
        config, "SELECT source_row_number FROM comext_monthly ORDER BY source_row_number"
    ) == [(1,), (3,), (4,)]


def test_failed_checkpoint_update_rolls_back_inserted_batch(config, monkeypatch):
    write_source(config, [["LU", 1, 202501, i, str(i), ""] for i in range(4)])
    original_connection = storage.connection
    updates = 0

    class FailingConnection:
        def __init__(self, conn):
            self.conn = conn

        def register(self, *args):
            return self.conn.register(*args)

        def execute(self, sql, *args):
            nonlocal updates
            if "UPDATE ingestion_checkpoint SET byte_offset" in sql:
                updates += 1
                if updates == 2:
                    raise RuntimeError("simulated failure between insert and checkpoint")
            return self.conn.execute(sql, *args)

    @contextmanager
    def failing_connection(path):
        with original_connection(path) as conn:
            yield FailingConnection(conn)

    monkeypatch.setattr(storage, "connection", failing_connection)
    with pytest.raises(RuntimeError, match="between insert and checkpoint"):
        pipeline.ingest_sources(config)
    assert query(config, "SELECT count(*) FROM comext_monthly") == [(2,)]
    assert query(config, "SELECT parsed_rows FROM ingestion_checkpoint") == [(2,)]
    monkeypatch.undo()
    assert pipeline.ingest_sources(config)["total_rows"] == 4


def test_export_failure_keeps_rows_and_resumes_without_parsing(config, monkeypatch):
    write_source(config, [["LU", 1, 202501, 1, "01", ""]])

    def fail_export(*args):
        raise OSError("simulated disk error during export")

    monkeypatch.setattr(pipeline, "export_source", fail_export)
    with pytest.raises(OSError, match="disk error"):
        pipeline.ingest_sources(config)
    assert query(config, "SELECT count(*) FROM comext_monthly") == [(1,)]
    assert query(config, "SELECT at_eof, status FROM ingestion_checkpoint") == [(True, "failed")]
    monkeypatch.undo()

    def should_not_parse(row):
        pytest.fail("An EOF checkpoint must resume export without reparsing")

    monkeypatch.setattr(pipeline, "normalize_record", should_not_parse)
    stats = pipeline.ingest_sources(config)
    assert stats["total_rows"] == 1
    assert stats["rows"] == 0
    assert stats["partitions"] == 1


def test_changed_source_is_rejected_without_changing_saved_data(config):
    path = write_source(config, [["LU", 1, 202501, 1, "01", ""]])
    pipeline.ingest_sources(config)
    with path.open("a", encoding="utf-8") as handle:
        handle.write("LU,1,202501,2,02,new\n")
    with pytest.raises(ValueError, match="changed since its checkpoint"):
        pipeline.ingest_sources(config)
    assert query(config, "SELECT count(*) FROM comext_monthly") == [(1,)]


def test_legacy_view_is_migrated_once(config):
    conn = duckdb.connect(config["storage"]["database_path"])
    conn.execute("CREATE VIEW comext_monthly AS SELECT 202501 period, 'LU' declarant, "
                 "'trade.csv' source_archive, 123 value_in_euros, 'extra value' CUSTOM")
    conn.close()
    init_database(config["storage"]["database_path"])
    init_database(config["storage"]["database_path"])
    assert query(config, "SELECT period, value_in_euros FROM comext_monthly") == [(202501, 123)]
    extras = query(config, "SELECT extra_source_columns FROM comext_monthly")[0][0]
    assert json.loads(extras)["CUSTOM"] == "extra value"


def test_metadata_import_is_retryable_and_reads_gzip(config):
    write_source(config, [["LU", 1, 202501, 1, "01", ""]])
    metadata = Path(config["storage"]["metadata_dir"])
    metadata.mkdir()
    for name in ["partner.tsv.gz", "product_cn.tsv.gz"]:
        with gzip.open(metadata / name, "wt", encoding="utf-8") as handle:
            handle.write("Code\tLabel_EN\n01\tExample\n")
    pipeline.ingest_sources(config)
    pipeline.ingest_sources(config)
    assert query(config, "SELECT count(*) FROM partner_dim") == [(1,)]
    assert query(config, "SELECT count(*) FROM product_dim") == [(1,)]


@pytest.mark.parametrize("batch_size", [0, -1])
def test_invalid_batch_size_fails_before_ingestion(config, batch_size):
    config["processing"]["batch_size"] = batch_size
    with pytest.raises(ValueError, match="must be positive"):
        pipeline.ingest_sources(config)


def test_live_read_from_another_process_and_resume_after_hard_stop(config, tmp_path):
    write_source(config, [["LU", 1, 202501, i, str(i), ""] for i in range(4)])
    ready = tmp_path / "ready"
    script = """
import json, sys
from pathlib import Path
from lux_comext.pipeline import ingest
settings = json.loads(sys.argv[1])
original = ingest.normalize_record
def pause(row):
    if row['PRODUCT_NC'] == '2':
        Path(sys.argv[2]).write_text('ready')
        sys.stdin.readline()
    return original(row)
ingest.normalize_record = pause
ingest.ingest_sources(settings)
"""
    env = dict(os.environ, PYTHONPATH=str(Path(pipeline.__file__).resolve().parents[2]))
    process = subprocess.Popen(
        [sys.executable, "-c", script, json.dumps(config), str(ready)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
    )
    try:
        deadline = monotonic() + 15
        while not ready.exists() and process.poll() is None and monotonic() < deadline:
            sleep(0.01)
        assert ready.exists(), "Child ingest did not reach the parsing pause"
        # A separate process can query saved batches while ingest parses its next batch.
        assert query(config, "SELECT count(*) FROM comext_monthly") == [(2,)]
        with pytest.raises(RuntimeError, match="already using"):
            pipeline.ingest_sources(config)
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)
    assert query(config, "SELECT parsed_rows, status FROM ingestion_checkpoint") == [(2, "parsing")]
    assert pipeline.ingest_sources(config)["total_rows"] == 4
    assert query(config, "SELECT status FROM ingestion_log ORDER BY started_at") == [
        ("interrupted",), ("completed",),
    ]


def test_archive_data_is_committed_before_extracting_next_archive(config, tmp_path):
    raw = Path(config["storage"]["raw_dir"])
    source = tmp_path / "first.csv"
    source.write_text("REPORTER,FLOW,PERIOD\nLU,1,202501\n", encoding="utf-8")
    with py7zr.SevenZipFile(raw / "a.7z", "w") as archive:
        archive.write(source, arcname="first.csv")
    broken = raw / "b.7z"
    broken.write_bytes(b"invalid archive")
    with pytest.raises(py7zr.Bad7zFile):
        pipeline.ingest_sources(config)
    assert query(config, "SELECT count(*) FROM comext_monthly") == [(1,)]
    assert query(config, "SELECT status FROM ingestion_checkpoint") == [("completed",)]
    broken.unlink()
    with py7zr.SevenZipFile(broken, "w") as archive:
        archive.write(source, arcname="second.csv")
    stats = pipeline.ingest_sources(config)
    assert stats["skipped"] == 1
    assert stats["rows"] == 1
    assert stats["total_rows"] == 2


def test_cli_reports_progress_and_preserves_rows_on_reinitialization(config, monkeypatch):
    write_source(config, [["LU", 1, 202501, 1, "01", ""]])
    monkeypatch.setattr(cli, "load_settings", lambda: config)
    monkeypatch.setattr(cli, "setup_logging", lambda **kwargs: None)
    monkeypatch.chdir(config["storage"]["raw_dir"])
    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest"])
    assert result.exit_code == 0, result.output
    assert "Saved 1 new rows" in result.output
    assert "Database total: 1 rows" in result.output
    result = runner.invoke(cli.app, ["status"])
    assert result.exit_code == 0, result.output
    assert "Rows: 1" in result.output
    assert "completed: files=1 scanned=1 saved=1" in result.output
    assert runner.invoke(cli.app, ["validate"]).exit_code == 0
    assert runner.invoke(cli.app, ["init-db"]).exit_code == 0
    assert query(config, "SELECT count(*) FROM comext_monthly") == [(1,)]
