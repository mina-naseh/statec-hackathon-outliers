import logging
from threading import enumerate as active_threads

import py7zr
import pytest

from lux_comext.archive.extractor import extract_archive, extract_downloaded_archives
from lux_comext.pipeline.ingest import ingest_sources


def test_archive_logs_progress_and_extracts_data(tmp_path, caplog):
    source = tmp_path / "sample.csv"
    source.write_text("REPORTER,PERIOD\nLU,202501\n", encoding="utf-8")
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    with py7zr.SevenZipFile(raw_dir / "sample.7z", "w") as archive:
        archive.write(source, arcname="sample.csv")

    with caplog.at_level(logging.INFO):
        extracted = extract_downloaded_archives(raw_dir)

    assert (raw_dir / "sample" / "sample.csv") in extracted
    assert "Extracting archive [1/1]" in caplog.text
    assert "Extracted archive=" in caplog.text
    assert "elapsed=" in caplog.text
    assert not any(thread.name == "archive-progress" for thread in active_threads())


def test_archive_failure_logs_error_and_stops_reporter(tmp_path, caplog):
    (tmp_path / "broken.7z").write_bytes(b"invalid archive")

    with caplog.at_level(logging.INFO), pytest.raises(py7zr.Bad7zFile):
        extract_downloaded_archives(tmp_path)

    assert "broken.7z" in caplog.text
    assert "failed extract_archive" in caplog.text
    assert not any(thread.name == "archive-progress" for thread in active_threads())


def test_complete_extractions_are_reused_and_missing_files_are_reextracted(tmp_path, monkeypatch):
    source = tmp_path / "sample.csv"
    source.write_text("REPORTER,PERIOD\nLU,202501\n", encoding="utf-8")
    archive_path = tmp_path / "sample.7z"
    with py7zr.SevenZipFile(archive_path, "w") as archive:
        archive.write(source, arcname="sample.csv")
    destination = tmp_path / "extracted"
    extract_archive(archive_path, destination)
    extracted = destination / "sample.csv"
    modified = extracted.stat().st_mtime_ns
    original = py7zr.SevenZipFile.extractall
    calls = []

    def track_extraction(self, *args, **kwargs):
        calls.append(True)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(py7zr.SevenZipFile, "extractall", track_extraction)
    extract_archive(archive_path, destination)
    assert calls == []
    assert extracted.stat().st_mtime_ns == modified
    # An unmarked but complete extraction from the previous ingest is also reusable.
    (destination / ".extraction.json").unlink()
    extract_archive(archive_path, destination)
    assert calls == []
    extracted.unlink()
    extract_archive(archive_path, destination)
    assert calls == [True]
    assert extracted.read_text(encoding="utf-8") == source.read_text(encoding="utf-8")


def test_parsing_logs_progress_when_all_rows_are_filtered(tmp_path, caplog):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "trade.csv").write_text(
        "REPORTER,FLOW,PERIOD\n" + "DE,1,202501\n" * 100_000, encoding="utf-8"
    )
    settings = {
        "storage": {
            "raw_dir": str(raw_dir),
            "processed_dir": str(tmp_path / "processed"),
            "metadata_dir": str(tmp_path / "metadata"),
            "database_path": str(tmp_path / "test.duckdb"),
        }
    }

    with caplog.at_level(logging.INFO):
        stats = ingest_sources(settings)

    assert stats["rows"] == stats["total_rows"] == 0
    assert stats["files"] == 1
    assert "Parsing trade.csv: scanned=100000 retained=0" in caplog.text
    assert "non_luxembourg=100000" in caplog.text
    assert "end ingest_sources" in caplog.text
