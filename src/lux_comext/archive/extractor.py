from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from pathlib import Path
from threading import Event, Thread
from time import perf_counter

import py7zr

from luxtrade.logging_setup import timed

logger = logging.getLogger(__name__)


def _matching_extraction(destination: Path, members: list[dict]) -> bool:
    return isinstance(members, list) and bool(members) and all(
        isinstance(member, dict) and "name" in member and "size" in member
        and isinstance(member["name"], str)
        and (destination / member["name"]).is_file()
        and (destination / member["name"]).stat().st_size == member["size"]
        for member in members
    )


@timed
def extract_archive(archive_path: Path, destination: Path) -> list[Path]:
    archive_path = Path(archive_path)
    destination = Path(destination)
    stat = archive_path.stat()
    fingerprint = {"size": stat.st_size, "modified_ns": stat.st_mtime_ns}
    marker = destination / ".extraction.json"
    cached = None
    if marker.exists():
        try:
            cached = json.loads(marker.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            logger.warning("Ignoring invalid extraction checkpoint: %s", marker)
    if (
        isinstance(cached, dict)
        and cached.get("archive") == fingerprint
        and _matching_extraction(destination, cached.get("members", []))
    ):
        logger.info(
            "Reusing completed extraction archive=%s destination=%s", archive_path, destination
        )
        return [destination / member["name"] for member in cached["members"]]
    logger.info(
        "Extracting archive=%s bytes=%d destination=%s",
        archive_path,
        archive_path.stat().st_size,
        destination,
    )
    destination.mkdir(parents=True, exist_ok=True)
    started = perf_counter()
    stopped = Event()

    def report_progress() -> None:
        while not stopped.wait(10):
            logger.info(
                "Still extracting archive=%s elapsed=%.1fs", archive_path, perf_counter() - started
            )

    reporter = Thread(target=report_progress, name="archive-progress", daemon=True)
    reporter.start()
    try:
        with py7zr.SevenZipFile(archive_path, mode="r") as archive:
            members = [
                {"name": member.filename, "size": member.uncompressed}
                for member in archive.list() if not member.is_directory
            ]
            # Accept complete files extracted by the old ingest implementation.
            if cached is None and _matching_extraction(destination, members):
                logger.info("Reusing existing extracted files archive=%s", archive_path)
            else:
                archive.extractall(path=str(destination))
        if members and not _matching_extraction(destination, members):
            raise ValueError(f"Extraction is incomplete for {archive_path}")
        current_stat = archive_path.stat()
        if current_stat.st_size != stat.st_size or current_stat.st_mtime_ns != stat.st_mtime_ns:
            raise ValueError(f"Archive changed during extraction: {archive_path}")
        temporary = marker.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps({"archive": fingerprint, "members": members}), encoding="utf-8"
        )
        temporary.replace(marker)
        entries = [destination / member["name"] for member in members]
    finally:
        stopped.set()
        reporter.join()
    logger.info(
        "Extracted archive=%s entries=%d elapsed=%.1fs",
        archive_path,
        len(entries),
        perf_counter() - started,
    )
    return entries


def iter_extracted_archives(raw_dir: str | Path) -> Iterator[list[Path]]:
    raw_dir = Path(raw_dir)
    archives = sorted(raw_dir.rglob("*.7z"))
    for index, archive in enumerate(archives, start=1):
        logger.info("Extracting archive [%d/%d]: %s", index, len(archives), archive)
        target = archive.parent / archive.stem
        target.mkdir(parents=True, exist_ok=True)
        yield extract_archive(archive, target)


def extract_downloaded_archives(raw_dir: str | Path) -> list[Path]:
    return [path for entries in iter_extracted_archives(raw_dir) for path in entries]
