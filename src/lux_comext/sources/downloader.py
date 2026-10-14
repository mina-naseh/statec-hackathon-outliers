from __future__ import annotations

import logging
from pathlib import Path

import requests

from luxtrade.logging_setup import timed

from ..config import resolve_storage_dirs
from .catalogue import discover_matching_files

logger = logging.getLogger(__name__)


def build_download_targets(settings: dict) -> list[dict]:
    return discover_matching_files(settings)


def validate_download_url(url: str) -> None:
    """Check the remote file without downloading its body; fail on invalid links."""
    logger.debug("HEAD url=%s timeout=60", url)
    with requests.head(url, allow_redirects=True, timeout=60) as response:
        if response.status_code not in {405, 501}:
            _check_file_response(response, url)
            return
        logger.debug(
            "HEAD unsupported status=%s; using streamed GET url=%s", response.status_code, url
        )
    # Some servers do not support HEAD. Close a streamed GET after its headers.
    with requests.get(url, stream=True, timeout=60) as response:
        _check_file_response(response, url)


def _check_file_response(response: requests.Response, url: str) -> None:
    logger.debug(
        "Validation response url=%s status=%s content_type=%s",
        url,
        response.status_code,
        response.headers.get("Content-Type", ""),
    )
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
    if response.status_code not in {200, 206} or content_type in {
        "text/html",
        "application/xhtml+xml",
        "application/json",
        "application/xml",
        "text/xml",
    }:
        raise ValueError(
            f"Invalid download URL (HTTP {response.status_code}, type {content_type!r}): {url}"
        )


@timed
def download_sources(settings: dict, dry_run: bool = False) -> list[dict]:
    logger.info("Starting download planning dry_run=%s", dry_run)
    targets = build_download_targets(settings)
    if dry_run:
        logger.info("Validating %d download URLs without downloading archives", len(targets))
        for index, target in enumerate(targets, start=1):
            logger.info("Validating [%d/%d] file=%s", index, len(targets), target["name"])
            validate_download_url(target["url"])
        logger.info("Dry run complete: %d download URLs validated", len(targets))
        return [{key: target[key] for key in ("source_name", "name", "url")} for target in targets]

    raw_dir, _, _, _, _ = resolve_storage_dirs(settings)
    raw_dir.mkdir(parents=True, exist_ok=True)
    downloaded: list[dict] = []
    for target in targets:
        name = target["name"]
        url = target["url"]
        file_name = Path(name).name
        local_dir = raw_dir / target["source_name"]
        local_dir.mkdir(parents=True, exist_ok=True)
        local_path = local_dir / file_name
        if local_path.exists():
            logger.info("Skipping existing file=%s", local_path)
            downloaded.append(
                {
                    "source_name": target["source_name"],
                    "name": file_name,
                    "url": url,
                    "local_path": str(local_path),
                }
            )
            continue
        logger.info("Downloading url=%s destination=%s", url, local_path)
        byte_count = 0
        with requests.get(url, stream=True, timeout=180) as response:
            logger.debug("Download response status=%s url=%s", response.status_code, url)
            response.raise_for_status()
            with local_path.with_suffix(local_path.suffix + ".part").open("wb") as handle:
                for chunk in response.iter_content(chunk_size=65536):
                    if chunk:
                        handle.write(chunk)
                        byte_count += len(chunk)
        local_path.with_suffix(local_path.suffix + ".part").replace(local_path)
        logger.info("Download complete file=%s bytes=%d", local_path, byte_count)
        downloaded.append(
            {
                "source_name": target["source_name"],
                "name": file_name,
                "url": url,
                "local_path": str(local_path),
            }
        )
    logger.info("Download run complete: %d files available locally", len(downloaded))
    return downloaded
