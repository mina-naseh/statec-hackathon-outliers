from __future__ import annotations

import csv
import io
import logging
import re
from html import unescape
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlsplit

import requests

from luxtrade.logging_setup import timed

from ..config import match_patterns

logger = logging.getLogger(__name__)


def build_file_url(files_api: str, file_name: str) -> str:
    request = requests.Request("GET", files_api, params={"file": file_name})
    return request.prepare().url


def parse_inventory_html(html: str) -> list[dict]:
    """Read file paths and download links from Eurostat's HTML listing."""
    rows: list[dict] = []
    by_name: dict[str, dict] = {}

    for match in re.finditer(
        r"<a\b[^>]*href\s*=\s*(?P<quote>[\"'])(?P<href>.*?)(?P=quote)[^>]*>(?P<text>.*?)</a>",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        href = unescape(match.group("href"))
        query = parse_qs(urlsplit(href).query)
        file_values = query.get("downfile") or query.get("file")
        if not file_values:
            continue
        file_name = file_values[0].strip()
        if not file_name or file_name.endswith("/"):
            continue

        if file_name in by_name:
            if "downfile" in query:
                by_name[file_name]["url"] = href
            continue
        row = {"name": file_name, "size": "", "type": "file", "date": "", "url": href}
        by_name[file_name] = row
        rows.append(row)

    return rows


@timed
def fetch_inventory(files_api: str, directory: str) -> list[dict]:
    """Fetch a fresh listing and resolve its published download links."""
    params = {
        "sort": "1",
        "dir": directory,
    }
    listing_url = files_api.rstrip("/") + "/"
    logger.info("Fetching fresh Eurostat listing directory=%s", directory)
    logger.debug("GET url=%s params=%s timeout=60", listing_url, params)
    response = requests.get(listing_url, params=params, timeout=60)
    logger.debug("Listing response url=%s status=%s", response.url, response.status_code)
    response.raise_for_status()
    text = response.text.strip()
    if text.startswith("<"):
        rows = parse_inventory_html(text)
        for row in rows:
            row["url"] = urljoin(response.url, row["url"])
        if not rows:
            raise ValueError(f"No file links found in Eurostat listing: {response.url}")
        logger.info("Read %d files from HTML listing directory=%s", len(rows), directory)
        return rows

    reader = csv.DictReader(io.StringIO(response.text))
    if not {"NAME", "name"}.intersection(reader.fieldnames or []):
        raise ValueError(f"Unrecognized Eurostat listing: {response.url}")
    rows: list[dict] = []
    for row in reader:
        if not row:
            continue
        name = (row.get("NAME") or row.get("name") or "").strip()
        if not name:
            continue
        file_type = (row.get("TYPE") or row.get("type") or "").strip().lower()
        if file_type in {"directory", "dir", "folder", "d"}:
            continue
        if not name.startswith(directory.rstrip("/") + "/"):
            name = f"{directory.rstrip('/')}/{name}"
        rows.append(
            {
                "name": name,
                "size": row.get("SIZE") or row.get("size") or "",
                "type": file_type,
                "date": row.get("DATE") or row.get("date") or "",
                "url": build_file_url(listing_url, name),
            }
        )
    logger.info("Read %d files from CSV listing directory=%s", len(rows), directory)
    return rows


@timed
def discover_matching_files(settings: dict) -> list[dict]:
    results: list[dict] = []
    files_api = settings.get("eurostat", {}).get(
        "files_api", "https://ec.europa.eu/eurostat/api/dissemination/files"
    )
    for source in settings.get("downloads", {}).get("sources", []):
        if not source.get("enabled", False):
            logger.debug("Skipping disabled source=%s", source.get("name", "unknown"))
            continue
        directory = source.get("directory", "")
        inventory = fetch_inventory(files_api, directory)
        include = source.get("include", ["*"])
        exclude = source.get("exclude", [])
        recursive = bool(source.get("recursive", False))
        logger.debug(
            "Filtering source=%s include=%s exclude=%s recursive=%s",
            source.get("name", "unknown"),
            include,
            exclude,
            recursive,
        )
        for item in inventory:
            name = item["name"]
            if not name or str(name).endswith("/"):
                continue
            if recursive:
                path_match = name
            else:
                path_match = Path(name).name
            if not match_patterns(path_match, include=include, exclude=exclude):
                logger.debug("Excluded file=%s", name)
                continue
            period = None
            if "full_v2_" in name or name.endswith(".7z"):
                stem = Path(name).name
                file_part = stem.replace("full_v2_", "")
                if file_part.endswith(".7z"):
                    file_part = file_part[:-3]
                if len(file_part) >= 6 and file_part[:4].isdigit():
                    month = file_part[:6]
                    if len(month) == 6:
                        period = f"{month[:4]}-{month[4:6]}"
            results.append(
                {
                    "source_name": source.get("name", "unknown"),
                    "directory": directory,
                    "name": name,
                    "period": period,
                    "size": item.get("size", ""),
                    "date": item.get("date", ""),
                    "url": item["url"],
                }
            )
    logger.info("Discovery complete: %d matching files", len(results))
    return sorted(results, key=lambda row: (row["source_name"], row["name"]))


def get_period_from_filename(name: str) -> str | None:
    stem = Path(name).name
    if stem.startswith("full_v2_"):
        marker = stem[len("full_v2_") :]
        if marker.endswith(".7z"):
            marker = marker[:-3]
        if len(marker) == 6 and marker.isdigit():
            return f"{marker[:4]}-{marker[4:6]}"
    return None


def valid_month(value: str | None) -> bool:
    if not value:
        return False
    if len(value) != 7 or value[4] != "-":
        return False
    yy, mm = value.split("-", 1)
    if not yy.isdigit() or not mm.isdigit():
        return False
    if not (1 <= int(mm) <= 12):
        return False
    return True
