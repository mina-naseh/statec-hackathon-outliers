from __future__ import annotations

import gzip
from pathlib import Path

import requests

from ..config import project_root


def fetch_codelist(url: str, target_path: Path) -> Path:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    with target_path.open("wb") as handle:
        handle.write(response.content)
    return target_path


def import_metadata(settings: dict) -> list[str]:
    root = project_root()
    metadata_dir = root / settings.get("storage", {}).get("metadata_dir", "data/metadata")
    metadata_dir.mkdir(parents=True, exist_ok=True)
    imported: list[str] = []
    partner_url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/codelist/ESTAT/PARTNER/?compress=true&format=TSV&formatVersion=2.0"
    product_url = "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/3.0/structure/codelist/ESTAT/CXT_NC/?compress=true&format=TSV&formatVersion=2.0"
    for label, url in {"partner.tsv.gz": partner_url, "product_cn.tsv.gz": product_url}.items():
        target = metadata_dir / label
        try:
            fetch_codelist(url, target)
        except requests.RequestException:
            continue
        imported.append(label)
    return imported
