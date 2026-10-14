from __future__ import annotations

import fnmatch
from pathlib import Path

import yaml

ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = ROOT_DIR / "config" / "settings.yaml"


def load_settings(config_path: str | Path | None = None) -> dict:
    path = Path(config_path) if config_path is not None else DEFAULT_CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"Missing settings file: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return data


def match_patterns(
    name: str, include: list[str] | None = None, exclude: list[str] | None = None
) -> bool:
    if include is None:
        include = ["*"]
    target = name.replace("\\", "/")
    basename = Path(target).name
    included = any(
        fnmatch.fnmatchcase(target, pattern) or fnmatch.fnmatchcase(basename, pattern)
        for pattern in include
    )
    if not included:
        return False
    if exclude:
        for pattern in exclude:
            if fnmatch.fnmatchcase(target, pattern) or fnmatch.fnmatchcase(basename, pattern):
                return False
    return True


def project_root() -> Path:
    return ROOT_DIR


def resolve_storage_dirs(settings: dict) -> tuple[Path, Path, Path, Path, Path]:
    root = project_root()
    storage = settings.get("storage", {})
    raw_dir = root / storage.get("raw_dir", "data/raw")
    extracted_dir = root / storage.get("extracted_dir", "data/extracted")
    processed_dir = root / storage.get("processed_dir", "data/processed")
    metadata_dir = root / storage.get("metadata_dir", "data/metadata")
    database_path = root / storage.get("database_path", "data/database/comext.duckdb")
    return raw_dir, extracted_dir, processed_dir, metadata_dir, database_path


def month_to_int(value: str) -> int | None:
    try:
        cleaned = value.strip()
        return int(cleaned.replace("-", "")) if cleaned else None
    except ValueError:
        return None
