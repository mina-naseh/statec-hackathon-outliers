from __future__ import annotations

import csv
import gzip
from pathlib import Path

def normalize_name(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_record(row: dict) -> dict:
    normalized = {key.strip(): value for key, value in row.items()}
    standard = {
        "declarant": normalized.get("DECLARANT")
        or normalized.get("REPORTER")
        or normalized.get("reporter")
        or normalized.get("DECLARANT_ID")
        or "",
        "partner": normalized.get("PARTNER") or normalized.get("partner") or "",
        "product_nc": normalized.get("PRODUCT_NC")
        or normalized.get("PRODUCT")
        or normalized.get("product_nc")
        or normalized.get("PRODUCT_CODE")
        or "",
        "flow": normalized.get("FLOW") or normalized.get("flow") or "",
        "period": normalized.get("PERIOD")
        or normalized.get("period")
        or normalized.get("TIME_PERIOD")
        or "",
        "value_in_euros": normalized.get("VALUE_IN_EUROS")
        or normalized.get("VALUE_EUR")
        or normalized.get("value_in_euros")
        or normalized.get("VALUE_EUR")
        or "",
        "quantity_in_kg": normalized.get("QUANTITY_IN_KG")
        or normalized.get("QUANTITY_KG")
        or normalized.get("quantity_in_kg")
        or "",
        "sup_quantity": normalized.get("SUP_QUANTITY")
        or normalized.get("sup_quantity")
        or normalized.get("SUP_QUANTITY")
        or "",
        "source_archive": normalized.get("SOURCE_ARCHIVE")
        or normalized.get("source_archive")
        or "",
    }
    source_archive = normalize_name(standard.get("source_archive"))
    period_value = normalize_name(standard.get("period"))
    product_code = normalize_name(standard.get("product_nc"))
    flow_value = normalize_name(standard.get("flow"))
    value_eur = normalize_name(standard.get("value_in_euros"))
    quantity_kg = normalize_name(standard.get("quantity_in_kg"))
    if value_eur in {"", "NA", "N/A", "NULL", "null"}:
        value_eur = None
    if quantity_kg in {"", "NA", "N/A", "NULL", "null"}:
        quantity_kg = None
    try:
        flow_int = int(flow_value) if flow_value not in (None, "") else None
    except (TypeError, ValueError):
        flow_int = None
    try:
        period_int = (
            int(str(period_value).replace("-", "")) if period_value not in (None, "") else None
        )
    except (TypeError, ValueError):
        period_int = None
    try:
        value_int = int(value_eur) if value_eur not in (None, "") else None
    except (TypeError, ValueError):
        value_int = None
    try:
        quantity_int = int(quantity_kg) if quantity_kg not in (None, "") else None
    except (TypeError, ValueError):
        quantity_int = None
    standard["flow"] = flow_int
    standard["period"] = period_int
    standard["value_in_euros"] = value_int
    standard["quantity_in_kg"] = quantity_int
    standard["source_archive"] = source_archive
    standard["product_nc"] = product_code
    return {**normalized, **standard}


def read_source_rows(file_path: str | Path) -> list[dict]:
    target = Path(file_path)
    rows = []
    with target.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=",")
        for row in reader:
            rows.append(normalize_record(row))
    return rows


def read_metadata_tsv(path: Path) -> list[dict]:
    rows = []
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            rows.append({key: value for key, value in row.items()})
    return rows
