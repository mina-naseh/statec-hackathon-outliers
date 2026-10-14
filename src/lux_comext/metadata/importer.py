from __future__ import annotations

from pathlib import Path

from ..parsing.reader import read_metadata_tsv
from ..storage.ingestion import connection


def import_codelists(database_path: str | Path, metadata_dir: str | Path) -> dict:
    """Replace available codelists transactionally so retries cannot duplicate them."""
    metadata_dir = Path(metadata_dir)
    loaded = {"partner_dim": 0, "product_dim": 0}
    resources = {}
    codelists = {"partner_dim": "partner.tsv.gz", "product_dim": "product_cn.tsv.gz"}
    for table, name in codelists.items():
        path = metadata_dir / name
        if path.exists():
            resources[table] = read_metadata_tsv(path)
    if not resources:
        return loaded
    with connection(Path(database_path)) as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            for table, rows in resources.items():
                conn.execute(f"DELETE FROM {table}")
                if table == "partner_dim":
                    values = [
                        (
                            row.get("Code") or row.get("code") or row.get("PARTNER") or "",
                            row.get("Label_EN") or row.get("label_en") or "",
                            row.get("Label_FR") or row.get("label_fr") or "",
                            row.get("Label_DE") or row.get("label_de") or "",
                            str(
                                row.get("is_standard_code") or row.get("IS_STANDARD_CODE") or ""
                            ).lower() in {"1", "true", "yes"},
                            row.get("source") or "ESTAT/PARTNER",
                            row.get("note") or "",
                            row.get("ec_corporate_code") or "",
                            row.get("ec_corporate_uri") or "",
                            row.get("version") or "",
                        )
                        for row in rows
                    ]
                else:
                    values = [
                        (
                            row.get("Code") or row.get("code") or row.get("PRODUCT") or "",
                            row.get("Label_EN") or row.get("label_en") or "",
                            row.get("Label_FR") or row.get("label_fr") or "",
                            row.get("Label_DE") or row.get("label_de") or "",
                            int(row.get("level") or 0),
                            row.get("parent_code") or "",
                            row.get("chapter") or "",
                            str(
                                row.get("is_not_elsewhere_specified")
                                or row.get("IS_NOT_ELSEWHERE_SPECIFIED") or ""
                            ).lower() in {"1", "true", "yes"},
                            row.get("classification_id") or "ESTAT/CXT_NC",
                            row.get("classification_version") or "",
                        )
                        for row in rows
                    ]
                if values:
                    placeholders = ", ".join(["?"] * len(values[0]))
                    conn.executemany(f"INSERT INTO {table} VALUES ({placeholders})", values)
                loaded[table] = len(rows)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    return loaded
