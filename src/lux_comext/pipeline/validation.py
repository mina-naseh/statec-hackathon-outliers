from __future__ import annotations

from pathlib import Path

import duckdb


def validate_database(database_path: str | Path) -> dict:
    target = Path(database_path)
    if not target.exists():
        raise FileNotFoundError(f"Database not found: {target}")
    conn = duckdb.connect(str(target))
    counts = conn.execute("SELECT COUNT(*) FROM comext_monthly").fetchone()[0]
    conn.close()
    return {"rows": int(counts)}
