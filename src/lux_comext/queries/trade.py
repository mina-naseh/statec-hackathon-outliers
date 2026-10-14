from __future__ import annotations

import duckdb


def lux_imports_from_germany(database_path: str) -> list[tuple]:
    conn = duckdb.connect(database_path)
    result = conn.execute(
        """
        SELECT period, product_nc, value_in_euros
        FROM comext_monthly
        WHERE declarant = 'LU' AND partner = 'DE' AND flow = 1
        ORDER BY period DESC
        LIMIT 20
        """
    ).fetchall()
    conn.close()
    return result
