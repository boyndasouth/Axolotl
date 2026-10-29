"""Load the validated table into DuckDB."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from pipeline.config import DATA_DIR

WAREHOUSE = DATA_DIR / "warehouse.duckdb"


def load_orders(df: pd.DataFrame, path: Path = WAREHOUSE) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    try:
        con.register("incoming", df)
        con.execute("CREATE OR REPLACE TABLE orders_clean AS SELECT * FROM incoming")
        return con.execute("SELECT count(*) FROM orders_clean").fetchall()[0][0]
    finally:
        con.close()
