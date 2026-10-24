"""Raw order records -> canonical orders table.

Output columns: order_id, customer_id, amount, currency, order_date (ISO date string), status.
The contract the output must satisfy lives in pipeline/schemas.py.
"""

from __future__ import annotations

import re
import pandas as pd

OUTPUT_COLUMNS = ["order_id", "customer_id", "amount", "currency", "order_date", "status"]


def _clean_amount(val):
    """Convert amount to float, handling strings with currency symbols and commas."""
    if isinstance(val, str):
        # Remove any character that is not a digit, minus sign, or decimal point
        cleaned = re.sub(r"[^\d\.-]", "", val)
        return float(cleaned) if cleaned else float("nan")
    return float(val)


def transform_orders(records: list[dict], mappings: dict) -> pd.DataFrame:
    cfg = mappings["orders"]
    df = pd.DataFrame.from_records(records)
    df = df.rename(columns=cfg.get("rename", {}))

    missing = [c for c in OUTPUT_COLUMNS if c not in df.columns]
    if missing:
        raise KeyError(f"missing required columns after rename: {missing}")

    df = df[OUTPUT_COLUMNS].copy()
    df["order_id"] = df["order_id"].astype(str)
    df["customer_id"] = df["customer_id"].astype(str)

    # Clean amount values before casting to float
    df["amount"] = df["amount"].apply(_clean_amount)

    df["currency"] = df["currency"].astype(str)
    df["order_date"] = pd.to_datetime(df["order_date"], format=cfg["date_format"]).dt.strftime("%Y-%m-%d")
    df["status"] = df["status"].astype(str)
    return df.reset_index(drop=True)
