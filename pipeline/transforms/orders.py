"""Raw order records -> canonical orders table.

Output columns: order_id, customer_id, amount, currency, order_date (ISO date string), status.
The contract the output must satisfy lives in pipeline/schemas.py.
"""

from __future__ import annotations

import pandas as pd

OUTPUT_COLUMNS = ["order_id", "customer_id", "amount", "currency", "order_date", "status"]


def transform_orders(records: list[dict], mappings: dict) -> pd.DataFrame:
    cfg = mappings["orders"]
    df = pd.DataFrame.from_records(records)

    # Apply the configured renames (e.g., cust_id -> customer_id)
    df = df.rename(columns=cfg.get("rename", {}))

    # If the required column is still missing, try to map the new upstream field.
    if "customer_id" not in df.columns:
        if "client_id" in df.columns:
            df = df.rename(columns={"client_id": "customer_id"})
    else:
        # Both columns may exist (old and new); prefer cust_id values and drop the duplicate.
        if "client_id" in df.columns:
            df["customer_id"] = df["customer_id"].fillna(df["client_id"])
            df = df.drop(columns=["client_id"])

    missing = [c for c in OUTPUT_COLUMNS if c not in df.columns]
    if missing:
        raise KeyError(f"missing required columns after rename: {missing}")

    df = df[OUTPUT_COLUMNS].copy()
    df["order_id"] = df["order_id"].astype(str)
    df["customer_id"] = df["customer_id"].astype(str)
    df["amount"] = df["amount"].astype(float)
    df["currency"] = df["currency"].astype(str)
    df["order_date"] = pd.to_datetime(df["order_date"], format=cfg["date_format"]).dt.strftime("%Y-%m-%d")
    df["status"] = df["status"].astype(str)
    return df.reset_index(drop=True)
