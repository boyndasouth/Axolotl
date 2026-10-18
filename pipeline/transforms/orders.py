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

    # Apply the standard rename mapping (e.g., cust_id -> customer_id)
    df = df.rename(columns=cfg.get("rename", {}))

    # New upstream may use client_id instead of cust_id.
    # Populate/replace missing customer_id values from client_id on a row‑wise basis.
    if "client_id" in df.columns:
        if "customer_id" not in df.columns:
            # No customer_id column yet – create it from client_id.
            df["customer_id"] = df["client_id"]
        else:
            # Fill any gaps where cust_id was missing.
            df["customer_id"] = df["customer_id"].fillna(df["client_id"])
        # Drop client_id if it still exists after the above handling.
        if "client_id" in df.columns:
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
