"""Transformations for the orders feed."""

from __future__ import annotations

import pandas as pd

# The order of columns expected in the final clean table.
OUTPUT_COLUMNS = [
    "order_id",
    "customer_id",
    "amount",
    "currency",
    "order_date",
    "status",
]

def _apply_customer_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Populate the canonical ``customer_id`` column from whichever source column
    is present (``cust_id`` – original upstream, or ``client_id`` – new upstream).

    The function is careful to avoid duplicate ``customer_id`` columns when both
    source columns appear in the same batch: it prefers ``cust_id`` (the original
    name) and drops the other.
    """
    # Determine which source column(s) exist.
    has_cust = "cust_id" in df.columns
    has_client = "client_id" in df.columns

    if has_cust:
        # Use the original column.
        df["customer_id"] = df["cust_id"]
    elif has_client:
        # Fallback to the new column name.
        df["customer_id"] = df["client_id"]

    # Drop any source identifier columns that may still be present.
    cols_to_drop = [c for c in ("cust_id", "client_id") if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    return df


def transform_orders(raw_records: list[dict], mappings: dict) -> pd.DataFrame:
    """
    Convert a list of raw order dictionaries into a clean DataFrame that
    conforms to ``pipeline.schemas.OrdersSchema``.

    The function:
    1. Builds a DataFrame from the raw list.
    2. Renames columns according to the static mapping (e.g. any future
       additional renames that are safe for all rows).
    3. Applies the flexible ``customer_id`` handling to support both old and new
       upstream field names.
    4. Deduplicates rows by ``order_id`` (keeping the first occurrence) to satisfy
       the uniqueness contract.
    5. Normalises the ``order_date`` format.
    6. Re‑orders columns to match ``OUTPUT_COLUMNS``.
    """
    # 1. Load raw data into a DataFrame.
    df = pd.DataFrame(raw_records)

    # 2. Apply any static rename mappings that are safe (they won't clash with
    #    the flexible handling below). We keep the original mapping for
    #    backward compatibility.
    static_rename = mappings.get("orders", {}).get("rename", {})
    # Filter out mappings that would rename to a column we are handling manually.
    safe_rename = {
        src: tgt
        for src, tgt in static_rename.items()
        if src not in ("cust_id", "client_id") and tgt != "customer_id"
    }
    if safe_rename:
        df = df.rename(columns=safe_rename)

    # 3. Populate the canonical ``customer_id`` column.
    df = _apply_customer_id(df)

    # 4. Deduplicate by order_id, keeping the first occurrence.
    if "order_id" in df.columns:
        df = df.drop_duplicates(subset=["order_id"], keep="first")

    # 5. Normalise the order_date format using the configured date format.
    date_fmt = mappings.get("orders", {}).get("date_format", "%Y-%m-%d")
    # Ensure the column exists before attempting conversion.
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.strftime(date_fmt)

    # 6. Re‑order columns to the expected output order.
    # Any missing columns will raise a KeyError, which is appropriate because
    # they represent a contract violation.
    df = df[OUTPUT_COLUMNS]

    return df
