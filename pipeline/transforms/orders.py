"""Transform the raw orders feed into the canonical clean format.

The original implementation only renamed `cust_id` → `customer_id` and
assumed numeric `amount`.  Upstream now sometimes sends `client_id` instead
of `cust_id` and may embed currency symbols in the `amount` field.  This
module now:
* extracts the customer identifier from either column,
* normalises `amount` strings to floats,
* respects the configured date format,
* preserves the original column order and types required by the contract.
"""

from __future__ import annotations

import re
from typing import Any, List, Mapping

import pandas as pd

# The order of columns expected by downstream consumers and the contract.
OUTPUT_COLUMNS = [
    "order_id",
    "customer_id",
    "amount",
    "currency",
    "order_date",
    "status",
]

_currency_symbol_re = re.compile(r"[^\d\.\-]+")


def _parse_amount(value: Any) -> float | None:
    """Coerce raw ``amount`` values to a non‑negative float.

    Handles:
    * numeric types (int, float) – cast to float,
    * strings containing currency symbols (e.g. ``"$12.34"`` or ``"£1,795.12"``) –
      strip non‑numeric characters before conversion.
    Returns ``None`` for values that cannot be parsed.
    """
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = _currency_symbol_re.sub("", value)
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def transform_orders(raw_records: List[Mapping[str, Any]], mappings: Mapping[str, Any]) -> pd.DataFrame:
    """Transform a list of raw order dictionaries into a clean DataFrame.

    Parameters
    ----------
    raw_records:
        List of dictionaries as received from the upstream feed.
    mappings:
        Mapping configuration loaded via ``pipeline.config.load_mappings``.
        Expected keys:
        * ``rename`` – column rename mapping (e.g. ``cust_id`` → ``customer_id``)
        * ``date_format`` – strftime pattern for ``order_date``.
    """
    # Load into a DataFrame; keep original columns for custom handling.
    df = pd.DataFrame(raw_records)

    # ------------------------------------------------------------------
    # 1. Customer identifier handling (cust_id / client_id → customer_id)
    # ------------------------------------------------------------------
    rename_map = mappings.get("orders", {}).get("rename", {})
    df = df.rename(columns=rename_map)

    # Ensure a canonical ``customer_id`` column exists and fill missing values
    # from ``client_id`` when present.
    if "customer_id" not in df.columns:
        df["customer_id"] = df.get("client_id")
    else:
        df["customer_id"] = df["customer_id"].fillna(df.get("client_id"))

    # Drop source identifier columns to avoid duplicates.
    for col in ("cust_id", "client_id"):
        if col in df.columns:
            df = df.drop(columns=col)

    # ------------------------------------------------------------------
    # 2. Amount normalisation
    # ------------------------------------------------------------------
    df["amount"] = df["amount"].apply(_parse_amount)

    # ------------------------------------------------------------------
    # 3. Date formatting
    # ------------------------------------------------------------------
    date_fmt = mappings.get("orders", {}).get("date_format", "%Y-%m-%d")
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.strftime(date_fmt)

    # ------------------------------------------------------------------
    # 4. Status normalisation (handle upstream uppercase values)
    # ------------------------------------------------------------------
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.lower()

    # ------------------------------------------------------------------
    # 5. Re‑order columns to match the contract exactly.
    # ------------------------------------------------------------------
    df = df[OUTPUT_COLUMNS]

    return df
