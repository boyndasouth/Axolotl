"""A scripted stand-in for the LLM, for smoke-testing the harness and dashboard
without an API key. Results labelled "oracle" are NOT a measurement of anything.

It deliberately submits an overfitted fix for currency_string first (strips the
symbols but not thousands separators). That patch passes the sandbox, because the
held-out batch has no amounts over 1,000, and fails the hidden set, which shows
the false-fix metric working.
"""

from __future__ import annotations

import json

ROBUST_TRANSFORM = '''"""Raw order records -> canonical orders table."""

from __future__ import annotations

import math
import re
from datetime import datetime

import pandas as pd

OUTPUT_COLUMNS = ["order_id", "customer_id", "amount", "currency", "order_date", "status"]


def _amount(v):
    if isinstance(v, dict):
        v = v.get("value")
    if v is None:
        return math.nan
    if isinstance(v, (int, float)):
        return float(v)
    cleaned = re.sub(r"[^0-9.\\-]", "", str(v))
    return float(cleaned) if cleaned not in ("", "-", ".") else math.nan


def _date(v, formats):
    for fmt in formats:
        try:
            return datetime.strptime(str(v), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"unparseable order_date {v!r}; tried {formats}")


def transform_orders(records: list[dict], mappings: dict) -> pd.DataFrame:
    cfg = mappings["orders"]
    rename = cfg.get("rename", {})
    rows = []
    for r in records:              # record by record: a stream mixes old and new shapes
        r = dict(r)
        if isinstance(r.get("amount"), dict) and "currency" not in r:
            r["currency"] = r["amount"].get("currency")
        for src, dst in rename.items():
            if src in r and dst not in r:
                r[dst] = r.pop(src)
        rows.append(r)
    df = pd.DataFrame.from_records(rows)

    missing = [c for c in OUTPUT_COLUMNS if c not in df.columns]
    if missing:
        raise KeyError(f"missing required columns after rename: {missing}")

    df = df[OUTPUT_COLUMNS].copy()
    formats = cfg.get("date_formats") or [cfg["date_format"]]
    df["order_id"] = df["order_id"].astype(str)
    df["customer_id"] = df["customer_id"].astype(str)
    df["amount"] = df["amount"].map(_amount).astype(float)
    df["currency"] = df["currency"].astype(str)
    df["order_date"] = df["order_date"].map(lambda v: _date(v, formats))
    df["status"] = df["status"].astype(str).str.lower()

    # rows that can never meet the contract, and exact resends
    df = df[df["amount"].notna() & (df["amount"] >= 0)]
    df = df.drop_duplicates()
    return df.reset_index(drop=True)
'''

ROBUST_MAPPING = '''# Source-to-canonical mapping for the orders feed.
orders:
  rename:
    cust_id: customer_id
    client_id: customer_id
  date_format: "%Y-%m-%d"
  date_formats: ["%Y-%m-%d", "%m/%d/%Y"]
'''

OVERFIT_CURRENCY = ROBUST_TRANSFORM.replace(
    'cleaned = re.sub(r"[^0-9.\\-]", "", str(v))',
    'cleaned = str(v).lstrip("$€£")')


def _reply(transform: str) -> str:
    return (f"RATIONALE: oracle reference fix\n\n=== FILE: pipeline/transforms/orders.py ===\n{transform}"
            f"=== END FILE ===\n\n=== FILE: pipeline/mappings.yaml ===\n{ROBUST_MAPPING}=== END FILE ===")


class OracleLLM:
    def __init__(self):
        self.repairs = 0

    def __call__(self, role: str, system: str, user: str) -> str:
        outage = '"rows_in": 0' in user or "only 0 rows" in user
        if role == "diagnosis":
            return json.dumps({
                "root_cause": "oracle: upstream returned no rows" if outage else "oracle: upstream format changed",
                "category": "upstream_outage" if outage else "upstream_schema_change",
                "repairable": not outage, "confidence": 0.95, "evidence": ["oracle"],
                "target_files": ["pipeline/transforms/orders.py", "pipeline/mappings.yaml"],
                "fix_plan": "apply the reference fix"})
        self.repairs += 1
        currency = "€" in user or "£" in user
        return _reply(OVERFIT_CURRENCY if currency and self.repairs == 1 else ROBUST_TRANSFORM)
