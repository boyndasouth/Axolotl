"""Drift functions: each takes canonical records and returns what a changed upstream sends.

Every drift is applied to a deep copy, so callers can keep the canonical records
to compute the expected (golden) output.
"""

from __future__ import annotations

import copy
import random
from datetime import date
from typing import Callable

CURRENCY_SYMBOLS = {"USD": "$", "EUR": "€", "GBP": "£"}


def rename_field(records, rng, src: str, dst: str):
    for r in records:
        r[dst] = r.pop(src)
    return records


def currency_string(records, rng):
    """amount: 12.5 -> "$12.50"; large values get thousands separators: "€1,234.50"."""
    for r in records:
        r["amount"] = f"{CURRENCY_SYMBOLS[r['currency']]}{r['amount']:,.2f}"
    return records


def date_format(records, rng, fmt: str):
    for r in records:
        r["order_date"] = date.fromisoformat(r["order_date"]).strftime(fmt)
    return records


def nest_amount(records, rng):
    """amount + currency collapse into one nested object; top-level currency disappears."""
    for r in records:
        r["amount"] = {"value": r["amount"], "currency": r.pop("currency")}
    return records


def status_case(records, rng):
    for r in records:
        r["status"] = r["status"].upper()
    return records


def bad_rows(records, rng, rate: float = 0.05):
    """Append invalid rows (null or negative amounts) that should be kept out of the table."""
    originals = list(records)
    n_bad = max(2, int(len(records) * rate))
    for i in range(n_bad):
        row = copy.deepcopy(rng.choice(originals))
        row["order_id"] = f"{row['order_id']}-BAD{i:03d}"
        row["amount"] = None if i % 2 == 0 else -abs(row["amount"])
        records.append(row)
    return records


def duplicate_rows(records, rng, rate: float = 0.1):
    """Upstream retries resend some rows verbatim."""
    dupes = [copy.deepcopy(r) for r in rng.sample(records, max(2, int(len(records) * rate)))]
    records.extend(dupes)
    rng.shuffle(records)
    return records


def empty_response(records, rng):
    return []


def add_field(records, rng, name: str = "channel"):
    for r in records:
        r[name] = rng.choice(["web", "app", "store"])
    return records


DRIFTS: dict[str, Callable] = {
    "rename_field": rename_field,
    "currency_string": currency_string,
    "date_format": date_format,
    "nest_amount": nest_amount,
    "status_case": status_case,
    "bad_rows": bad_rows,
    "duplicate_rows": duplicate_rows,
    "empty_response": empty_response,
    "add_field": add_field,
}


def apply_drift(records: list[dict], drift: str, params: dict | None = None,
                seed: int = 0) -> list[dict]:
    rng = random.Random(seed + 7919)
    return DRIFTS[drift](copy.deepcopy(records), rng, **(params or {}))
