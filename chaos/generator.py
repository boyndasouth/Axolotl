"""Canonical order records: the format the pipeline was originally written for."""

from __future__ import annotations

import random
from datetime import date, timedelta

STATUSES = ["pending", "shipped", "delivered", "cancelled"]
CURRENCIES = ["USD", "EUR", "GBP"]


def generate_orders(n: int = 200, seed: int = 0, start: str = "2026-09-01",
                    max_amount: float = 500.0) -> list[dict]:
    rng = random.Random(seed)
    base = date.fromisoformat(start)
    records = []
    for i in range(n):
        records.append({
            "order_id": f"ORD-{seed:04d}-{i:05d}",
            "cust_id": f"C{rng.randint(1, 9999):04d}",
            "amount": round(rng.uniform(1.0, max_amount), 2),
            "currency": rng.choice(CURRENCIES),
            "order_date": (base + timedelta(days=rng.randint(0, 40))).isoformat(),
            "status": rng.choice(STATUSES),
        })
    return records
