"""Pipeline unit tests. These run inside the sandbox against every candidate patch,
so they must stay self-contained: only pipeline/ code, no chaos/ or agent/ imports.

They pin the ORIGINAL upstream format. A repair has to keep this working,
because during a migration upstream often sends both shapes for a while.
"""

from __future__ import annotations

import pandera.pandas as pa
import pytest

from pipeline.config import load_mappings
from pipeline.schemas import OrdersSchema
from pipeline.transforms.orders import OUTPUT_COLUMNS, transform_orders

RAW = [
    {"order_id": "ORD-1", "cust_id": "C0001", "amount": 12.5, "currency": "USD",
     "order_date": "2026-10-01", "status": "shipped"},
    {"order_id": "ORD-2", "cust_id": "C0420", "amount": 0.99, "currency": "EUR",
     "order_date": "2026-10-02", "status": "pending"},
    {"order_id": "ORD-3", "cust_id": "C9999", "amount": 300, "currency": "GBP",
     "order_date": "2026-10-03", "status": "delivered"},
]


@pytest.fixture
def clean():
    return transform_orders(RAW, load_mappings())


def test_output_columns(clean):
    assert list(clean.columns) == OUTPUT_COLUMNS


def test_passes_contract(clean):
    OrdersSchema.validate(clean, lazy=True)


def test_values_preserved(clean):
    row = clean.set_index("order_id").loc["ORD-1"]
    assert row["customer_id"] == "C0001"
    assert row["amount"] == pytest.approx(12.5)
    assert row["currency"] == "USD"
    assert row["order_date"] == "2026-10-01"
    assert row["status"] == "shipped"


def test_integer_amount_becomes_float(clean):
    assert clean.set_index("order_id").loc["ORD-3", "amount"] == pytest.approx(300.0)


def test_row_count_preserved(clean):
    assert len(clean) == len(RAW)


def test_contract_rejects_unknown_status():
    bad = [dict(RAW[0], status="lost")]
    with pytest.raises(pa.errors.SchemaErrors):
        OrdersSchema.validate(transform_orders(bad, load_mappings()), lazy=True)
