"""The data contract for the clean orders table.

The repair agent cannot edit this file: a fix has to make the data meet the
contract, not loosen the contract until the data passes.
"""

from __future__ import annotations

import pandera.pandas as pa

STATUSES = ["pending", "shipped", "delivered", "cancelled"]
CURRENCIES = ["USD", "EUR", "GBP"]

OrdersSchema = pa.DataFrameSchema(
    {
        "order_id": pa.Column(str, unique=True, nullable=False),
        "customer_id": pa.Column(str, pa.Check.str_matches(r"^C\d{4}$"), nullable=False),
        "amount": pa.Column(float, pa.Check.ge(0), nullable=False),
        "currency": pa.Column(str, pa.Check.isin(CURRENCIES), nullable=False),
        "order_date": pa.Column(str, pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}$"), nullable=False),
        "status": pa.Column(str, pa.Check.isin(STATUSES), nullable=False),
    },
    strict=True,
    ordered=True,
    name="orders_clean",
)
