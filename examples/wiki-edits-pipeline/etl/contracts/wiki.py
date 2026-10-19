"""The data contract for clean Wikipedia changes.

Protected: the repair agent can read it but never change it (healer.yaml lists it
as the contract), so a fix has to make the data meet these rules, not loosen them.
"""

from __future__ import annotations

import pandera.pandas as pa

CHANGE_TYPES = ["edit", "new", "log", "categorize", "external"]

WikiChangesSchema = pa.DataFrameSchema(
    {
        "event_id": pa.Column(str, unique=True, nullable=False),
        "wiki": pa.Column(str, pa.Check.str_matches(r"^[a-z0-9_]+$"), nullable=False),
        "change_type": pa.Column(str, pa.Check.isin(CHANGE_TYPES), nullable=False),
        "namespace": pa.Column(int, pa.Check.ge(-2), nullable=False),
        "title": pa.Column(str, pa.Check.str_length(min_value=1), nullable=False),
        "user": pa.Column(str, nullable=True),
        "is_bot": pa.Column(bool, nullable=False),
        "size_delta": pa.Column(int, nullable=False),
        # a plausible UTC timestamp: catches seconds-vs-milliseconds and format changes
        "edited_at": pa.Column(str, [pa.Check.str_matches(r"^20\d{2}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")],
                               nullable=False),
    },
    strict=True,
    ordered=True,
    name="wiki_changes",
)
