"""Raw Wikimedia recentchange events -> one clean row per change.

Output columns: event_id, wiki, change_type, namespace, title, user, is_bot,
size_delta, edited_at (UTC, ISO 8601 with a trailing Z).
The contract the output must meet lives in etl/contracts/wiki.py.

Records are handled one at a time on purpose: in a stream, one micro-batch can mix
the old and the new upstream shape.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

COLUMNS = ["event_id", "wiki", "change_type", "namespace", "title", "user", "is_bot",
           "size_delta", "edited_at"]

# change types that carry page lengths; for the others (log, categorize) there are none
SIZED_TYPES = {"edit", "new"}


def _edited_at(value, unit: str) -> str:
    seconds = int(float(value))
    if seconds > 99_999_999_999 or unit == "ms":
        seconds /= 1000
    return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _size_delta(change_type: str, length) -> int:
    if change_type not in SIZED_TYPES:
        return 0
    if not isinstance(length, dict):
        raise KeyError(f"'{change_type}' event has no length object")
    return int(length.get("new") or 0) - int(length.get("old") or 0)


def transform(records: list[dict], mappings: dict) -> pd.DataFrame:
    rename = mappings.get("rename") or {}
    unit = mappings.get("timestamp_unit", "s")
    rows = []
    for raw in records:
        r = {rename.get(k, k): v for k, v in raw.items()}
        title = r.get("title")
        if title is None or pd.isna(title) or str(title).strip() == "":
            continue
        rows.append({
            "event_id": str(r["event_id"]),
            "wiki": r["wiki"],
            "change_type": r["type"],
            "namespace": int(r["namespace"]),
            "title": str(title),
            "user": r["user"],
            "is_bot": bool(r.get("bot", False)),
            "size_delta": _size_delta(r["type"], r.get("length")),
            "edited_at": _edited_at(r["timestamp"], unit),
        })
    df = pd.DataFrame(rows, columns=COLUMNS)
    return df.astype({"event_id": str, "namespace": "int64", "is_bot": bool, "size_delta": "int64"}) \
        .reset_index(drop=True)
