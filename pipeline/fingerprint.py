"""Schema fingerprint of a raw batch: which fields appear and with which value types.

Compared against the last known-good fingerprint to produce a schema diff.
"""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.config import DATA_DIR

SNAPSHOT = DATA_DIR / "schema_snapshots" / "orders.json"


def _type_name(v) -> str:
    if v is None:
        return "null"
    return type(v).__name__


def fingerprint(records: list[dict]) -> dict[str, list[str]]:
    fields: dict[str, set[str]] = {}
    for r in records:
        for k, v in r.items():
            t = _type_name(v)
            if isinstance(v, dict):
                t = "dict{" + ",".join(sorted(v)) + "}"
            fields.setdefault(k, set()).add(t)
    return {k: sorted(v) for k, v in sorted(fields.items())}


def load_snapshot(path: Path = SNAPSHOT) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save_snapshot(fp: dict, path: Path = SNAPSHOT) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(fp, indent=2), encoding="utf-8")


def diff(before: dict | None, after: dict) -> dict:
    if before is None:
        return {"baseline": "none", "added": [], "removed": [], "type_changed": {}}
    added = sorted(set(after) - set(before))
    removed = sorted(set(before) - set(after))
    changed = {k: {"before": before[k], "after": after[k]}
               for k in sorted(set(before) & set(after))
               if set(after[k]) - set(before[k])}
    return {"added": added, "removed": removed, "type_changed": changed}


def is_empty(d: dict) -> bool:
    return not (d.get("added") or d.get("removed") or d.get("type_changed"))
