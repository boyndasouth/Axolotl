"""Upstream changes to apply to REAL events before they're published, so the healer
faces real-world data with a realistic break in it.

    python producer/drift.py list | off | <scenario>    (switch while the producer runs)
"""

from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

DRIFT_FILE = Path(__file__).resolve().parent / ".drift.json"

# name -> (what upstream does, what the healer should do)
SCENARIOS: dict[str, tuple[str, str]] = {
    "rename_user": ("the `user` field is renamed to `performer`", "repair"),
    "timestamp_ms": ("`timestamp` switches from Unix seconds to milliseconds", "repair"),
    "timestamp_iso": ("`timestamp` becomes an ISO 8601 string", "repair"),
    "length_flattened": ("`length: {old, new}` becomes `length_old` and `length_new`", "repair"),
    "missing_titles": ("about 5% of events arrive with no title", "repair (drop the rows that can't be fixed)"),
    "bot_as_string": ("`bot` true/false becomes the strings \"true\"/\"false\"",
                      "nothing: every value still passes the contract, so this corrupts data silently"),
}


def apply(name: str | None, event: dict, rng: random.Random) -> dict:
    if not name:
        return event
    e = dict(event)
    if name == "rename_user":
        if "user" in e:
            e["performer"] = e.pop("user")
    elif name == "timestamp_ms":
        if isinstance(e.get("timestamp"), int):
            e["timestamp"] = e["timestamp"] * 1000
    elif name == "timestamp_iso":
        if isinstance(e.get("timestamp"), int):
            e["timestamp"] = datetime.fromtimestamp(e["timestamp"], tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif name == "length_flattened":
        length = e.pop("length", None)
        if isinstance(length, dict):
            if "old" in length:
                e["length_old"] = length["old"]
            if "new" in length:
                e["length_new"] = length["new"]
    elif name == "missing_titles":
        if rng.random() < 0.05:
            e["title"] = None
    elif name == "bot_as_string":
        if "bot" in e:
            e["bot"] = "true" if e["bot"] else "false"
    else:
        raise ValueError(f"unknown drift {name!r}; have {sorted(SCENARIOS)}")
    return e


def current() -> str | None:
    try:
        return json.loads(DRIFT_FILE.read_text(encoding="utf-8")).get("scenario")
    except (OSError, ValueError):
        return None


def set_drift(name: str | None) -> None:
    if name and name not in SCENARIOS:
        raise ValueError(f"unknown drift {name!r}; have {sorted(SCENARIOS)}")
    DRIFT_FILE.write_text(json.dumps({"scenario": name, "since": time.time()}), encoding="utf-8")
