"""The drift switch: which chaos scenario upstream is currently applying.

One small JSON file, shared by everything that plays "upstream": the fake HTTP API
(batch mode), the stream producer, the CLI injector and the live console. The
benchmark uses MemoryDrift instead, so parallel runs never touch the real switch.
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

from chaos.catalog import list_scenarios

DRIFT_FILE = Path(os.environ.get("SHP_DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "upstream_drift.json"


class FileDrift:
    def __init__(self, path: Path = DRIFT_FILE):
        self.path = path
        self._cache: tuple[int, dict] | None = None

    def get(self) -> dict:
        """{"scenario": str | None, "since": float | None}. Cheap: re-reads only when the file changes."""
        try:
            mtime = self.path.stat().st_mtime_ns
        except FileNotFoundError:
            self._cache = None
            return {"scenario": None, "since": None}
        if self._cache and self._cache[0] == mtime:
            return self._cache[1]
        try:
            state = json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {"scenario": None, "since": None}
        state = {"scenario": state.get("scenario"), "since": state.get("since")}
        self._cache = (mtime, state)
        return state

    def set(self, scenario: str) -> dict:
        if scenario not in list_scenarios():
            raise KeyError(f"unknown scenario {scenario!r}")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        state = {"scenario": scenario, "since": time.time()}
        self.path.write_text(json.dumps(state), encoding="utf-8")
        return state

    def clear(self) -> dict:
        self.path.unlink(missing_ok=True)
        return {"scenario": None, "since": None}


class MemoryDrift:
    def __init__(self):
        self._state = {"scenario": None, "since": None}
        self._lock = threading.Lock()

    def get(self) -> dict:
        with self._lock:
            return dict(self._state)

    def set(self, scenario: str) -> dict:
        if scenario not in list_scenarios():
            raise KeyError(f"unknown scenario {scenario!r}")
        with self._lock:
            self._state = {"scenario": scenario, "since": time.time()}
            return dict(self._state)

    def clear(self) -> dict:
        with self._lock:
            self._state = {"scenario": None, "since": None}
            return dict(self._state)
