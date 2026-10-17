"""JSON-lines log for the stream processor, in the working directory it serves.

The diagnosis agent tails <workdir>/logs/pipeline.jsonl, so the streaming pipeline
writes there in the same shape as the batch pipeline. Repeated events are
throttled per key, so a broken stream doesn't write thousands of identical lines.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


class JsonLog:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._last: dict[str, float] = {}

    def write(self, level: str, event: str, throttle_key: str | None = None,
              every_s: float = 5.0, **fields) -> bool:
        now = time.time()
        if throttle_key:
            if now - self._last.get(throttle_key, 0) < every_s:
                return False
            self._last[throttle_key] = now
        line = {"event": event, "level": level,
                "timestamp": datetime.now(timezone.utc).isoformat(), **fields}
        with self._lock, open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(line, default=str) + "\n")
        return True

    def info(self, event: str, **kw) -> bool:
        return self.write("info", event, **kw)

    def warning(self, event: str, **kw) -> bool:
        return self.write("warning", event, **kw)

    def error(self, event: str, **kw) -> bool:
        return self.write("error", event, **kw)
