"""Per-second counters for the live stream, kept in memory for the last few minutes.

The detector reads rolling windows from here, and the console charts the series.
"""

from __future__ import annotations

import threading
import time
from collections import Counter, deque

FIELDS = ("produced", "in", "loaded", "dlq", "filtered", "sink_dupes")


class Metrics:
    def __init__(self, history_s: int = 900):
        self.history_s = history_s
        self._lock = threading.Lock()
        self._buckets: dict[int, Counter] = {}
        self._order: deque[int] = deque()
        self._reasons: dict[int, Counter] = {}
        self.totals: Counter = Counter()
        self.last_record_ts: float | None = None
        self.first_record_ts: float | None = None

    def _bucket(self, sec: int) -> Counter:
        b = self._buckets.get(sec)
        if b is None:
            b = self._buckets[sec] = Counter()
            self._order.append(sec)
            while self._order and self._order[0] < sec - self.history_s:
                old = self._order.popleft()
                self._buckets.pop(old, None)
                self._reasons.pop(old, None)
        return b

    def add(self, ts: float | None = None, reasons: Counter | None = None, **counts: int) -> None:
        ts = ts or time.time()
        with self._lock:
            b = self._bucket(int(ts))
            for k, v in counts.items():
                if v:
                    b[k] += v
                    self.totals[k] += v
            if reasons:
                self._reasons.setdefault(int(ts), Counter()).update(reasons)
            if counts.get("in"):
                self.last_record_ts = ts
                self.first_record_ts = self.first_record_ts or ts

    def window(self, seconds: float, now: float | None = None, since: float | None = None) -> dict:
        """Sums over the last `seconds` (or since a timestamp), plus failure signatures seen."""
        now = now or time.time()
        start = since if since is not None else now - seconds
        out: Counter = Counter()
        reasons: Counter = Counter()
        with self._lock:
            for sec, b in self._buckets.items():
                if sec >= int(start) and sec <= now:
                    out.update(b)
                    reasons.update(self._reasons.get(sec, {}))
        res = {k: int(out.get(k, 0)) for k in FIELDS}
        res["reasons"] = dict(reasons)
        res["dlq_rate"] = res["dlq"] / res["in"] if res["in"] else 0.0
        return res

    def series(self, seconds: int = 300, now: float | None = None) -> list[dict]:
        now = int(now or time.time())
        with self._lock:
            return [{"t": s, **{k: int(self._buckets.get(s, {}).get(k, 0)) for k in FIELDS}}
                    for s in range(now - seconds + 1, now + 1)]
