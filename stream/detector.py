"""Streaming monitor: deterministic checks over a rolling window, no LLM.

Fires when, over the last WINDOW_S seconds,
  - enough records were quarantined (at least MIN_BAD, and at least MIN_RATE of the traffic), or
  - nothing arrived for SILENCE_S seconds after the stream had been flowing.

One root cause should mean one incident, not thousands. Each failure signature
(say "transform:KeyError:missing required columns after rename") is disarmed when it
opens an incident and re-armed only after it has been absent for a full window. And
only one incident is worked at a time, so two repairs never edit the same files at once.

On firing it builds the same run report the batch pipeline produces, so the
diagnosis and repair agents work unchanged.
"""

from __future__ import annotations

import os
import time
from collections import Counter
from pathlib import Path

from pipeline import fingerprint
from stream.processor import StreamProcessor

WINDOW_S = float(os.environ.get("SHP_WINDOW_S", "10"))
MIN_BAD = int(os.environ.get("SHP_MIN_BAD", "5"))
MIN_RATE = float(os.environ.get("SHP_MIN_DLQ_RATE", "0.02"))
SILENCE_S = float(os.environ.get("SHP_SILENCE_S", "12"))
SILENCE = "silence:upstream"


class Detector:
    def __init__(self, processor: StreamProcessor, metrics, window_s: float = WINDOW_S,
                 min_bad: int = MIN_BAD, min_rate: float = MIN_RATE, silence_s: float = SILENCE_S):
        self.processor = processor
        self.metrics = metrics
        self.window_s = window_s
        self.min_bad = min_bad
        self.min_rate = min_rate
        self.silence_s = silence_s
        self.disarmed: set[str] = set()
        self._absent_since: dict[str, float] = {}
        self.last = {}

    def conditions(self, now: float) -> tuple[dict, dict]:
        """Which failure signatures are firing right now, and the window they were measured on."""
        w = self.metrics.window(self.window_s, now=now)
        firing: dict[str, str] = {}
        if w["dlq"] >= self.min_bad and w["dlq_rate"] >= self.min_rate:
            for sig, n in w["reasons"].items():
                if n >= max(2, self.min_bad // 2) or len(w["reasons"]) == 1:
                    firing[sig] = "dlq"
        last = self.metrics.last_record_ts
        if last is not None and now - last >= self.silence_s:
            firing[SILENCE] = "silence"
        self.last = {"window": w, "firing": sorted(firing), "checked_at": now}
        return firing, w

    def check(self, now: float | None = None, busy: bool = False) -> dict | None:
        """Returns a run report when a new incident should open, else None."""
        now = now or time.time()
        firing, w = self.conditions(now)

        # re-arm signatures that have been quiet for a whole window
        for sig in list(self.disarmed):
            if sig in firing:
                self._absent_since.pop(sig, None)
            elif now - self._absent_since.setdefault(sig, now) >= self.window_s:
                self.disarmed.discard(sig)
                self._absent_since.pop(sig, None)

        new = [s for s in firing if s not in self.disarmed]
        if not new or busy:
            return None
        sigs = sorted(firing)          # report everything that's wrong, not just the new part
        self.disarmed.update(sigs)
        return self.build_report(sigs, w, now)

    def build_report(self, sigs: list[str], w: dict, now: float) -> dict:
        run_id = time.strftime("stream-%Y%m%dT%H%M%S")
        report = {"run_id": run_id, "source": f"stream:{self.processor.spec.name}", "mode": "stream", "status": "failed",
                  "rows_in": w["in"], "rows_out": w["loaded"], "raw_snapshot": None,
                  "schema_diff": None, "error": None, "failures": [], "failed_check": None,
                  "window_s": self.window_s, "quarantined": w["dlq"],
                  "dlq_rate": round(w["dlq_rate"], 4), "signatures": sigs}

        if SILENCE in sigs and len(sigs) == 1:
            silent = now - (self.metrics.last_record_ts or now)
            report.update(failed_check="min_rows", rows_in=0, rows_out=0, silence_s=round(silent, 1),
                          first_failure=self.metrics.last_record_ts)
            return report

        failures = self.processor.recent_failures(since=now - self.window_s)
        first = min((ts for ts, _, r in failures if r["signature"] in sigs), default=now)
        report["first_failure"] = first
        bad_records = [rec for _, rec, _ in failures]
        if bad_records:
            report["schema_diff"] = fingerprint.diff(self.processor.known_fp,
                                                     fingerprint.fingerprint(bad_records))

        transform = [s for s in sigs if s.startswith("transform:")]
        if transform:
            counts = Counter(r["signature"] for _, _, r in failures)
            main = max(transform, key=lambda s: counts.get(s, 0))
            err = dict(self.processor.last_transform_error.get(main) or {})
            err.pop("signature", None)
            report.update(failed_check="transform", error=err or {"type": "unknown", "message": main})
        else:
            report.update(failed_check="schema_validation", failures=self._summarize(failures))
        return report

    @staticmethod
    def _summarize(failures) -> list[dict]:
        groups: dict[tuple, dict] = {}
        for _, _, r in failures:
            if r.get("stage") != "contract":
                continue
            key = (r.get("column"), r.get("check"))
            g = groups.setdefault(key, {"column": key[0], "check": key[1], "count": 0, "examples": []})
            g["count"] += 1
            if len(g["examples"]) < 5 and r.get("value") not in g["examples"]:
                g["examples"].append(r.get("value"))
        return sorted(groups.values(), key=lambda g: -g["count"])

    def snapshot_batch(self, report: dict, path: Path) -> str:
        """The incident batch the agents sample and the sandbox verifies against:
        the most recent raw records, as they arrived (old and new shapes mixed)."""
        n = 0 if report.get("failed_check") == "min_rows" else 200
        return str(self.processor.snapshot_incident_batch(path, n=n))
