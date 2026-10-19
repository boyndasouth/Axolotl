"""Monitor: deterministic. Runs the pipeline and turns its run report into an Anomaly.

No LLM here on purpose: detection has to be cheap, fast and predictable, and a
false alarm wastes everything downstream.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

from agent.state import Anomaly, PipelineState


def run_pipeline(workdir: Path, source: str, timeout: int = 120) -> dict:
    """Run pipeline.run inside the working directory, as its own process."""
    p = subprocess.run([sys.executable, "-m", "pipeline.run", "--source", source, "--json"],
                       cwd=workdir, capture_output=True, text=True, timeout=timeout)
    for line in reversed(p.stdout.strip().splitlines()):
        if line.startswith("{"):
            return json.loads(line)
    return {"status": "failed", "failed_check": "ingest", "run_id": "unknown",
            "error": {"type": "PipelineCrashed", "message": (p.stderr or p.stdout)[-800:]},
            "rows_in": 0, "failures": [], "schema_diff": None, "raw_snapshot": None}


def classify(report: dict) -> Anomaly | None:
    if report["status"] == "ok":
        return None
    check = report.get("failed_check")
    diff = report.get("schema_diff") or {}
    err = report.get("error") or {}

    if check == "ingest":
        kind, summary = "ingest_error", f"could not read upstream: {err.get('message', '')[:120]}"
    elif check == "min_rows":
        kind, summary = "row_count", f"only {report['rows_in']} rows arrived"
    elif check == "schema_validation":
        checks = {f["check"] for f in report.get("failures", [])}
        if any("uniqueness" in c for c in checks):
            kind = "duplicates"
        elif any("nullable" in c for c in checks):
            kind = "null_spike"
        else:
            kind = "validation_error"
        parts = [f"{f['column']}: {f['check']} x{f['count']}" for f in report["failures"][:4]]
        summary = "contract violated: " + "; ".join(parts)
    else:
        kind = "schema_drift" if diff.get("added") or diff.get("removed") else "type_change"
        first_line = (err.get("message") or "").strip().splitlines()[0] if err.get("message") else ""
        summary = f"transform failed: {err.get('type')}: {first_line[:150]}"

    if report.get("mode") == "stream":      # rolling-window detection, see stream/detector.py
        if check == "min_rows":
            summary = f"upstream went silent: no records for {report.get('silence_s', '?')}s"
        else:
            summary += (f" ({report.get('quarantined', 0)} of {report.get('rows_in', 0)} records "
                        f"quarantined in the last {report.get('window_s', 0):g}s)")
    return {"kind": kind, "failed_check": check, "summary": summary,
            "run_id": report.get("run_id", "unknown"), "report": report}


def monitor(state: PipelineState) -> dict:
    if state.get("run_report"):
        # streaming: the detector already measured the stream and built the report
        report = state["run_report"]
        anomaly = classify(report)
        update = {"anomaly": anomaly, "status": "diagnosing" if anomaly else "healthy"}
        if anomaly and "detected" not in state.get("timestamps", {}):
            update["timestamps"] = {"detected": time.time()}
        return update

    workdir = Path(state["workdir"])
    report = run_pipeline(workdir, state["source"])
    anomaly = classify(report)
    update = {"run_report": report, "anomaly": anomaly,
              "incident_batch": report.get("raw_snapshot")}
    if anomaly is None:
        update["status"] = "healthy"
    else:
        update["status"] = "diagnosing"
        update["timestamps"] = {"detected": time.time()}
    return update
