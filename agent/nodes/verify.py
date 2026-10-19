"""Verify: deterministic sandbox run. Never an LLM judgment."""

from __future__ import annotations

import time
from pathlib import Path

from agent.nodes.rollout import emit
from agent.state import PipelineState
from sandbox.runner import PatchRejected, verify_patch


def _failed(summary: str) -> dict:
    return {"passed": False, "unit_tests": {}, "schema_checks": {}, "golden_diff_rows": -1,
            "regressions": [], "sandbox_logs": "", "duration_s": 0.0, "backend": "none",
            "summary": summary}


def verify(state: PipelineState) -> dict:
    patch = state["patch"]
    if patch.get("error"):
        result = _failed(patch["error"])
    else:
        batch = state.get("incident_batch")
        emit({"stage": "sandbox_started", "attempt": state.get("attempts", 0),
              "backend": state.get("sandbox_backend") or "auto"})
        try:
            result = verify_patch(Path(state["workdir"]), patch["files"],
                                  scenario_id=state.get("scenario_id"),
                                  incident_batch=Path(batch) if batch else None,
                                  live_sample=Path(state["live_sample"]) if state.get("live_sample") else None,
                                  backend=state.get("sandbox_backend"))
        except PatchRejected as e:
            result = _failed(str(e))
    update = {"verification": result,
              "history": [{"attempt": state["attempts"], "patch": patch, "verification": result}]}
    if result["passed"]:
        update["timestamps"] = {"verified": time.time()}
    return update
