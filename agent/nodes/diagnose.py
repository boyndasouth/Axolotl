"""Diagnosis agent: reads logs, the schema diff, git history and the broken batch."""

from __future__ import annotations

import json
import time
from pathlib import Path

from agent.config import DIAGNOSIS_MODEL
from agent.llm import complete_json, model_label
from agent.nodes.rollout import emit
from agent.state import PipelineState
from agent.tools.context import (allowed_files, format_files, git_history, last_commit_diff,
                                 read_files, read_only_files, render_prompt, sample_records, tail_logs)
from healer.spec import load_spec

PROMPT = (Path(__file__).resolve().parents[1] / "prompts" / "diagnosis.md").read_text(encoding="utf-8")
CATEGORIES = {"upstream_schema_change", "upstream_type_change", "bad_rows", "code_regression",
              "upstream_outage", "unknown"}


def build_context(state: PipelineState) -> str:
    workdir = Path(state["workdir"])
    report = dict(state["anomaly"]["report"])
    batch = state.get("incident_batch")
    spec = load_spec(workdir)
    samples = sample_records(batch, key=spec.key) if batch and Path(batch).exists() else []
    commits = git_history(workdir)
    where = ", ".join(f"{p}/" for p in spec.code if p != spec.tests)
    sections = [
        ("ANOMALY", state["anomaly"]["summary"]),
        ("RUN REPORT", json.dumps({k: report.get(k) for k in
                                   ("status", "failed_check", "rows_in", "error", "failures")}, indent=2)),
        ("SCHEMA DIFF (last known-good batch -> this batch)", json.dumps(report.get("schema_diff"), indent=2)),
        ("RECENT PIPELINE LOGS", "\n".join(json.dumps(e) for e in tail_logs(workdir))),
        (f"GIT HISTORY ({where})", json.dumps(commits, indent=2) if commits else "(no git history available)"),
        (f"LAST COMMIT DIFF ({where})", last_commit_diff(workdir) or "(none)"),
        (f"SAMPLE RECORDS FROM THE BATCH THAT BROKE ({len(samples)} of {report.get('rows_in')})",
         json.dumps(samples, indent=1, ensure_ascii=False)),
        ("PIPELINE CODE", format_files(read_files(workdir, allowed_files(workdir) + read_only_files(workdir)[:1]))),
    ]
    return "\n\n".join(f"## {title}\n{body}" for title, body in sections)


def normalize(raw: dict, allowed: list[str]) -> dict:
    targets = [t for t in raw.get("target_files", []) if t in allowed]
    repairable = bool(raw.get("repairable", False))
    if repairable and not targets:
        targets = list(allowed)
    category = raw.get("category", "unknown")
    return {
        "root_cause": str(raw.get("root_cause", ""))[:2000],
        "category": category if category in CATEGORIES else "unknown",
        "repairable": repairable,
        "confidence": max(0.0, min(1.0, float(raw.get("confidence", 0.0)))),
        "evidence": [str(e)[:400] for e in raw.get("evidence", [])][:10],
        "target_files": targets,
        "fix_plan": str(raw.get("fix_plan", ""))[:1000],
    }


def diagnose(state: PipelineState) -> dict:
    emit({"stage": "llm_call", "role": "diagnosis", "model": model_label(DIAGNOSIS_MODEL)})
    try:
        workdir = Path(state["workdir"])
        raw, cost, calls = complete_json("diagnosis", DIAGNOSIS_MODEL, render_prompt(PROMPT, workdir),
                                         build_context(state))
        diagnosis = normalize(raw, allowed_files(workdir))
    except Exception as e:   # unusable model output or API failure -> escalate, don't guess
        diagnosis = {"root_cause": f"diagnosis failed: {type(e).__name__}: {e}"[:500],
                     "category": "unknown", "repairable": False, "confidence": 0.0,
                     "evidence": [], "target_files": [], "fix_plan": "", "error": True}
        cost, calls = 0.0, 1
    return {"diagnosis": diagnosis, "status": "repairing", "llm_cost_usd": cost,
            "llm_calls": calls, "timestamps": {"diagnosed": time.time()}}
