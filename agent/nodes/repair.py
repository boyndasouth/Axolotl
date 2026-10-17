"""Repair agent: writes a patch to the allowed files, using verifier feedback on retries."""

from __future__ import annotations

import difflib
import json
import re
from pathlib import Path

from agent.config import REPAIR_MODEL
from agent.llm import complete, model_label
from agent.nodes.rollout import emit
from agent.state import PipelineState
from agent.tools.context import (allowed_files, format_files, read_files, read_only_files,
                                 render_prompt, sample_records)
from healer.spec import load_spec
from sandbox.runner import PatchRejected, check_allowed

PROMPT = (Path(__file__).resolve().parents[1] / "prompts" / "repair.md").read_text(encoding="utf-8")
FILE_BLOCK = re.compile(r"=== FILE: (?P<path>[^\n=]+?) ===\n(?P<body>.*?)\n?=== END FILE ===", re.S)


def build_context(state: PipelineState) -> str:
    workdir = Path(state["workdir"])
    d = state["diagnosis"]
    batch = state.get("incident_batch")
    samples = sample_records(batch, n=10, seed=state.get("attempts", 0),
                             key=load_spec(workdir).key) if batch else []
    parts = [
        "## DIAGNOSIS\n" + json.dumps({k: d[k] for k in ("root_cause", "category", "evidence", "fix_plan")}, indent=2),
        "## ALLOWED FILES (you may edit these)\n" + format_files(read_files(workdir, d["target_files"] or allowed_files(workdir))),
        "## READ-ONLY FILES (the contract and the tests; do not edit)\n" + format_files(read_files(workdir, read_only_files(workdir))),
        f"## SAMPLE RECORDS FROM THE BATCH THAT BROKE\n{json.dumps(samples, indent=1, ensure_ascii=False)}",
    ]
    for a in state.get("history", []):
        parts.append(f"## ATTEMPT {a['attempt']} FAILED VERIFICATION\n"
                     f"Your patch:\n{a['patch']['diff'] or a['patch'].get('error')}\n\n"
                     f"Verifier feedback:\n{a['verification']['summary'][:2500]}")
    return "\n\n".join(parts)


def parse_patch(text: str, workdir: Path) -> dict:
    rationale = re.search(r"RATIONALE:\s*(.+)", text)
    files = {}
    for m in FILE_BLOCK.finditer(text):
        path = check_allowed(m.group("path").strip(), workdir)   # raises PatchRejected
        body = m.group("body")
        body = re.sub(r"^```[a-zA-Z]*\n|\n```\s*$", "", body)  # tolerate code fences
        files[path] = body.rstrip("\n") + "\n"
    if not files:
        raise ValueError("no '=== FILE: ... ===' blocks in the reply")
    diff = []
    for path, new in files.items():
        old_path = workdir / path
        old = old_path.read_text(encoding="utf-8") if old_path.exists() else ""
        diff.extend(difflib.unified_diff(old.splitlines(keepends=True), new.splitlines(keepends=True),
                                         fromfile=f"a/{path}", tofile=f"b/{path}"))
    return {"files": files, "diff": "".join(diff),
            "rationale": rationale.group(1).strip() if rationale else "",
            "files_changed": sorted(files), "error": None}


def repair(state: PipelineState) -> dict:
    attempt = state.get("attempts", 0) + 1
    workdir = Path(state["workdir"])
    cost = 0.0
    emit({"stage": "llm_call", "role": "repair", "model": model_label(REPAIR_MODEL), "attempt": attempt})
    try:
        text, cost = complete("repair", REPAIR_MODEL, render_prompt(PROMPT, workdir), build_context(state))
        patch = parse_patch(text, workdir)
    except (PatchRejected, ValueError) as e:
        patch = {"files": {}, "diff": "", "rationale": "", "files_changed": [],
                 "error": f"unusable repair output: {e}"}
    except Exception as e:   # API error: count it as a failed attempt
        patch = {"files": {}, "diff": "", "rationale": "", "files_changed": [],
                 "error": f"repair model call failed: {type(e).__name__}: {e}"[:500]}
    return {"patch": patch, "attempts": attempt, "status": "verifying",
            "llm_cost_usd": cost, "llm_calls": 1}
