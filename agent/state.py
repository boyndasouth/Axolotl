"""Shared state for the self-healing pipeline agent graph."""

from __future__ import annotations

import operator
from typing import Annotated, Literal, Optional, TypedDict


def merge_dicts(left: dict, right: dict) -> dict:
    """Reducer: nodes add stage timestamps without overwriting earlier ones."""
    return {**(left or {}), **(right or {})}


# ---- Structured payloads passed between agents --------------------------------

class Anomaly(TypedDict):
    kind: Literal["schema_drift", "type_change", "null_spike", "row_count", "duplicates",
                  "validation_error", "ingest_error"]
    failed_check: str           # ingest | min_rows | transform | schema_validation
    summary: str                # one line, human readable
    run_id: str
    report: dict                # full run report from pipeline.run (error, failures, schema diff)


class Diagnosis(TypedDict):
    root_cause: str
    category: Literal["upstream_schema_change", "upstream_type_change", "bad_rows",
                      "code_regression", "upstream_outage", "unknown"]
    repairable: bool            # False -> escalate (e.g. upstream outage, no code fix)
    confidence: float           # 0..1, self-reported, gated by a threshold
    evidence: list[str]         # log lines, schema diff hunks, suspect commit SHAs
    target_files: list[str]     # files the repair agent is allowed to touch
    fix_plan: str               # what the repair should do, in one or two sentences


class Patch(TypedDict):
    files: dict[str, str]       # path -> full new content
    diff: str                   # unified diff, for the PR and the dashboard
    rationale: str
    files_changed: list[str]
    error: Optional[str]        # set when the repair agent's output was unusable


class Verification(TypedDict):
    passed: bool
    unit_tests: dict
    schema_checks: dict
    golden_diff_rows: int       # rows differing from expected output (0 = match, -1 = n/a)
    regressions: list[str]
    sandbox_logs: str
    duration_s: float
    backend: str
    summary: str                # what failed, fed back to the repair agent


class Attempt(TypedDict):
    attempt: int
    patch: Patch
    verification: Verification


Status = Literal["monitoring", "healthy", "diagnosing", "repairing", "verifying",
                 "pr_open", "merged", "rejected", "escalated",
                 "resolved", "rollout_failed"]     # streaming: fix is live and the DLQ was replayed


# ---- Graph state -------------------------------------------------------------

class PipelineState(TypedDict, total=False):
    # identity and inputs
    incident_id: str
    workdir: str                        # checkout of the pipeline repo being healed
    source: str                         # "api", "file:<path>"
    scenario_id: Optional[str]          # set when a chaos scenario was injected; enables held-out golden checks
    sandbox_backend: Optional[str]      # docker | local | auto

    # stage outputs
    run_report: dict
    incident_batch: Optional[str]       # raw snapshot of the batch that broke
    live_sample: Optional[str]          # dir with raw.json + expected.json from real clean traffic
    anomaly: Optional[Anomaly]
    diagnosis: Optional[Diagnosis]
    patch: Optional[Patch]
    verification: Optional[Verification]

    # retry loop
    attempts: int
    max_attempts: int
    history: Annotated[list[Attempt], operator.add]   # fed back to the repair agent

    # outcome
    status: Status
    pr_url: Optional[str]
    escalation_reason: Optional[str]
    rollout: Optional[dict]             # streaming only: patch applied, DLQ replay, live check

    # metrics: MTTR is read straight from these
    timestamps: Annotated[dict[str, float], merge_dicts]
    llm_cost_usd: Annotated[float, operator.add]
    llm_calls: Annotated[int, operator.add]
