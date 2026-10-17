"""Markdown bodies for PRs and escalations: the evidence a human reviews before merging."""

from __future__ import annotations

from agent.state import PipelineState


def _fmt_seconds(ts: dict, a: str, b: str) -> str:
    return f"{ts[b] - ts[a]:.1f}s" if a in ts and b in ts else "n/a"


def evidence_md(state: PipelineState) -> str:
    a, d = state.get("anomaly") or {}, state.get("diagnosis") or {}
    report = a.get("report", {})
    lines = [
        "## What broke",
        f"- **Anomaly:** `{a.get('kind')}` at `{a.get('failed_check')}` (run `{a.get('run_id')}`)",
        f"- {a.get('summary', '')}",
        f"- **Rows in:** {report.get('rows_in')}",
        "",
        "## Diagnosis",
        f"{d.get('root_cause', '')}",
        "",
        f"- **Category:** `{d.get('category')}`, confidence {d.get('confidence', 0):.2f}",
        f"- **Plan:** {d.get('fix_plan', '')}",
    ]
    if d.get("evidence"):
        lines += ["", "**Evidence**"] + [f"- {e}" for e in d["evidence"]]
    if report.get("schema_diff"):
        lines += ["", "**Schema diff**", "```json", __import__("json").dumps(report["schema_diff"], indent=2), "```"]
    return "\n".join(lines)


def pr_body(state: PipelineState) -> str:
    p, v = state["patch"], state["verification"]
    ts = state.get("timestamps", {})
    checks = v.get("schema_checks", {})
    rows = [
        ("Unit tests", f"{v['unit_tests'].get('passed', 0)} passed, {v['unit_tests'].get('failed', 0)} failed"),
        ("Batch that broke", _check_cell(checks.get("incident"))),
        ("Held-out batch vs golden output",
         "n/a (no fixture for this incident)" if v["golden_diff_rows"] == -1
         else f"{v['golden_diff_rows']} rows differ"),
        ("Regressions", ", ".join(v["regressions"]) or "none"),
        ("Sandbox", f"{v['backend']}, {v['duration_s']}s"),
    ]
    tried = "\n".join(f"- Attempt {h['attempt']}: {'passed' if h['verification']['passed'] else 'failed'}"
                      f" ({h['verification']['summary'][:160].splitlines()[0]})"
                      for h in state.get("history", []))
    return "\n".join([
        f"Automated repair for incident `{state['incident_id']}`. **A human must review and merge.**",
        "",
        evidence_md(state),
        "",
        "## The fix",
        p.get("rationale", ""),
        "",
        "```diff", p["diff"], "```",
        "",
        "## Verification (sandbox, before this PR was opened)",
        "| Check | Result |", "|---|---|",
        *[f"| {k} | {val} |" for k, val in rows],
        "",
        "## Attempts", tried,
        "",
        "## Timing",
        f"- Detected → diagnosed: {_fmt_seconds(ts, 'detected', 'diagnosed')}",
        f"- Diagnosed → verified: {_fmt_seconds(ts, 'diagnosed', 'verified')}",
        f"- LLM calls: {state.get('llm_calls', 0)}, cost ${state.get('llm_cost_usd', 0):.4f}",
    ])


def _check_cell(c: dict | None) -> str:
    if not c:
        return "n/a"
    if not c["ok"]:
        return f"fails: {c['error']}"
    return f"passes contract, {c['rows_out']}/{c['rows_in']} rows kept"


def escalation_body(state: PipelineState) -> str:
    tried = "\n\n".join(
        f"### Attempt {h['attempt']}\n```diff\n{h['patch']['diff'] or h['patch'].get('error')}\n```\n"
        f"Verifier: {h['verification']['summary'][:1500]}"
        for h in state.get("history", []))
    return "\n".join([
        f"# Escalation: incident `{state['incident_id']}`",
        "",
        f"**Why a human is needed:** {state.get('escalation_reason')}",
        "",
        evidence_md(state),
        "",
        "## Repair attempts" if tried else "",
        tried,
    ])
