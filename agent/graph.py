"""LangGraph definition: monitor -> diagnose -> repair <-> verify -> PR -> human merge.

    monitor      deterministic checks on a pipeline run (no LLM)
    diagnose     LLM: logs, schema diff, git history, sample rows -> root cause
    repair       LLM: writes a patch, restricted to the allowed files
    verify       deterministic: sandbox, unit tests, held-out data, golden diff, regressions
    open_pr      GitHub PR with the evidence (or reports/prs/ in dry-run mode)
    await_merge  interrupt(): pauses until a human merges or closes the PR
    rollout      after a merge: make the fix live and recover quarantined data (streaming only)
    escalate     report (and GitHub issue) with everything gathered so far
"""

from __future__ import annotations

import sqlite3
import time

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from agent.config import CHECKPOINT_DB, CONFIDENCE_THRESHOLD, MAX_ATTEMPTS
from agent.nodes.diagnose import diagnose
from agent.nodes.monitor import monitor
from agent.nodes.pr import escalate, open_pr
from agent.nodes.repair import repair
from agent.nodes.rollout import rollout
from agent.nodes.verify import verify
from agent.state import PipelineState


def await_merge(state: PipelineState) -> dict:
    # Pauses the graph. Resume with Command(resume={"merged": True/False}),
    # from the CLI or the GitHub webhook (agent/webhook.py).
    decision = interrupt({"pr_url": state["pr_url"], "incident_id": state["incident_id"]})
    merged = bool(decision.get("merged", False))
    return {"status": "merged" if merged else "rejected", "timestamps": {"resolved": time.time()}}


# ---- Routing -----------------------------------------------------------------

def route_after_monitor(state: PipelineState) -> str:
    return END if state["status"] == "healthy" else "diagnose"


def route_after_diagnose(state: PipelineState) -> str:
    d = state["diagnosis"]
    if not d["repairable"] or d["confidence"] < CONFIDENCE_THRESHOLD:
        return "escalate"
    return "repair"


def route_after_verify(state: PipelineState) -> str:
    if state["verification"]["passed"]:
        return "open_pr"
    if state["attempts"] < state.get("max_attempts", MAX_ATTEMPTS):
        return "repair"
    return "escalate"


# ---- Graph -------------------------------------------------------------------

def build_graph(checkpointer=None, wait_for_merge: bool = True):
    g = StateGraph(PipelineState)
    g.add_node("monitor", monitor)
    g.add_node("diagnose", diagnose)
    g.add_node("repair", repair)
    g.add_node("verify", verify)
    g.add_node("open_pr", open_pr)
    g.add_node("escalate", escalate)

    g.add_edge(START, "monitor")
    g.add_conditional_edges("monitor", route_after_monitor, ["diagnose", END])
    g.add_conditional_edges("diagnose", route_after_diagnose, ["repair", "escalate"])
    g.add_edge("repair", "verify")
    g.add_conditional_edges("verify", route_after_verify, ["open_pr", "repair", "escalate"])
    g.add_edge("escalate", END)
    if wait_for_merge:
        g.add_node("await_merge", await_merge)
        g.add_node("rollout", rollout)
        g.add_edge("open_pr", "await_merge")
        g.add_edge("await_merge", "rollout")
        g.add_edge("rollout", END)
    else:   # the benchmark stops the clock at "verified PR opened"
        g.add_edge("open_pr", END)
    return g.compile(checkpointer=checkpointer or MemorySaver())


def persistent_checkpointer(path=None):
    """SQLite checkpoints, so an incident waiting for merge survives restarts."""
    from pathlib import Path

    from langgraph.checkpoint.sqlite import SqliteSaver
    db = Path(path or CHECKPOINT_DB)
    db.parent.mkdir(parents=True, exist_ok=True)
    return SqliteSaver(sqlite3.connect(str(db), check_same_thread=False))
