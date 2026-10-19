"""Rollout: after a human merges, put the fix live and recover the quarantined data.

The graph doesn't know how the pipeline is deployed. Whoever runs it registers a
hook: the streaming runtime's hook applies the patch, hot-reloads the processor,
replays the dead-letter queue, and watches the live stream for a few seconds. With
no hook (the batch CLI), this node does nothing and the incident ends at "merged".
"""

from __future__ import annotations

import time
from typing import Callable

from agent.state import PipelineState

_hook: Callable[[PipelineState], dict] | None = None


def set_rollout_hook(fn: Callable[[PipelineState], dict] | None) -> None:
    global _hook
    _hook = fn


def emit(event: dict) -> None:
    """Progress for live viewers (stream_mode="custom"); a no-op outside a streamed run."""
    try:
        from langgraph.config import get_stream_writer
        get_stream_writer()(event)
    except Exception:
        pass


def rollout(state: PipelineState) -> dict:
    if state.get("status") != "merged" or _hook is None:
        return {}
    emit({"stage": "rollout_started"})
    t0 = time.time()
    try:
        result = _hook(state)
    except Exception as e:
        result = {"healthy": False, "error": f"{type(e).__name__}: {e}"[:500]}
    ts = {"deployed": result.get("deployed_at", t0)}
    if result.get("healthy"):
        ts["recovered"] = result.get("recovered_at", time.time())
    return {"rollout": result, "status": "resolved" if result.get("healthy") else "rollout_failed",
            "timestamps": ts}
