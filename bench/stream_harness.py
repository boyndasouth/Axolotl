"""Streaming benchmark: break a live stream on purpose and measure the whole recovery.

    python -m bench.stream_harness --runs 3 --label groq-llama70b
    python -m bench.stream_harness --scenarios rename_column,bad_rows --runs 5
    python -m bench.stream_harness --oracle --runs 1          # no API key: plumbing check only

Each run starts a fresh, private copy of everything: a throwaway checkout of the
pipeline, an in-memory message bus, a producer at --rate records per second, the
stream processor, the detector and the agent graph. After a short warm-up it injects
the scenario mid-stream and measures:

  time to detect      drift injected -> incident opened (includes waiting for the next upstream page)
  time to verified PR incident opened -> PR opened (the batch benchmark's MTTR, same definition)
  time to recover     drift injected -> quarantined data replayed through the fix. The harness
                      approves instantly, so this excludes human review time.
  quarantined         records held in the dead-letter queue while the fix was being made
  recovered           of those, records loaded or correctly dropped by the fix on replay
  post-fix DLQ        records still quarantined in the live check after deploy (should be 0)

Success: repair scenarios end "resolved" (fix live, DLQ replayed, live check clean) AND the
patch passes the hidden set; escalate scenarios escalate; no_incident scenarios stay quiet.
Results go to bench/results.duckdb with mode = "stream".
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from langgraph.checkpoint.memory import MemorySaver

import agent.nodes.pr as pr_node
from agent import llm
from agent.config import DIAGNOSIS_MODEL, REPAIR_MODEL
from bench.harness import ARTIFACTS, RESULTS_DB, hidden_check, save, summarize
from bench.workdir import make_workdir
from chaos.catalog import list_scenarios, load_scenario
from chaos.control import MemoryDrift
from stream.bus import TOPICS, MemoryBus
from stream.runtime import TERMINAL, Runtime

QUIET_S = 25          # how long a no_incident scenario must stay quiet
TIMEOUT_S = 240       # per run


def _wait(pred, timeout: float, step: float = 0.2):
    end = time.time() + timeout
    while time.time() < end:
        v = pred()
        if v:
            return v
        time.sleep(step)
    return None


def run_one(scenario: str, run_idx: int, label: str, backend: str | None, batch_id: str,
            rate: float, live_check_s: float) -> dict:
    sc = load_scenario(scenario)
    expected = sc["expected_outcome"]
    tmp = Path(tempfile.mkdtemp(prefix=f"shp-sbench-{scenario}-"))
    bus = MemoryBus()
    bus.ensure_topics(TOPICS)
    rt = Runtime(workdir=make_workdir(tmp / "wd"), bus=bus, drift=MemoryDrift(), data_dir=tmp / "wd" / "data",
                 rate=rate, sandbox_backend=backend, checkpointer=MemorySaver(), live_check_s=live_check_s)
    row = {"run_id": f"stream-{uuid.uuid4().hex[:10]}", "batch_id": batch_id, "label": label, "mode": "stream",
           "started_at": datetime.now(timezone.utc).replace(tzinfo=None),
           "diagnosis_model": DIAGNOSIS_MODEL if label != "oracle" else "oracle",
           "repair_model": REPAIR_MODEL if label != "oracle" else "oracle",
           "sandbox_backend": backend or "auto", "scenario": scenario, "run_idx": run_idx, "seed": None,
           "expected_outcome": expected, "attempts": 0, "llm_calls": 0, "llm_cost_usd": 0.0}
    try:
        rt.start()
        _wait(lambda: rt.processor.sink.count() >= rate * 2, 30)      # warm up: ~2 s of clean traffic
        t_inject = time.time()
        rt.set_drift(scenario)

        if expected == "no_incident":
            time.sleep(QUIET_S)
            incs = rt.store.incidents()
            row.update(outcome="incident" if incs else "healthy", success=not incs, false_fix=False,
                       hidden_passed=None, wall_s=round(time.time() - t_inject, 3))
            return row

        inc = _wait(lambda: (rt.store.incidents() or [None])[0], TIMEOUT_S)
        if not inc:
            row.update(outcome="not_detected", success=False, false_fix=False, hidden_passed=None,
                       wall_s=round(time.time() - t_inject, 3))
            return row
        iid = inc["id"]
        cfg = {"configurable": {"thread_id": iid}}
        status = _wait(lambda: (lambda s: s if s in TERMINAL or s == "pr_open" else None)(
            rt.store.incident(iid)["status"]), TIMEOUT_S)

        hidden = None
        if status == "pr_open":
            files = rt.graph.get_state(cfg).values["patch"]["files"]
            hidden = hidden_check(rt.workdir, files, scenario, backend)
            rt.decide(iid, True)                                          # a human approving instantly
            status = _wait(lambda: (lambda s: s if s in TERMINAL else None)(rt.store.incident(iid)["status"]),
                           TIMEOUT_S)

        v = rt.graph.get_state(cfg).values
        ts = v.get("timestamps", {})
        rollout = v.get("rollout") or {}
        replay = rollout.get("replay") or {}
        live = rollout.get("live_check") or {}
        verification = v.get("verification") or {}

        if expected == "repair":
            success = status == "resolved" and bool(hidden)
        else:
            success = status == "escalated"
        span = lambda a, b: round(ts[b] - ts[a], 3) if a in ts and b in ts else None   # noqa: E731
        recover = None
        if "recovered" in ts and "pr_opened" in ts:
            # injection -> PR, then deploy -> replay done; the (instant) review and the live-check wait are excluded
            recover = round((ts["pr_opened"] - t_inject) + (ts["recovered"] - ts["deployed"] - live_check_s), 3)
        row.update({
            "run_id": iid, "outcome": status, "success": success,
            "false_fix": expected == "repair" and hidden is False, "hidden_passed": hidden,
            "attempts": v.get("attempts", 0),
            "mttr_s": span("detected", "pr_opened") if "pr_opened" in ts else span("detected", "escalated"),
            "detect_s": span("started", "detected"),
            "time_to_detect_s": round(ts["detected"] - t_inject, 3) if "detected" in ts else None,
            "diagnose_s": span("detected", "diagnosed"),
            "repair_verify_s": span("diagnosed", "verified"),
            "time_to_recover_s": recover,
            "quarantined": replay.get("replayed"),
            "recovered": (replay.get("replayed", 0) - replay.get("still_failing", 0)) if replay else None,
            "still_failing": replay.get("still_failing"),
            "post_fix_dlq": live.get("quarantined"),
            "wall_s": round(time.time() - t_inject, 3),
            "llm_calls": v.get("llm_calls", 0), "llm_cost_usd": v.get("llm_cost_usd", 0.0),
            "diagnosis_category": (v.get("diagnosis") or {}).get("category"),
            "escalation_reason": v.get("escalation_reason"),
            "final_summary": verification.get("summary"),
            "patch_diff": (v.get("patch") or {}).get("diff"),
            "sandbox_backend": verification.get("backend", backend or "auto"),
        })
        return row
    finally:
        try:
            rt.stop()
        except Exception:
            pass
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--scenarios", default="all")
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--label", default=None)
    p.add_argument("--backend", choices=["auto", "docker", "local"], default=None)
    p.add_argument("--rate", type=float, default=25.0, help="upstream records per second")
    p.add_argument("--live-check", type=float, default=3.0, help="seconds to watch the stream after deploy")
    p.add_argument("--oracle", action="store_true", help="scripted fake LLM, for smoke tests only")
    p.add_argument("--db", default=str(RESULTS_DB))
    a = p.parse_args()

    scenarios = list_scenarios() if a.scenarios == "all" else a.scenarios.split(",")
    label = "oracle" if a.oracle else (a.label or f"{DIAGNOSIS_MODEL} / {REPAIR_MODEL}")
    batch_id = time.strftime("stream-%Y%m%d-%H%M%S")
    pr_node.PR_MODE = "dry-run"
    pr_node.REPORTS_DIR = ARTIFACTS / batch_id

    rows = []
    for sc in scenarios:
        for i in range(a.runs):
            if a.oracle:
                from bench.oracle import OracleLLM
                llm.set_fake_llm(OracleLLM())
            r = run_one(sc, i, label, a.backend, batch_id, a.rate, a.live_check)
            rows.append(r)
            mark = "✓" if r["success"] else ("✗ false fix" if r.get("false_fix") else "✗")
            fmt = lambda v: f"{v:.1f}s" if isinstance(v, (int, float)) else "-"   # noqa: E731
            rec = f"{r.get('recovered')}/{r.get('quarantined')}" if r.get("quarantined") is not None else "-"
            print(f"{sc:16} run {i}: {r['outcome']:14} {mark:12} detect={fmt(r.get('time_to_detect_s'))} "
                  f"pr={fmt(r.get('mttr_s'))} recover={fmt(r.get('time_to_recover_s'))} recovered={rec}")
    for r in rows:
        r.setdefault("mttr_s", None)
    save(rows, Path(a.db))
    print("\n" + summarize(rows))
    print(f"\nsaved {len(rows)} runs to {a.db} (batch {batch_id}). "
          "See them in the console's Benchmark tab or: streamlit run dashboard/app.py")


if __name__ == "__main__":
    main()
