"""Command line for the agent.

    python -m agent.cli run --source file:data/incoming/orders.json --scenario rename_column
    python -m agent.cli run --source api
    python -m agent.cli watch --source api --interval 60
    python -m agent.cli resume <incident_id> --merged      (or --rejected)
    python -m agent.cli list
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid

from langgraph.types import Command

from agent.config import CHECKPOINT_DB, MAX_ATTEMPTS, PR_MODE, REPO_ROOT
from agent.graph import build_graph, persistent_checkpointer

ICONS = {"monitor": "🔎", "diagnose": "🩺", "repair": "🔧", "verify": "🧪",
         "open_pr": "📬", "escalate": "🚨", "await_merge": "👤"}


def _line(node: str, upd: dict) -> str:
    if node == "monitor":
        a = upd.get("anomaly")
        return "healthy" if not a else f"anomaly [{a['kind']}] {a['summary']}"
    if node == "diagnose":
        d = upd["diagnosis"]
        return (f"{d['category']} (confidence {d['confidence']:.2f}, "
                f"{'repairable' if d['repairable'] else 'NOT repairable'})\n      {d['root_cause'][:300]}")
    if node == "repair":
        p = upd["patch"]
        return f"attempt {upd['attempts']}: " + (p["error"] or f"{', '.join(p['files_changed'])}: {p['rationale'][:200]}")
    if node == "verify":
        v = upd["verification"]
        return ("PASSED " if v["passed"] else "FAILED ") + v["summary"].splitlines()[0][:300]
    if node == "open_pr":
        return f"PR ready: {upd['pr_url']}"
    if node == "escalate":
        return f"escalated: {upd['escalation_reason']}"
    if node == "rollout" and upd.get("rollout"):
        r = upd["rollout"]
        rp = r.get("replay") or {}
        return (f"{upd['status']}: replayed {rp.get('replayed', 0)} quarantined records, "
                f"{rp.get('still_failing', 0)} still failing")
    return ""


def new_incident_id() -> str:
    return time.strftime("inc-%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:4]


def run_incident(graph, source: str, scenario: str | None, backend: str | None,
                 workdir: str | None = None, verbose: bool = True) -> dict:
    incident_id = new_incident_id()
    cfg = {"configurable": {"thread_id": incident_id}, "recursion_limit": 50}
    init = {"incident_id": incident_id, "workdir": workdir or str(REPO_ROOT), "source": source,
            "scenario_id": scenario, "sandbox_backend": backend, "attempts": 0,
            "max_attempts": MAX_ATTEMPTS, "history": [], "timestamps": {"started": time.time()},
            "llm_cost_usd": 0.0, "llm_calls": 0}
    if verbose:
        print(f"incident {incident_id}  (PR mode: {PR_MODE})")
    for event in graph.stream(init, cfg, stream_mode="updates"):
        for node, upd in event.items():
            if node == "__interrupt__":
                if verbose:
                    print(f"  {ICONS['await_merge']} waiting for a human to merge.\n"
                          f"     resume with: python -m agent.cli resume {incident_id} --merged")
                continue
            if verbose and upd:
                print(f"  {ICONS.get(node, '•')} {node:9} {_line(node, upd)}")
    return graph.get_state(cfg).values


def main() -> None:
    p = argparse.ArgumentParser(prog="agent.cli")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run the pipeline once and heal it if it breaks")
    r.add_argument("--source", default="api")
    r.add_argument("--scenario", help="chaos scenario id, enables the held-out golden check")
    r.add_argument("--backend", choices=["auto", "docker", "local"], default=None)
    w = sub.add_parser("watch", help="poll the pipeline and heal on failure")
    w.add_argument("--source", default="api")
    w.add_argument("--interval", type=int, default=60)
    w.add_argument("--backend", choices=["auto", "docker", "local"], default=None)
    s = sub.add_parser("resume", help="record the human decision on a PR")
    s.add_argument("incident_id")
    g = s.add_mutually_exclusive_group(required=True)
    g.add_argument("--merged", action="store_true")
    g.add_argument("--rejected", action="store_true")
    sub.add_parser("list", help="incidents in the checkpoint store")
    a = p.parse_args()

    graph = build_graph(checkpointer=persistent_checkpointer())

    if a.cmd == "run":
        final = run_incident(graph, a.source, a.scenario, a.backend)
        sys.exit(0 if final.get("status") in ("healthy", "pr_open") else 1)

    if a.cmd == "watch":
        print(f"watching {a.source} every {a.interval}s (Ctrl+C to stop)")
        while True:
            final = run_incident(graph, a.source, None, a.backend, verbose=True)
            if final.get("status") not in ("healthy",):
                print("incident handled; pausing watch until it is resolved. Re-run watch after merging.")
                return
            time.sleep(a.interval)

    if a.cmd == "resume":
        cfg = {"configurable": {"thread_id": a.incident_id}}
        if not graph.get_state(cfg).next:
            sys.exit(f"incident {a.incident_id} is not waiting for a decision")
        graph.invoke(Command(resume={"merged": a.merged}), cfg)
        st = graph.get_state(cfg).values
        ts = st["timestamps"]
        print(f"{a.incident_id}: {st['status']}. Detected → resolved: {ts['resolved'] - ts['detected']:.0f}s")

    if a.cmd == "list":
        import sqlite3
        con = sqlite3.connect(str(CHECKPOINT_DB))
        ids = [r[0] for r in con.execute("SELECT DISTINCT thread_id FROM checkpoints ORDER BY thread_id")]
        for tid in ids:
            v = graph.get_state({"configurable": {"thread_id": tid}}).values
            print(f"{tid}  {v.get('status', '?'):10} {(v.get('anomaly') or {}).get('summary', '')[:80]}")


if __name__ == "__main__":
    main()
