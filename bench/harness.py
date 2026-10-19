"""Failure-injection benchmark: mean time-to-repair across the chaos suite.

    python -m bench.harness --runs 3 --label groq-llama70b
    python -m bench.harness --scenarios rename_column,bad_rows --runs 5
    python -m bench.harness --oracle --runs 1            # no API key: smoke test only

Every run gets a fresh throwaway copy of the pipeline, a freshly injected failure
(new seed each run), and the full agent graph with PRs in dry-run mode. The clock
stops at "verified PR opened"; human merge time is excluded as noise.

Success depends on the scenario's expected outcome:
  repair       a PR was opened AND the patch also passes the hidden set
  escalate     the agent escalated instead of patching
  no_incident  the agent did not open an incident at all
A false fix is a patch that passed the sandbox but fails the hidden set.

Results go to bench/results.duckdb (table: runs); the dashboard reads them.
"""

from __future__ import annotations

import argparse
import json
import shutil
import statistics
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

import agent.nodes.pr as pr_node
from agent import llm
from agent.cli import run_incident
from agent.config import DIAGNOSIS_MODEL, REPAIR_MODEL
from agent.graph import build_graph
from bench.workdir import make_workdir
from chaos.catalog import FIXTURE_DIR, list_scenarios, load_scenario
from chaos.inject import make_incident_batch
from sandbox.runner import verify_patch

BENCH_DIR = Path(__file__).resolve().parent
RESULTS_DB = BENCH_DIR / "results.duckdb"
ARTIFACTS = BENCH_DIR / "artifacts"

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id VARCHAR, batch_id VARCHAR, label VARCHAR, started_at TIMESTAMP,
    diagnosis_model VARCHAR, repair_model VARCHAR, sandbox_backend VARCHAR,
    scenario VARCHAR, run_idx INTEGER, seed INTEGER, expected_outcome VARCHAR,
    outcome VARCHAR, success BOOLEAN, false_fix BOOLEAN, hidden_passed BOOLEAN,
    attempts INTEGER, mttr_s DOUBLE, detect_s DOUBLE, diagnose_s DOUBLE, repair_verify_s DOUBLE,
    wall_s DOUBLE, llm_calls INTEGER, llm_cost_usd DOUBLE, diagnosis_category VARCHAR,
    escalation_reason VARCHAR, final_summary VARCHAR, patch_diff VARCHAR
)"""

# Columns added for the streaming benchmark (bench/stream_harness.py). Older result
# files get them on the next save, so batch and stream runs share one table.
EXTRA_COLUMNS = {
    "mode": "VARCHAR",                 # batch | stream
    "time_to_detect_s": "DOUBLE",      # stream: drift injected -> incident opened
    "time_to_recover_s": "DOUBLE",     # stream: drift injected -> quarantined data recovered (review time excluded)
    "quarantined": "INTEGER",          # stream: records held in the DLQ and replayed
    "recovered": "INTEGER",            # stream: of those, loaded or correctly dropped by the fix
    "still_failing": "INTEGER",
    "post_fix_dlq": "INTEGER",         # stream: records quarantined in the live check after deploy
}


def hidden_check(workdir: Path, files: dict, scenario: str, backend: str | None) -> bool:
    """Run the final patch against the hidden split, which the sandbox never saw."""
    tmp = Path(tempfile.mkdtemp(prefix="shp-hidden-"))
    try:
        d = tmp / scenario
        d.mkdir()
        shutil.copy(FIXTURE_DIR / scenario / "hidden.json", d / "holdout.json")
        shutil.copy(FIXTURE_DIR / scenario / "hidden_expected.json", d / "holdout_expected.json")
        v = verify_patch(workdir, files, scenario_id=scenario, incident_batch=None,
                         backend=backend, fixture_dir=tmp)
        return v["schema_checks"].get("holdout", {}).get("ok", False) and v["golden_diff_rows"] == 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _span(ts: dict, a: str, b: str) -> float | None:
    return round(ts[b] - ts[a], 3) if a in ts and b in ts else None


def run_one(scenario: str, run_idx: int, label: str, backend: str | None, batch_id: str) -> dict:
    sc = load_scenario(scenario)
    seed = 5000 + run_idx * 101 + sum(map(ord, scenario))
    tmp = Path(tempfile.mkdtemp(prefix=f"shp-bench-{scenario}-"))
    try:
        wd = make_workdir(tmp / "wd")
        batch = make_incident_batch(scenario, out=wd / "data" / "incoming" / "orders.json", seed=seed)
        graph = build_graph(wait_for_merge=False)
        t0 = time.time()
        final = run_incident(graph, f"file:{batch}", scenario, backend, workdir=str(wd), verbose=False)
        wall = time.time() - t0

        ts, status = final.get("timestamps", {}), final.get("status")
        patch = final.get("patch") or {}
        expected = sc["expected_outcome"]
        hidden = None
        if status == "pr_open" and patch.get("files"):
            hidden = hidden_check(wd, patch["files"], scenario, backend)

        if expected == "repair":
            success = status == "pr_open" and bool(hidden)
        elif expected == "escalate":
            success = status == "escalated"
        else:
            success = status == "healthy"

        v = final.get("verification") or {}
        return {
            "run_id": final.get("incident_id", uuid.uuid4().hex), "batch_id": batch_id, "label": label,
            "started_at": datetime.now(timezone.utc).replace(tzinfo=None),
            "diagnosis_model": DIAGNOSIS_MODEL if label != "oracle" else "oracle",
            "repair_model": REPAIR_MODEL if label != "oracle" else "oracle",
            "sandbox_backend": v.get("backend", backend or "auto"),
            "scenario": scenario, "run_idx": run_idx, "seed": seed, "expected_outcome": expected,
            "outcome": status, "success": success,
            "false_fix": expected == "repair" and status == "pr_open" and hidden is False,
            "hidden_passed": hidden,
            "attempts": final.get("attempts", 0),
            "mttr_s": _span(ts, "detected", "pr_opened") if status == "pr_open" else _span(ts, "detected", "escalated"),
            "detect_s": _span(ts, "started", "detected"),
            "diagnose_s": _span(ts, "detected", "diagnosed"),
            "repair_verify_s": _span(ts, "diagnosed", "verified"),
            "wall_s": round(wall, 3),
            "llm_calls": final.get("llm_calls", 0), "llm_cost_usd": final.get("llm_cost_usd", 0.0),
            "diagnosis_category": (final.get("diagnosis") or {}).get("category"),
            "escalation_reason": final.get("escalation_reason"),
            "final_summary": v.get("summary"),
            "patch_diff": patch.get("diff"),
            "mode": "batch",
        }
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def save(rows: list[dict], db: Path = RESULTS_DB) -> None:
    con = duckdb.connect(str(db))
    try:
        con.execute(SCHEMA)
        for col, typ in EXTRA_COLUMNS.items():
            con.execute(f"ALTER TABLE runs ADD COLUMN IF NOT EXISTS {col} {typ}")
        cols = [row[0] for row in con.execute("DESCRIBE runs").fetchall()]
        for r in rows:
            con.execute(f"INSERT INTO runs ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
                        [r.get(c) for c in cols])
    finally:
        con.close()


def summarize(rows: list[dict]) -> str:
    out = [f"{'scenario':18} {'expect':11} {'success':>8} {'false fix':>9} {'MTTR p50':>9} {'attempts':>8}"]
    for sc in sorted({r["scenario"] for r in rows}):
        rs = [r for r in rows if r["scenario"] == sc]
        mt = [r["mttr_s"] for r in rs if r["success"] and r["mttr_s"] is not None]
        out.append(f"{sc:18} {rs[0]['expected_outcome']:11} {sum(r['success'] for r in rs):>4}/{len(rs):<3} "
                   f"{sum(r['false_fix'] for r in rs):>9} "
                   f"{(f'{statistics.median(mt):.1f}s' if mt else '-'):>9} "
                   f"{statistics.mean(r['attempts'] for r in rs):>8.1f}")
    ok = sum(r["success"] for r in rows)
    mt = [r["mttr_s"] for r in rows if r["success"] and r["expected_outcome"] == "repair" and r["mttr_s"]]
    out.append(f"\noverall: {ok}/{len(rows)} succeeded, "
               f"{sum(r['false_fix'] for r in rows)} false fixes, "
               f"mean time-to-repair {statistics.mean(mt):.1f}s" if mt else f"\noverall: {ok}/{len(rows)} succeeded")
    return "\n".join(out)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--scenarios", default="all")
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--label", default=None)
    p.add_argument("--backend", choices=["auto", "docker", "local"], default=None)
    p.add_argument("--oracle", action="store_true", help="scripted fake LLM, for smoke tests only")
    p.add_argument("--db", default=str(RESULTS_DB))
    a = p.parse_args()

    scenarios = list_scenarios() if a.scenarios == "all" else a.scenarios.split(",")
    label = "oracle" if a.oracle else (a.label or f"{DIAGNOSIS_MODEL} / {REPAIR_MODEL}")
    batch_id = time.strftime("%Y%m%d-%H%M%S")
    pr_node.PR_MODE = "dry-run"
    pr_node.REPORTS_DIR = ARTIFACTS / batch_id

    rows = []
    for sc in scenarios:
        for i in range(a.runs):
            if a.oracle:
                from bench.oracle import OracleLLM
                llm.set_fake_llm(OracleLLM())
            r = run_one(sc, i, label, a.backend, batch_id)
            rows.append(r)
            mark = "✓" if r["success"] else ("✗ false fix" if r["false_fix"] else "✗")
            mttr = f"{r['mttr_s']:.1f}s" if r["mttr_s"] is not None else "-"
            print(f"{sc:18} run {i}: {r['outcome']:10} {mark:12} attempts={r['attempts']} mttr={mttr}")
    save(rows, Path(a.db))
    print("\n" + summarize(rows))
    print(f"\nsaved {len(rows)} runs to {a.db} (batch {batch_id}). Dashboard: streamlit run dashboard/app.py")


if __name__ == "__main__":
    main()
