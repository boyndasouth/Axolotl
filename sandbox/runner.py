"""Verify a candidate patch in a sandbox. This is the gate: nothing reaches a PR
unless it passes here, and the decision is made by this code, not by an LLM.

A patch passes only if ALL of these hold for the patched code:
  1. the pipeline unit tests pass
  2. the incident batch (the data that broke) now transforms and meets the contract,
     without dropping more than MAX_DROP_FRACTION of rows
  3. the scenario's held-out batch, when one exists, matches the golden output exactly
  4. no regressions: every fixture the original code handled correctly still matches,
     including the live sample (real records and the clean rows the current code made
     from them), so a fix can't quietly change rows that were already right

What the pipeline is (its code, contract, tests, editable files, fixtures) comes from
healer.yaml in the working directory; see healer/spec.py.

Backends:
  docker  throwaway container, --network none, CPU/memory/pid limits (default when available)
  local   plain subprocess with a timeout; NOT isolated, for dev machines without Docker
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from healer.spec import PipelineSpec, load_spec

HERE = Path(__file__).resolve().parent
IMAGE = os.environ.get("SHP_SANDBOX_IMAGE", "shp-sandbox:latest")
MAX_DROP_FRACTION = 0.2
LIVE_SAMPLE = "live_sample"
TIMEOUT_S = int(os.environ.get("SHP_SANDBOX_TIMEOUT", "180"))


class PatchRejected(ValueError):
    pass


def check_allowed(path: str, workdir: Path | PipelineSpec) -> str:
    """The patch may only touch the spec's editable files, never the contract or tests."""
    spec = workdir if isinstance(workdir, PipelineSpec) else load_spec(workdir)
    norm = path.strip().replace("\\", "/")
    while norm.startswith("./"):
        norm = norm[2:]
    if ".." in norm.split("/") or not spec.is_editable(norm):
        raise PatchRejected(f"patch touches {path!r}; allowed: {list(spec.editable)} "
                            f"(never {list(spec.protected)})")
    return norm


def docker_available() -> bool:
    try:
        ok = subprocess.run(["docker", "image", "inspect", IMAGE], capture_output=True, timeout=15)
        return ok.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def pick_backend(requested: str | None = None) -> str:
    requested = requested or os.environ.get("SHP_SANDBOX_BACKEND", "auto")
    if requested == "auto":
        return "docker" if docker_available() else "local"
    return requested


def _copy_code(src_root: Path, dst: Path, spec: PipelineSpec) -> None:
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache")
    for rel in spec.code:
        src = src_root / rel
        if src.is_dir():
            shutil.copytree(src, dst / rel, ignore=ignore)
        elif src.exists():
            (dst / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(src, dst / rel)


def _build_inputs(work: Path, incident_batch: Path | None, fixture_dir: Path | None,
                  live_sample: Path | None = None) -> list[dict]:
    inputs_dir = work / "inputs"
    inputs_dir.mkdir()
    items = []
    if incident_batch:
        shutil.copy(incident_batch, inputs_dir / "incident.json")
        items.append({"name": "incident", "raw": "inputs/incident.json", "expected": None})
    if live_sample and (live_sample / "raw.json").exists() and (live_sample / "expected.json").exists():
        shutil.copy(live_sample / "raw.json", inputs_dir / f"{LIVE_SAMPLE}.json")
        shutil.copy(live_sample / "expected.json", inputs_dir / f"{LIVE_SAMPLE}_expected.json")
        items.append({"name": LIVE_SAMPLE, "raw": f"inputs/{LIVE_SAMPLE}.json",
                      "expected": f"inputs/{LIVE_SAMPLE}_expected.json"})
    if not fixture_dir or not fixture_dir.exists():
        return items
    for d in sorted(p for p in fixture_dir.iterdir() if p.is_dir()):
        shutil.copy(d / "holdout.json", inputs_dir / f"{d.name}.json")
        shutil.copy(d / "holdout_expected.json", inputs_dir / f"{d.name}_expected.json")
        items.append({"name": d.name, "raw": f"inputs/{d.name}.json",
                      "expected": f"inputs/{d.name}_expected.json"})
    return items


def _run(work: Path, backend: str) -> None:
    if backend == "docker":
        cmd = ["docker", "run", "--rm", "--network", "none", "--memory", "1g", "--cpus", "1",
               "--pids-limit", "256", "--security-opt", "no-new-privileges",
               "-v", f"{work}:/work", "-w", "/work", IMAGE, "python", "entry.py"]
        subprocess.run(cmd, capture_output=True, text=True, timeout=TIMEOUT_S)
    else:
        env = {k: v for k, v in os.environ.items() if k in ("PATH", "SYSTEMROOT", "TEMP", "TMP", "HOME")}
        subprocess.run([sys.executable, "entry.py"], cwd=work, env=env,
                       capture_output=True, text=True, timeout=TIMEOUT_S)


def _summary(failures: list[str]) -> str:
    return "passed all checks" if not failures else "; ".join(failures)


def verify_patch(workdir: Path, files: dict[str, str], scenario_id: str | None = None,
                 incident_batch: Path | None = None, backend: str | None = None,
                 fixture_dir: Path | None = None, live_sample: Path | None = None,
                 keep: bool = False) -> dict:
    """fixture_dir defaults to the spec's holdout fixtures; live_sample is a directory
    with raw.json and expected.json (the stream processor keeps one per pipeline)."""
    t0 = time.time()
    backend = pick_backend(backend)
    workdir = Path(workdir)
    spec = load_spec(workdir)
    for p in files:
        check_allowed(p, spec)
    if fixture_dir is None:
        fixture_dir = spec.fixture_dir()

    work = Path(tempfile.mkdtemp(prefix="shp-sandbox-"))
    try:
        _copy_code(workdir, work / "base", spec)
        _copy_code(workdir, work / "patched", spec)
        for rel, content in files.items():
            target = work / "patched" / check_allowed(rel, spec)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        inputs = _build_inputs(work, incident_batch, fixture_dir, live_sample)
        (work / "manifest.json").write_text(json.dumps({"inputs": inputs, "spec": spec.sandbox_manifest()}))
        shutil.copy(HERE / "evaluate.py", work / "evaluate.py")
        shutil.copy(HERE / "entry.py", work / "entry.py")
        if backend == "docker":
            os.chmod(work, 0o777)   # container runs as an unprivileged user

        try:
            _run(work, backend)
            raw = json.loads((work / "result.json").read_text(encoding="utf-8"))
        except subprocess.TimeoutExpired:
            raw = {"error": f"sandbox timed out after {TIMEOUT_S}s"}
        except FileNotFoundError:
            raw = {"error": "sandbox produced no result (crashed before writing result.json)"}
        return _decide(raw, scenario_id, backend, time.time() - t0)
    finally:
        if not keep:
            shutil.rmtree(work, ignore_errors=True)


def _decide(raw: dict, scenario_id: str | None, backend: str, duration: float) -> dict:
    failures: list[str] = []
    if "error" in raw:
        return {"passed": False, "unit_tests": {}, "schema_checks": {}, "golden_diff_rows": -1,
                "regressions": [], "sandbox_logs": raw["error"], "duration_s": round(duration, 2),
                "backend": backend, "summary": raw["error"]}

    tests = raw["unit_tests"]
    if tests["exit_code"] != 0:
        failures.append(f"unit tests: {tests['failed']} failed\n{tests['output'][-1200:]}")

    if isinstance(raw["patched"], dict):   # evaluate.py crashed on the patched code
        failures.append(f"patched code crashed on import or run: {raw['patched'].get('output', '')[-800:]}")
        patched = {}
    else:
        patched = {r["name"]: r for r in raw["patched"]}
    base = {r["name"]: r for r in raw["base"]} if isinstance(raw["base"], list) else {}

    checks = {}
    inc = patched.get("incident")
    if inc:
        checks["incident"] = inc
        if not inc["ok"]:
            failures.append(f"incident batch still fails: {inc['error']} {inc.get('schema_failures') or ''}")
        elif inc["rows_in"] and inc["rows_out"] < (1 - MAX_DROP_FRACTION) * inc["rows_in"]:
            failures.append(f"incident batch: patch dropped {inc['rows_in'] - inc['rows_out']} of "
                            f"{inc['rows_in']} rows (limit {int(MAX_DROP_FRACTION * 100)}%)")

    golden = -1
    hold = patched.get(scenario_id) if scenario_id else None
    if hold:
        checks["holdout"] = hold
        golden = hold["diff_rows"] if hold["ok"] else -1
        if not hold["ok"]:
            failures.append(f"held-out batch fails: {hold['error']}")
        elif hold["diff_rows"]:
            failures.append(f"held-out batch: {hold['diff_rows']} rows differ from the expected output")

    regressions = []
    for name, b in base.items():
        if name in ("incident", scenario_id) or not (b["ok"] and b["diff_rows"] == 0):
            continue
        p = patched.get(name)
        if not p or not p["ok"] or p["diff_rows"] != 0:
            regressions.append(name)
    if regressions:
        failures.append(f"regressions on previously-passing fixtures: {regressions}")

    return {
        "passed": not failures,
        "unit_tests": {k: tests[k] for k in ("passed", "failed", "exit_code")},
        "schema_checks": checks,
        "golden_diff_rows": golden,
        "regressions": regressions,
        "sandbox_logs": tests["output"][-1500:],
        "duration_s": round(duration, 2),
        "backend": backend,
        "summary": _summary(failures),
    }
