"""Runs INSIDE the sandbox (container or local fallback), with /work laid out as:

    base/       original pipeline code + tests
    patched/    the same code with the candidate patch applied
    inputs/     fixture and incident batches
    manifest.json, evaluate.py, entry.py

Runs the unit tests against the patched code, then evaluates base and patched on
every input, and writes result.json. Pass/fail is decided on the host, not here.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time

WORK = os.path.dirname(os.path.abspath(__file__))
TIMEOUT = int(os.environ.get("SHP_STEP_TIMEOUT", "60"))


def _run(cmd: list[str], cwd: str) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=TIMEOUT)
        return p.returncode, p.stdout + p.stderr
    except subprocess.TimeoutExpired:
        return -1, f"timed out after {TIMEOUT}s"


def _tests_path() -> str | None:
    try:
        with open(os.path.join(WORK, "manifest.json"), encoding="utf-8") as f:
            spec = json.load(f).get("spec")
    except (OSError, ValueError):
        spec = None
    return spec.get("tests") if spec else "tests/pipeline"


def run_tests() -> dict:
    tests = _tests_path()
    if not tests:   # the spec has no tests: the data checks still decide
        return {"exit_code": 0, "passed": 0, "failed": 0, "skipped": True,
                "output": "no tests configured in healer.yaml"}
    code, out = _run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "--tb=line", "-rf",
                      tests], cwd=os.path.join(WORK, "patched"))
    counts = {k: int(n) for n, k in re.findall(r"(\d+) (passed|failed|error)", out)}
    return {"exit_code": code, "passed": counts.get("passed", 0),
            "failed": counts.get("failed", 0) + counts.get("error", 0),
            "output": out[-2000:]}


def run_eval(which: str) -> list[dict] | dict:
    code, out = _run([sys.executable, os.path.join(WORK, "evaluate.py"),
                      "--code", os.path.join(WORK, which),
                      "--manifest", os.path.join(WORK, "manifest.json")], cwd=WORK)
    for line in reversed(out.splitlines()):
        if line.startswith("@@RESULT@@"):
            return json.loads(line[len("@@RESULT@@"):])
    return {"crashed": True, "exit_code": code, "output": out[-2000:]}


if __name__ == "__main__":
    t0 = time.time()
    result = {"unit_tests": run_tests(), "base": run_eval("base"), "patched": run_eval("patched")}
    result["duration_s"] = round(time.time() - t0, 2)
    with open(os.path.join(WORK, "result.json"), "w", encoding="utf-8") as f:
        json.dump(result, f)
