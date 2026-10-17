"""Throwaway working copies of the pipeline, so injected failures and patches never
touch your real checkout."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
_IGNORE = shutil.ignore_patterns("__pycache__", "*.pyc")


def make_workdir(dst: Path, repo_root: Path = REPO_ROOT, git: bool = True) -> Path:
    dst.mkdir(parents=True, exist_ok=True)
    shutil.copytree(repo_root / "pipeline", dst / "pipeline", ignore=_IGNORE)
    shutil.copytree(repo_root / "tests" / "pipeline", dst / "tests" / "pipeline", ignore=_IGNORE)
    if (repo_root / "healer.yaml").exists():
        shutil.copy(repo_root / "healer.yaml", dst / "healer.yaml")
    if (repo_root / "chaos" / "fixtures").exists():    # the spec's holdout fixtures, relative to the workdir
        shutil.copytree(repo_root / "chaos" / "fixtures", dst / "chaos" / "fixtures", ignore=_IGNORE)
    snap = repo_root / "data" / "schema_snapshots"
    if snap.exists():
        shutil.copytree(snap, dst / "data" / "schema_snapshots")
    if git:
        # a one-commit history, so the diagnosis agent's git tool has something to read
        cmds = [["git", "init", "-q"], ["git", "add", "pipeline", "tests"],
                ["git", "-c", "user.name=self-heal-bench", "-c", "user.email=bench@localhost",
                 "commit", "-q", "-m", "baseline: orders pipeline"]]
        for c in cmds:
            subprocess.run(c, cwd=dst, capture_output=True, check=False)
    return dst
