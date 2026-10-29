"""open_pr must never strand a verified fix: GitHub problems fall back to a local PR."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from agent.nodes import pr


def _state(workdir: Path) -> dict:
    return {
        "incident_id": "inc-test", "workdir": str(workdir),
        "anomaly": {"summary": "transform failed", "kind": "schema_drift", "report": {}},
        "diagnosis": {"fix_plan": "map client_id to customer_id", "category": "upstream_schema_change",
                      "root_cause": "rename", "confidence": 0.95, "evidence": []},
        "patch": {"files": {"pipeline/mappings.yaml": "a: b\n"}, "diff": "--- a\n+++ b\n", "rationale": "r"},
        "verification": {"unit_tests": {"passed": 1, "failed": 0}, "schema_checks": {}, "golden_diff_rows": 0,
                         "regressions": [], "backend": "local", "duration_s": 1.0, "summary": "passed all checks"},
        "history": [], "timestamps": {},
    }


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    def git(*a):
        subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *a], cwd=tmp_path, check=True,
                       capture_output=True)
    (tmp_path / "pipeline").mkdir()
    (tmp_path / "pipeline" / "mappings.yaml").write_text("x: y\n")
    (tmp_path / "data" / "schema_snapshots").mkdir(parents=True)
    (tmp_path / "data" / "schema_snapshots" / "orders.json").write_text("{}")
    git("init", "-q", "-b", "main")
    git("add", ".")
    git("commit", "-qm", "init")
    return tmp_path


def test_github_failure_falls_back_to_local_pr(repo, tmp_path, monkeypatch):
    monkeypatch.setattr(pr, "PR_MODE", "github")
    monkeypatch.setattr(pr, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(pr, "_github", lambda s, b: (_ for _ in ()).throw(RuntimeError("bad token")))
    out = pr.open_pr(_state(repo))
    assert out["status"] == "pr_open"
    assert "bad token" in out["pr_error"]
    assert (tmp_path / "reports" / "prs" / "inc-test" / "PR.md").exists()


def test_runtime_files_do_not_block_branching(repo):
    # the stream rewrites the tracked schema snapshot; that must not count as "dirty"
    (repo / "data" / "schema_snapshots" / "orders.json").write_text('{"changed": 1}')
    state = _state(repo)
    with pytest.raises(Exception) as e:      # no 'origin' remote here, so it fails later, at push
        pr._github(state, "body")
    assert "refusing to branch" not in str(e.value)


def test_local_edits_to_patched_files_still_block(repo):
    (repo / "pipeline" / "mappings.yaml").write_text("hand edit\n")
    with pytest.raises(RuntimeError, match="refusing to branch"):
        pr._github(_state(repo), "body")
