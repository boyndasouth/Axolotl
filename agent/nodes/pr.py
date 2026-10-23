"""Open a PR with the verified patch, or write it to reports/prs/ in dry-run mode."""

from __future__ import annotations

import time
from pathlib import Path

from agent.config import GITHUB_REPO, GITHUB_TOKEN, PR_MODE, REPORTS_DIR
from agent.nodes.report import escalation_body, pr_body
from agent.state import PipelineState


def _title(state: PipelineState) -> str:
    d = state.get("diagnosis") or {}
    what = (d.get("fix_plan") or state["anomaly"]["summary"]).split(". ")[0]
    return f"[self-heal] {d.get('category', 'fix')}: {what[:80]}"


def _dry_run(state: PipelineState, body: str) -> str:
    out = REPORTS_DIR / "prs" / state["incident_id"]
    out.mkdir(parents=True, exist_ok=True)
    (out / "PR.md").write_text(f"# {_title(state)}\n\n{body}\n", encoding="utf-8")
    (out / "patch.diff").write_text(state["patch"]["diff"], encoding="utf-8")
    return (out / "PR.md").resolve().as_uri()


def _github(state: PipelineState, body: str) -> str:
    from git import Repo
    from github import Auth, Github

    repo = Repo(state["workdir"])
    # Only the files the patch touches matter. The running stream rewrites tracked runtime files
    # (data/schema_snapshots/orders.json), so a whole-tree dirty check fails on a healthy setup.
    dirty = [p for p in state["patch"]["files"] if repo.is_dirty(untracked_files=False, path=p)]
    if dirty:
        raise RuntimeError(f"uncommitted local edits to {dirty}; refusing to branch over them")
    original = repo.active_branch.name
    branch = f"self-heal/{state['incident_id']}"
    repo.git.checkout("-b", branch)
    try:
        for rel, content in state["patch"]["files"].items():
            (Path(state["workdir"]) / rel).write_text(content, encoding="utf-8")
        repo.index.add(list(state["patch"]["files"]))
        repo.index.commit(f"{_title(state)}\n\nIncident {state['incident_id']}. Verified in sandbox.")
        repo.remote("origin").push(refspec=f"{branch}:{branch}")
    finally:
        repo.git.checkout(original)
    gh = Github(auth=Auth.Token(GITHUB_TOKEN)).get_repo(GITHUB_REPO)
    pr = gh.create_pull(title=_title(state), body=body, head=branch, base=gh.default_branch)
    pr.add_to_labels("self-heal")
    return pr.html_url


def open_pr(state: PipelineState) -> dict:
    body = pr_body(state)
    pr_error = None
    if PR_MODE == "github":
        try:
            url = _github(state, body)
        except Exception as e:
            # The fix is already verified. A GitHub problem (auth, remote, repo name, network) must not
            # strand it: write the PR locally and let the human approve it from the console instead.
            pr_error = f"{type(e).__name__}: {e}"[:500]
            url = _dry_run(state, f"> **Could not open a GitHub PR:** {pr_error}\n\n{body}")
    else:
        url = _dry_run(state, body)
    return {"pr_url": url, "pr_error": pr_error, "status": "pr_open",
            "timestamps": {"pr_opened": time.time()}}


def escalate(state: PipelineState) -> dict:
    from agent.config import CONFIDENCE_THRESHOLD
    d = state.get("diagnosis")
    if d and d.get("error"):
        reason = f"the diagnosis step failed, so nothing was attempted: {d['root_cause'][:300]}"
    elif d and not d["repairable"]:
        reason = f"diagnosis says this can't be fixed in code ({d['category']})"
    elif d and d["confidence"] < CONFIDENCE_THRESHOLD:
        reason = f"diagnosis confidence {d['confidence']:.2f} is below {CONFIDENCE_THRESHOLD}"
    else:
        reason = f"no patch passed verification after {state.get('attempts', 0)} attempts"
    state = {**state, "escalation_reason": reason}

    out = REPORTS_DIR / "escalations"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{state['incident_id']}.md").write_text(escalation_body(state), encoding="utf-8")
    if PR_MODE == "github":
        try:
            from github import Auth, Github
            gh = Github(auth=Auth.Token(GITHUB_TOKEN)).get_repo(GITHUB_REPO)
            gh.create_issue(title=f"[self-heal] escalation: {state['anomaly']['summary'][:80]}",
                            body=escalation_body(state), labels=["self-heal"])
        except Exception:
            pass   # the local report is the record of truth
    return {"status": "escalated", "escalation_reason": reason,
            "timestamps": {"escalated": time.time()}}
