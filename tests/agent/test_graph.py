"""End-to-end tests of the agent graph with a scripted fake LLM.

These exercise everything except the model itself: the real monitor, the real
sandbox (local backend), the retry loop, PR writing, escalation and the merge pause.
"""

from __future__ import annotations

import json
import os

import pytest
from langgraph.types import Command

from agent import llm
from agent.cli import run_incident
from agent.graph import build_graph
from bench.workdir import make_workdir
from chaos.inject import make_incident_batch

GOOD_MAPPING = ('orders:\n  rename:\n    cust_id: customer_id\n    client_id: customer_id\n'
                '  date_format: "%Y-%m-%d"\n')
BACKEND = os.environ.get("SHP_TEST_BACKEND", "local")   # CI sets "docker"
BREAKS_OLD_FORMAT = 'orders:\n  rename:\n    client_id: customer_id\n  date_format: "%Y-%m-%d"\n'


def diagnosis_json(repairable=True, confidence=0.9, category="upstream_schema_change"):
    return json.dumps({"root_cause": "cust_id was renamed to client_id upstream", "category": category,
                       "repairable": repairable, "confidence": confidence,
                       "evidence": ["schema diff: removed cust_id, added client_id"],
                       "target_files": ["pipeline/mappings.yaml"], "fix_plan": "map client_id"})


def patch_reply(mapping: str) -> str:
    return f"RATIONALE: map the new field name\n\n=== FILE: pipeline/mappings.yaml ===\n{mapping}=== END FILE ==="


class ScriptedLLM:
    def __init__(self, diagnosis: str, repairs: list[str]):
        self.diagnosis, self.repairs, self.prompts = diagnosis, list(repairs), []

    def __call__(self, role, system, user):
        self.prompts.append((role, user))
        return self.diagnosis if role == "diagnosis" else self.repairs.pop(0)


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    import agent.nodes.pr as pr
    monkeypatch.setattr(pr, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(pr, "PR_MODE", "dry-run")
    yield make_workdir(tmp_path / "wd")
    llm.set_fake_llm(None)


def _run(workdir, scenario, fake, wait=True):
    batch = workdir / "data" / "incoming" / "orders.json"
    make_incident_batch(scenario, out=batch, seed=3000)
    llm.set_fake_llm(fake)
    graph = build_graph(wait_for_merge=wait)
    final = run_incident(graph, f"file:{batch}", scenario, BACKEND, workdir=str(workdir), verbose=False)
    return graph, final


def test_retry_then_pr_then_merge(workdir, tmp_path):
    fake = ScriptedLLM(diagnosis_json(), [patch_reply(BREAKS_OLD_FORMAT), patch_reply(GOOD_MAPPING)])
    graph, final = _run(workdir, "rename_column", fake)

    assert final["status"] == "pr_open"
    assert final["attempts"] == 2
    assert not final["history"][0]["verification"]["passed"]
    assert final["history"][1]["verification"]["golden_diff_rows"] == 0
    # the second repair prompt carried the verifier's feedback from the first
    assert "FAILED VERIFICATION" in fake.prompts[-1][1]
    assert (tmp_path / "reports" / "prs" / final["incident_id"] / "PR.md").exists()

    cfg = {"configurable": {"thread_id": final["incident_id"]}}
    graph.invoke(Command(resume={"merged": True}), cfg)
    done = graph.get_state(cfg).values
    assert done["status"] == "merged"
    assert {"detected", "diagnosed", "verified", "pr_opened", "resolved"} <= set(done["timestamps"])


def test_outage_escalates_without_repair(workdir):
    fake = ScriptedLLM(diagnosis_json(repairable=False, category="upstream_outage"), [])
    _, final = _run(workdir, "upstream_outage", fake)
    assert final["anomaly"]["kind"] == "row_count"
    assert final["status"] == "escalated"
    assert final.get("attempts", 0) == 0


def test_additive_field_is_not_an_incident(workdir):
    fake = ScriptedLLM(diagnosis_json(), [])
    _, final = _run(workdir, "additive_field", fake)
    assert final["status"] == "healthy"
    assert fake.prompts == []


def test_gives_up_after_max_attempts(workdir):
    fake = ScriptedLLM(diagnosis_json(), [patch_reply(BREAKS_OLD_FORMAT)] * 3)
    _, final = _run(workdir, "rename_column", fake)
    assert final["status"] == "escalated"
    assert final["attempts"] == 3
    assert "no patch passed" in final["escalation_reason"]


def test_patch_to_contract_is_rejected(workdir):
    sneaky = "RATIONALE: relax it\n\n=== FILE: pipeline/schemas.py ===\nOrdersSchema = None\n=== END FILE ==="
    fake = ScriptedLLM(diagnosis_json(), [sneaky, patch_reply(GOOD_MAPPING)])
    _, final = _run(workdir, "rename_column", fake, wait=False)
    assert "not allowed" in final["history"][0]["verification"]["summary"] or \
           "allowed" in final["history"][0]["verification"]["summary"]
    assert final["status"] == "pr_open"


def test_low_confidence_escalates(workdir):
    fake = ScriptedLLM(diagnosis_json(confidence=0.3), [])
    _, final = _run(workdir, "rename_column", fake)
    assert final["status"] == "escalated"
    assert "confidence" in final["escalation_reason"]
