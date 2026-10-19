"""The live loop end to end, with a scripted LLM and the local sandbox: drift injected
mid-stream -> detected -> diagnosed -> fixed and verified -> PR -> human decision ->
fix deployed -> DLQ replayed -> live check. Plus the control-plane API on top of it."""

from __future__ import annotations

import os
import time

import pytest
from langgraph.checkpoint.memory import MemorySaver

from agent import llm
from bench.oracle import OracleLLM
from bench.workdir import make_workdir
from chaos.control import MemoryDrift
from stream.bus import TOPICS, MemoryBus
from stream.runtime import TERMINAL, Runtime

BACKEND = os.environ.get("SHP_TEST_BACKEND", "local")


def wait_status(rt, want: set[str], timeout=120.0):
    end = time.time() + timeout
    while time.time() < end:
        incs = rt.store.incidents()
        if incs and incs[0]["status"] in want:
            return incs[0]
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {want}; have {rt.store.incidents()[:1]}")


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    import agent.nodes.pr as pr
    monkeypatch.setattr(pr, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(pr, "PR_MODE", "dry-run")
    llm.set_fake_llm(OracleLLM())
    bus = MemoryBus()
    bus.ensure_topics(TOPICS)
    events = []
    rt = Runtime(workdir=make_workdir(tmp_path / "wd"), bus=bus, drift=MemoryDrift(),
                 data_dir=tmp_path / "wd" / "data", sandbox_backend=BACKEND,
                 checkpointer=MemorySaver(), publish=events.append, live_check_s=1.5).start()
    rt.events = events
    end = time.time() + 15
    while rt.processor.sink.count() < 25 and time.time() < end:
        time.sleep(0.1)
    yield rt
    rt.stop()
    llm.set_fake_llm(None)


def test_drift_is_fixed_deployed_and_the_quarantine_replayed(runtime):
    runtime.set_drift("rename_column")
    inc = wait_status(runtime, {"pr_open"} | TERMINAL)
    assert inc["status"] == "pr_open", inc
    assert inc["scenario"] == "rename_column"
    assert runtime.busy()                                  # no new incident while one waits for review

    runtime.decide(inc["id"], merged=True)
    inc = wait_status(runtime, TERMINAL)
    assert inc["status"] == "resolved"

    v = runtime.graph.get_state({"configurable": {"thread_id": inc["id"]}}).values
    r = v["rollout"]
    assert r["healthy"] and r["replay"]["replayed"] > 0 and r["replay"]["still_failing"] == 0
    assert r["live_check"]["quarantined"] == 0
    assert {"first_failure", "detected", "diagnosed", "verified", "pr_opened", "deployed", "recovered"} \
        <= set(v["timestamps"])
    assert runtime.processor.code.version == 2
    kinds = [e["kind"] for e in runtime.events if e.get("incident_id") == inc["id"]]
    for k in ("incident_opened", "awaiting_decision", "decision", "fix_applied", "dlq_replayed", "live_check"):
        assert k in kinds
    # the merged fix was committed to the checkout, so the next diagnosis sees it in git history
    import subprocess
    log = subprocess.run(["git", "log", "--oneline"], cwd=runtime.workdir, capture_output=True, text=True).stdout
    assert "self-heal:" in log


def test_rejected_fix_leaves_the_quarantine_alone(runtime):
    runtime.set_drift("status_case")
    inc = wait_status(runtime, {"pr_open"} | TERMINAL)
    depth = runtime.processor.dlq_depth()
    runtime.decide(inc["id"], merged=False)
    inc = wait_status(runtime, TERMINAL)
    assert inc["status"] == "rejected"
    assert runtime.processor.code.version == 1 and runtime.processor.dlq_depth() >= depth
    with pytest.raises(ValueError):
        runtime.decide(inc["id"], merged=True)            # not waiting any more


def test_upstream_silence_escalates(runtime):
    runtime.detector.silence_s = 3
    runtime.set_drift("upstream_outage")
    inc = wait_status(runtime, TERMINAL, timeout=30)
    assert inc["status"] == "escalated" and "silent" in inc["summary"]


def test_harmless_new_field_raises_nothing(runtime):
    runtime.set_drift("additive_field")
    time.sleep(4)
    assert runtime.store.incidents() == []
    assert runtime.metrics.totals["dlq"] == 0


def test_control_plane_api(tmp_path, monkeypatch):
    """The server, on a throwaway checkout: status, chaos, incident detail, decision."""
    from fastapi.testclient import TestClient

    import agent.nodes.pr as pr
    monkeypatch.setattr(pr, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(pr, "PR_MODE", "dry-run")
    wd = make_workdir(tmp_path / "wd")
    monkeypatch.setenv("SHP_WORKDIR", str(wd))
    monkeypatch.setenv("SHP_BUS", "memory")
    monkeypatch.setenv("SHP_SANDBOX_BACKEND", BACKEND)
    monkeypatch.setenv("SHP_FAKE_LLM", "oracle")
    import stream.server as server
    monkeypatch.setattr(server, "FAKE_LLM", "oracle")
    from chaos import control
    monkeypatch.setattr(control, "DRIFT_FILE", tmp_path / "drift.json")
    monkeypatch.setattr(control.FileDrift.__init__, "__defaults__", (tmp_path / "drift.json",))

    with TestClient(server.app) as c:
        s = c.get("/api/status").json()
        assert s["bus"] == "memory" and s["models"]["repair"].startswith("scripted")
        assert len(c.get("/api/chaos").json()["scenarios"]) == 9
        assert c.post("/api/chaos", json={"scenario": "nope"}).status_code == 404
        assert c.post("/api/chaos", json={"scenario": "date_format"}).json()["scenario"] == "date_format"

        end, inc = time.time() + 120, None
        while time.time() < end:
            rows = c.get("/api/incidents").json()
            if rows and rows[0]["status"] == "pr_open":
                inc = rows[0]
                break
            time.sleep(0.3)
        assert inc, "no incident reached review"
        detail = c.get(f"/api/incidents/{inc['id']}").json()
        assert detail["waiting_for_decision"] and detail["attempts"][-1]["verification"]["passed"]
        assert detail["pr_body"] and "Verification" in detail["pr_body"]
        assert c.post(f"/api/incidents/{inc['id']}/decision", json={"merged": True}).status_code == 200
        assert c.post(f"/api/incidents/{inc['id']}/decision", json={"merged": True}).status_code == 409

        end = time.time() + 60
        while time.time() < end and c.get(f"/api/incidents/{inc['id']}").json()["status"] != "resolved":
            time.sleep(0.3)
        assert c.get(f"/api/incidents/{inc['id']}").json()["rollout"]["healthy"]
        assert c.delete("/api/chaos").json()["scenario"] is None
        assert len(c.get("/api/metrics?seconds=60").json()) == 60
