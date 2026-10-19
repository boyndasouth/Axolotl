"""The healer on a pipeline that isn't the demo, configured only by healer.yaml:
its own topics, key, contract, table and files. The whole live loop runs, and the
live sample (real records + the rows they produced) catches a fix that passes the
unit tests but changes rows that were already right.

Set SHP_TEST_POSTGRES_DSN to also run it against Postgres."""

from __future__ import annotations

import json
import os
import time
import uuid

import pytest
from langgraph.checkpoint.memory import MemorySaver

from agent import llm
from chaos.control import MemoryDrift
from sandbox.runner import verify_patch
from stream.bus import MemoryBus
from stream.runtime import TERMINAL, Runtime
from tests.healer.custom_pipeline import MAPPINGS, TRANSFORM, events, make_repo

BACKEND = os.environ.get("SHP_TEST_BACKEND", "local")
PG_DSN = os.environ.get("SHP_TEST_POSTGRES_DSN")

FIXED_MAPPINGS = "rename:\n  uid: user_id\n"
FIXED_TRANSFORM = TRANSFORM.replace(
    '    df = pd.DataFrame.from_records(records).rename(columns=mappings.get("rename", {}))',
    '''    records = [dict(r, uid=r["user"]) if "user" in r and "uid" not in r else r for r in records]
    records = [{k: v for k, v in r.items() if k != "user"} for r in records]
    df = pd.DataFrame.from_records(records).rename(columns=mappings.get("rename", {}))''')
# handles the new name too, passes the unit tests and the contract... but inflates every value
SNEAKY_TRANSFORM = FIXED_TRANSFORM.replace('df["value"].astype(float)', 'df["value"].astype(float) * 1.1')


def file_block(path, body):
    return f"=== FILE: {path} ===\n{body}=== END FILE ==="


def diagnosis():
    return json.dumps({"root_cause": "upstream renamed uid to user", "category": "upstream_schema_change",
                       "repairable": True, "confidence": 0.9, "evidence": ["schema diff: -uid +user"],
                       "target_files": ["etl/events/transform.py"], "fix_plan": "accept both names"})


class ScriptedLLM:
    def __init__(self, repairs):
        self.repairs, self.calls = list(repairs), []

    def __call__(self, role, system, user):
        self.calls.append((role, system, user))
        return diagnosis() if role == "diagnosis" else "RATIONALE: accept both\n\n" + self.repairs.pop(0)


def wait(pred, timeout=60.0):
    end = time.time() + timeout
    while time.time() < end:
        v = pred()
        if v:
            return v
        time.sleep(0.1)
    raise AssertionError("timed out")


SINKS = ["duckdb"] + (["postgres"] if PG_DSN else [])


@pytest.fixture(params=SINKS)
def repo(request, tmp_path, monkeypatch):
    import agent.nodes.pr as pr
    monkeypatch.setattr(pr, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(pr, "PR_MODE", "dry-run")
    table = f"healer_test_{uuid.uuid4().hex[:8]}.events_clean"
    root = make_repo(tmp_path / "repo", sink=request.param, dsn=PG_DSN, table=table)
    yield root
    llm.set_fake_llm(None)
    if request.param == "postgres":
        import psycopg
        with psycopg.connect(PG_DSN, autocommit=True) as con:
            con.execute(f"DROP SCHEMA IF EXISTS {table.split('.')[0]} CASCADE")


def test_live_loop_on_a_custom_pipeline(repo):
    fake = ScriptedLLM([
        file_block("etl/contracts/events.py", "EventsSchema = None\n"),        # 1: tries to loosen the contract
        file_block("etl/events/transform.py", SNEAKY_TRANSFORM),               # 2: passes tests, alters valid rows
        file_block("etl/events/transform.py", FIXED_TRANSFORM),                # 3: correct
    ])
    llm.set_fake_llm(fake)
    bus = MemoryBus()
    rt = Runtime(workdir=repo, bus=bus, drift=MemoryDrift(), data_dir=repo / "data",
                 sandbox_backend=BACKEND, checkpointer=MemorySaver(), live_check_s=1.0).start()
    try:
        assert rt.producer is None                     # healer.yaml has no demo_producer: nothing fake is sent
        assert set(rt.status()["topics"]) == {"events.raw", "events.raw.dlq"}

        for i in range(4):                             # clean traffic: loads and builds the live sample
            bus.publish_many("events.raw", events(20, start=i * 20), key_field="event_id")
        wait(lambda: rt.processor.sink.count() == 80)
        assert len(rt.processor.live_sample) == 80

        bus.publish_many("events.raw", events(30, start=1000, renamed=True), key_field="event_id")
        inc = wait(lambda: next((i for i in rt.store.incidents() if i["status"] in {"pr_open"} | TERMINAL), None),
                   timeout=120)
        assert inc["status"] == "pr_open", inc
        assert rt.processor.dlq_depth() == 30 and rt.processor.sink.count() == 80

        v = rt.graph.get_state({"configurable": {"thread_id": inc["id"]}}).values
        h = v["history"]
        assert len(h) == 3
        assert "allowed" in h[0]["verification"]["summary"]                   # contract edit refused
        assert h[1]["verification"]["unit_tests"]["failed"] == 0              # the tests didn't catch it...
        assert h[1]["verification"]["regressions"] == ["live_sample"]         # ...the live sample did
        assert h[2]["verification"]["passed"]

        # the prompts were rendered for this pipeline, not the demo
        system = next(s for role, s, _ in fake.calls if role == "diagnosis")
        assert "etl/contracts/events.py" in system and "pipeline/schemas.py" not in system
        assert "- etl/events/transform.py" in system

        rt.decide(inc["id"], merged=True)
        inc = wait(lambda: next((i for i in rt.store.incidents() if i["status"] in TERMINAL), None), timeout=60)
        assert inc["status"] == "resolved", inc
        assert rt.processor.sink.count() == 110 and rt.processor.dlq_depth() == 0
        rows = {r["event_id"]: r for r in rt.processor.sink.recent(200)}
        assert rows["E10"]["value"] == pytest.approx(12.5)                    # untouched by the fix
        assert rows["E1000"]["user_id"] == "U6"                                # recovered from quarantine
    finally:
        rt.stop()


def test_sandbox_uses_the_live_sample_directly(tmp_path):
    """verify_patch on its own: no fixtures configured, only the live sample."""
    root = make_repo(tmp_path / "repo", git=False)
    from stream.processor import LiveCode, LiveSample, process_records
    code = LiveCode(root)
    sample = LiveSample(tmp_path / "live", "event_id", rows=50)
    batch = events(40)
    sample.add(batch, process_records(batch, code).good)
    assert len(sample) == 40
    live = sample.save()
    incident = tmp_path / "incident.json"
    incident.write_text(json.dumps(events(10, start=500, renamed=True)))

    ok = verify_patch(root, {"etl/events/transform.py": FIXED_TRANSFORM}, incident_batch=incident,
                      live_sample=live, backend=BACKEND)
    assert ok["passed"], ok["summary"]
    bad = verify_patch(root, {"etl/events/transform.py": SNEAKY_TRANSFORM}, incident_batch=incident,
                       live_sample=live, backend=BACKEND)
    assert not bad["passed"] and bad["regressions"] == ["live_sample"]
    no_fix = verify_patch(root, {"etl/events/mappings.yaml": MAPPINGS}, incident_batch=incident,
                          live_sample=live, backend=BACKEND)
    assert not no_fix["passed"] and "incident batch still fails" in no_fix["summary"]


def test_live_sample_is_a_bounded_reservoir_that_survives_restarts(tmp_path):
    root = make_repo(tmp_path / "repo", git=False)
    from stream.processor import LiveCode, LiveSample, process_records
    code = LiveCode(root)
    s = LiveSample(tmp_path / "live", "event_id", rows=25, seed=1)
    for i in range(10):
        b = events(20, start=i * 20)
        s.add(b, process_records(b, code).good)
    assert len(s) == 25 and s.seen == 200
    keys = [r["event_id"] for r in s.raw]
    resent = [r for r in events(200) if r["event_id"] in set(keys)]     # upstream resends what we hold
    assert s.add(resent, process_records(resent, code).good) == 0 and s.seen == 200
    assert len(set(keys)) == 25 and any(int(k[1:]) >= 100 for k in keys)   # not just the first rows
    assert [r["event_id"] for r in s.expected] == keys                    # raw and expected stay aligned
    s.save()
    again = LiveSample(tmp_path / "live", "event_id", rows=25)
    assert again.raw == s.raw and again.seen == 200
