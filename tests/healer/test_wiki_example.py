"""Keeps examples/wiki-edits-pipeline working: its healer.yaml is valid, the agent may only
touch its transform, and its own tests (transform + producer) pass."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from healer.spec import load_specs

WIKI = Path(__file__).resolve().parents[2] / "examples" / "wiki-edits-pipeline"


def test_wiki_example_spec(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_DSN", "postgresql://u:p@localhost:5433/warehouse")
    (spec,) = load_specs(WIKI)
    assert spec.name == "wiki_edits" and spec.key == "event_id" and not spec.demo_producer
    assert spec.editable_files() == ["etl/wiki/mappings.yaml", "etl/wiki/transform.py"]
    assert not any(p.startswith("producer") for p in spec.code)      # the agent never sees the upstream
    assert spec.check() == []


def test_wiki_example_own_tests():
    r = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "--rootdir", str(WIKI),
                        "tests/wiki", "producer"], cwd=WIKI, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stdout[-2000:] + r.stderr[-1000:]


# The original, unhealed transform. The example's own copy changes as heals are merged,
# so this test keeps a frozen one (like chaos/baseline does for the demo).
TRANSFORM = (Path(__file__).parent / "wiki_baseline" / "transform.py").read_text(encoding="utf-8")
# the repair missing_titles calls for: a record with no title can't be fixed, so drop it
DROPS_UNTITLED = TRANSFORM.replace(
    "        r = {rename.get(k, k): v for k, v in raw.items()}\n",
    "        r = {rename.get(k, k): v for k, v in raw.items()}\n"
    "        if not r.get(\"title\"):\n"
    "            continue\n")


def test_missing_titles_is_repaired_by_dropping_the_untitled(monkeypatch, tmp_path):
    """missing_titles expects a repair, not an escalation: dropping the few untitled
    records passes the verifier, and leaving the code alone doesn't."""
    from sandbox.runner import verify_patch
    monkeypatch.setenv("WAREHOUSE_DSN", "postgresql://u:p@localhost:5433/warehouse")
    assert DROPS_UNTITLED != TRANSFORM
    sample = json.loads((WIKI / "tests" / "wiki" / "sample_events.json").read_text(encoding="utf-8"))
    events = [dict(sample[i % len(sample)], event_id=f"mt-{i}") for i in range(40)]
    for e in events[:2]:                                   # 5%, as producer/drifts.py does
        e["title"] = None
    batch = tmp_path / "incident.json"
    batch.write_text(json.dumps(events))

    backend = os.environ.get("SHP_TEST_BACKEND", "local")
    ok = verify_patch(WIKI, {"etl/wiki/transform.py": DROPS_UNTITLED}, incident_batch=batch, backend=backend)
    assert ok["passed"], ok["summary"]
    unchanged = verify_patch(WIKI, {"etl/wiki/transform.py": TRANSFORM}, incident_batch=batch, backend=backend)
    assert not unchanged["passed"] and "incident batch still fails" in unchanged["summary"]
