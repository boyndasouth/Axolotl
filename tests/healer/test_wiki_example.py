"""Keeps examples/wiki-edits-pipeline working: its healer.yaml is valid, the agent may only
touch its transform, and its own tests (transform + producer) pass."""

from __future__ import annotations

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
