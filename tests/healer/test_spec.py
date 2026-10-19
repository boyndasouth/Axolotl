"""healer.yaml: loading, defaults, env interpolation, and the safety rules that keep the
agent away from the contract and the tests whatever the configuration says."""

from __future__ import annotations

from pathlib import Path

import pytest

from healer.spec import SpecError, load_spec, load_specs, main
from sandbox.runner import PatchRejected, check_allowed
from tests.healer.custom_pipeline import make_repo

REPO = Path(__file__).resolve().parents[2]


def write_spec(root: Path, body: str) -> None:
    (root / "healer.yaml").write_text(body, encoding="utf-8")


def test_demo_spec_matches_what_was_hardcoded():
    spec = load_spec(REPO)
    assert spec.name == "orders" and spec.key == "order_id"
    assert spec.editable_files() == ["pipeline/mappings.yaml", "pipeline/transforms/orders.py"]
    assert spec.read_only_files() == ["pipeline/schemas.py", "tests/pipeline/test_orders.py"]
    assert (spec.source_topic, spec.dlq_topic) == ("orders", "orders.dlq")
    assert spec.columns() == ["order_id", "customer_id", "amount", "currency", "order_date", "status"]
    assert spec.check() == []


def test_no_file_falls_back_to_the_demo(tmp_path):
    spec = load_spec(tmp_path)
    assert spec.name == "orders" and not spec.from_file


def test_custom_pipeline_loads_and_checks(tmp_path):
    spec = load_spec(make_repo(tmp_path, git=False))
    assert spec.name == "events" and spec.key == "event_id"
    assert spec.editable_files() == ["etl/events/mappings.yaml", "etl/events/transform.py"]
    assert spec.read_only_files() == ["etl/contracts/events.py"]
    assert spec.protected == ("etl/contracts/events.py", "etl/contracts", "tests/events")
    assert not spec.demo_producer
    assert spec.column_types() == {"event_id": "string", "user_id": "string", "value": "float", "day": "string"}
    assert spec.check() == []


def test_patches_are_checked_against_the_spec(tmp_path):
    root = make_repo(tmp_path, git=False)
    assert check_allowed("./etl/events/transform.py", root) == "etl/events/transform.py"
    for bad in ("etl/contracts/events.py", "tests/events/test_transform.py", "../etl/events/x.py",
                "pipeline/transforms/orders.py", "healer.yaml"):
        with pytest.raises(PatchRejected):
            check_allowed(bad, root)


@pytest.mark.parametrize("pattern", ["etl/*", "etl/**", "*", "etl/contracts/*.py", "tests/*"])
def test_editable_may_never_reach_protected_files(tmp_path, pattern):
    root = make_repo(tmp_path, git=False)
    body = (root / "healer.yaml").read_text().replace(
        "editable: [etl/events/*.py, etl/events/mappings.yaml]", f'editable: ["{pattern}"]')
    write_spec(root, body)
    with pytest.raises(SpecError, match="protected"):
        load_specs(root)


def test_paths_must_stay_inside_the_repo(tmp_path):
    root = make_repo(tmp_path, git=False)
    body = (root / "healer.yaml").read_text().replace("tests: tests/events", "tests: ../elsewhere")
    write_spec(root, body)
    with pytest.raises(SpecError, match="inside the repo"):
        load_specs(root)


def test_env_interpolation_and_missing_vars(tmp_path, monkeypatch):
    root = make_repo(tmp_path, sink="postgres", dsn="${TEST_WAREHOUSE_DSN}", git=False)
    monkeypatch.delenv("TEST_WAREHOUSE_DSN", raising=False)
    with pytest.raises(SpecError, match="TEST_WAREHOUSE_DSN"):
        load_specs(root)
    monkeypatch.setenv("TEST_WAREHOUSE_DSN", "postgresql://u:p@h/db")
    assert load_specs(root)[0].sink["dsn"] == "postgresql://u:p@h/db"
    write_spec(root, (root / "healer.yaml").read_text().replace("${TEST_WAREHOUSE_DSN}", "${NOPE:-postgresql://d/x}"))
    assert load_specs(root)[0].sink["dsn"] == "postgresql://d/x"


def test_selecting_a_pipeline_by_name(tmp_path, monkeypatch):
    root = make_repo(tmp_path, git=False)
    body = (root / "healer.yaml").read_text()
    second = body.split("pipelines:\n", 1)[1].replace("name: events", "name: events2")
    write_spec(root, body + second)
    assert [s.name for s in load_specs(root)] == ["events", "events2"]
    monkeypatch.setenv("SHP_PIPELINE", "events2")
    assert load_spec(root).name == "events2"
    monkeypatch.setenv("SHP_PIPELINE", "nope")
    with pytest.raises(SpecError, match="no pipeline named"):
        load_spec(root)


def test_check_command(tmp_path, capsys):
    root = make_repo(tmp_path, git=False)
    assert main(["check", "--root", str(root)]) == 0
    out = capsys.readouterr().out
    assert "agent may edit:    etl/events/mappings.yaml, etl/events/transform.py" in out
    (root / "etl/events/transform.py").write_text("def transform(:\n")       # broken user code
    assert main(["check", "--root", str(root)]) == 1
    assert "transform failed to load" in capsys.readouterr().out


def test_unknown_or_misplaced_keys_are_errors(tmp_path):
    root = make_repo(tmp_path, git=False)
    body = (root / "healer.yaml").read_text()
    write_spec(root, body + "holdout:\n  live_sample: {rows: 5}\n")        # indented wrong: top level
    with pytest.raises(SpecError, match="top-level"):
        load_specs(root)
    write_spec(root, body.replace("key: event_id", "key: event_id\n    editible: [x]"))
    with pytest.raises(SpecError, match="editible"):
        load_specs(root)
