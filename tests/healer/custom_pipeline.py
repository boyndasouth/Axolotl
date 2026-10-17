"""A small pipeline that is NOT the demo: different paths, key, topics, table and contract.
Used to prove the healer works from healer.yaml alone."""

from __future__ import annotations

import subprocess
import textwrap
from pathlib import Path

TRANSFORM = '''"""Raw events -> canonical events table."""
import pandas as pd

COLUMNS = ["event_id", "user_id", "value", "day"]


def transform(records, mappings):
    df = pd.DataFrame.from_records(records).rename(columns=mappings.get("rename", {}))
    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        raise KeyError(f"missing required columns after rename: {missing}")
    df = df[COLUMNS].copy()
    df["event_id"] = df["event_id"].astype(str)
    df["user_id"] = df["user_id"].astype(str)
    df["value"] = df["value"].astype(float)
    df["day"] = df["day"].astype(str)
    return df.reset_index(drop=True)
'''

MAPPINGS = "rename:\n  uid: user_id\n"

CONTRACT = '''import pandera.pandas as pa

EventsSchema = pa.DataFrameSchema(
    {
        "event_id": pa.Column(str, unique=True, nullable=False),
        "user_id": pa.Column(str, pa.Check.str_matches(r"^U\\d+$"), nullable=False),
        "value": pa.Column(float, pa.Check.ge(0), nullable=False),
        "day": pa.Column(str, pa.Check.str_matches(r"^\\d{4}-\\d{2}-\\d{2}$"), nullable=False),
    },
    strict=True, ordered=True,
)
'''

# Pins the original format and the contract, but not exact values: that is the live
# sample's job, which is the point of one of the tests.
UNIT_TEST = '''import importlib.util
import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]


def load(rel, attr):
    spec = importlib.util.spec_from_file_location(rel.replace("/", "_")[:-3], ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, attr)


def test_original_format_meets_the_contract():
    transform = load("etl/events/transform.py", "transform")
    schema = load("etl/contracts/events.py", "EventsSchema")
    mappings = yaml.safe_load((ROOT / "etl/events/mappings.yaml").read_text())
    df = transform([{"event_id": "E1", "uid": "U1", "value": 2.5, "day": "2026-10-01"}], mappings)
    schema.validate(df)
'''


def healer_yaml(sink: str = "duckdb", dsn: str | None = None, table: str = "events_clean") -> str:
    sink_block = (f"    sink: {{type: postgres, dsn: \"{dsn}\", table: {table}}}\n" if sink == "postgres"
                  else f"    sink: {{type: duckdb, path: events.duckdb, table: {table}}}\n")
    return textwrap.dedent("""\
        version: 1
        pipelines:
          - name: events
            transform: etl/events/transform.py:transform
            mappings: etl/events/mappings.yaml
            contract: etl/contracts/events.py:EventsSchema
            key: event_id
            editable: [etl/events/*.py, etl/events/mappings.yaml]
            read_only: [etl/contracts]
            tests: tests/events
            code: [etl, tests/events]
            source: {type: kafka, topic: events.raw}
            quarantine: {topic: events.raw.dlq}
        """) + sink_block + "    holdout:\n      live_sample: {rows: 200}\n    compare: {float_decimals: 4}\n"


def make_repo(root: Path, sink: str = "duckdb", dsn: str | None = None, table: str = "events_clean",
              git: bool = True) -> Path:
    files = {
        "etl/events/transform.py": TRANSFORM,
        "etl/events/mappings.yaml": MAPPINGS,
        "etl/contracts/events.py": CONTRACT,
        "tests/events/test_transform.py": UNIT_TEST,
        "healer.yaml": healer_yaml(sink, dsn, table),
    }
    for rel, content in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    if git:
        for c in (["git", "init", "-q"], ["git", "add", "."],
                  ["git", "-c", "user.name=t", "-c", "user.email=t@localhost", "commit", "-q", "-m", "events pipeline"]):
            subprocess.run(c, cwd=root, capture_output=True, check=False)
    return root


def events(n: int, start: int = 0, renamed: bool = False) -> list[dict]:
    """Clean upstream records; renamed=True is the drift: `uid` becomes `user`."""
    out = []
    for i in range(start, start + n):
        r = {"event_id": f"E{i}", "uid": f"U{i % 7}", "value": round(i * 1.25, 2), "day": f"2026-10-{1 + i % 28:02d}"}
        if renamed:
            r["user"] = r.pop("uid")
        out.append(r)
    return out
