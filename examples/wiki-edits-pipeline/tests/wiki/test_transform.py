"""Unit tests for the wiki transform. They also run inside the healer's sandbox against
every candidate patch, so they stay self-contained: only etl/ code and this folder.

They pin the ORIGINAL upstream format. A fix has to keep this working, because during
a migration upstream often sends both shapes for a while.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
EVENTS = json.loads((Path(__file__).parent / "sample_events.json").read_text(encoding="utf-8"))


def _load(rel: str, attr: str):
    spec = importlib.util.spec_from_file_location("t_" + rel.replace("/", "_")[:-3], ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, attr)


transform = _load("etl/wiki/transform.py", "transform")
Schema = _load("etl/contracts/wiki.py", "WikiChangesSchema")
MAPPINGS = yaml.safe_load((ROOT / "etl/wiki/mappings.yaml").read_text(encoding="utf-8"))


def test_original_format_meets_the_contract():
    df = transform(EVENTS, MAPPINGS)
    Schema.validate(df, lazy=True)
    assert len(df) == len(EVENTS)


def test_every_change_type_in_the_sample():
    df = transform(EVENTS, MAPPINGS).set_index("title")
    assert df.loc["Ada Lovelace", "size_delta"] == 26          # edit: new - old
    assert df.loc["Neuer Artikel", "size_delta"] == 1532       # new page: no old length
    assert df.loc["File:Sunset.jpg", "size_delta"] == 0        # log events carry no lengths
    assert df.loc["Q42", "is_bot"] and not df.loc["Paris", "is_bot"]
    assert df.loc["Ada Lovelace", "edited_at"] == "2026-10-18T18:21:07Z"


def test_empty_batch():
    assert list(transform([], MAPPINGS).columns) == list(Schema.columns)
