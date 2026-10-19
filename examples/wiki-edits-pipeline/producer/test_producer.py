"""The producer's parsing, shaping and drift switch (no network, no broker).

    python -m pytest producer
"""

from __future__ import annotations

import json
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import drifts  # noqa: E402
import wiki_producer as wp  # noqa: E402

SAMPLE = json.loads((HERE.parent / "tests" / "wiki" / "sample_events.json").read_text(encoding="utf-8"))


def test_parse_sse_handles_comments_ids_and_multiline_data():
    text = [":ok", "", "event: message", 'id: [{"offset":1}]', 'data: {"a":', 'data: 1}', "",
            "data: not json", "", 'id: [{"offset":2}]', 'data: {"b": 2}', ""]
    got = list(wp.parse_sse(text))
    assert got == [('[{"offset":1}]', {"a": 1}), ('[{"offset":2}]', {"b": 2})]


def test_prepare_stamps_the_key_trims_and_filters():
    e = dict(SAMPLE[0], log_params={"x": 1}, parsedcomment="<b>hi</b>")
    e.pop("event_id")
    out = wp.prepare(e, None)
    assert out["event_id"] == e["meta"]["id"] and "log_params" not in out and "parsedcomment" not in out
    assert wp.prepare(dict(e, meta={**e["meta"], "domain": "canary"}), None) is None
    assert wp.prepare(e, {"dewiki"}) is None and wp.prepare(e, {"enwiki"}) is not None


def test_every_drift_changes_the_shape_it_says():
    rng = random.Random(0)
    edit = SAMPLE[0]
    assert "performer" in drifts.apply("rename_user", edit, rng) and "user" not in drifts.apply("rename_user", edit, rng)
    assert drifts.apply("timestamp_ms", edit, rng)["timestamp"] == edit["timestamp"] * 1000
    assert drifts.apply("timestamp_iso", edit, rng)["timestamp"].endswith("Z")
    flat = drifts.apply("length_flattened", edit, rng)
    assert "length" not in flat and flat["length_old"] == edit["length"]["old"]
    assert drifts.apply("bot_as_string", edit, rng)["bot"] == "false"
    titles = [drifts.apply("missing_titles", edit, random.Random(i))["title"] for i in range(400)]
    assert 5 <= titles.count(None) <= 40
    assert drifts.apply(None, edit, rng) is edit
    assert edit["user"] == "Wikignome42"                      # the original event is untouched


def test_drift_switch_round_trip(tmp_path, monkeypatch):
    monkeypatch.setattr(drifts, "DRIFT_FILE", tmp_path / "d.json")
    assert drifts.current() is None
    drifts.set_drift("timestamp_ms")
    assert drifts.current() == "timestamp_ms"
    drifts.set_drift(None)
    assert drifts.current() is None


def test_replay_gives_each_pass_fresh_ids(tmp_path):
    rec = tmp_path / "r.jsonl"
    rec.write_text("\n".join(json.dumps(e) for e in SAMPLE[:2]))
    seen = []
    for e in wp.replay_events(rec, rate=10_000, stop=lambda: len(seen) >= 6):
        seen.append(wp.prepare(e, None)["event_id"])
    assert len(seen) == 6 and len(set(seen)) == 6
