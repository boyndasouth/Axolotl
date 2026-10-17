"""Sinks: idempotent upserts on the key, tables built from the contract, and no data
loss when the warehouse goes away. Postgres tests run when SHP_TEST_POSTGRES_DSN is set."""

from __future__ import annotations

import os
import time
import uuid

import pandas as pd
import pytest

from healer.spec import load_spec
from stream.bus import MemoryBus
from stream.metrics import Metrics
from stream.processor import StreamProcessor
from stream.sinks import DuckDBSink, PostgresSink, SinkUnavailable, _redact
from tests.healer.custom_pipeline import events, make_repo

PG_DSN = os.environ.get("SHP_TEST_POSTGRES_DSN")
COLS = ["event_id", "user_id", "value", "day"]
TYPES = {"event_id": "string", "user_id": "string", "value": "float", "day": "string"}


def frame(ids, value=1.5):
    return pd.DataFrame({"event_id": [f"E{i}" for i in ids], "user_id": ["U1"] * len(ids),
                         "value": [value] * len(ids), "day": ["2026-10-01"] * len(ids)})


def test_duckdb_sink_upserts_on_the_key(tmp_path):
    s = DuckDBSink(tmp_path / "w.duckdb", "analytics.events", COLS, TYPES, "event_id")
    assert s.write(frame(range(5))) == (5, 0)
    assert s.write(frame(range(3, 8), value=9.0)) == (3, 2)     # resent keys are ignored, not overwritten
    assert s.count() == 8
    assert {r["event_id"]: r["value"] for r in s.recent(20)}["E3"] == 1.5
    assert s.write(None) == (0, 0) and s.write(frame([])) == (0, 0)
    s.close()


def test_key_must_be_a_contract_column(tmp_path):
    with pytest.raises(ValueError, match="key"):
        DuckDBSink(tmp_path / "w.duckdb", "t", COLS, TYPES, "nope")


@pytest.mark.skipif(not PG_DSN, reason="set SHP_TEST_POSTGRES_DSN to run against Postgres")
def test_postgres_sink_upserts_on_the_key():
    import psycopg
    schema = f"healer_test_{uuid.uuid4().hex[:8]}"
    s = PostgresSink(PG_DSN, f"{schema}.events", COLS, TYPES, "event_id")
    try:
        assert s.write(frame(range(5))) == (5, 0)
        assert s.write(frame(range(3, 8), value=9.0)) == (3, 2)
        assert s.count() == 8
        assert {r["event_id"]: r["value"] for r in s.recent(20)}["E3"] == 1.5
        again = PostgresSink(PG_DSN, f"{schema}.events", COLS, TYPES, "event_id")   # existing table is reused
        assert again.count() == 8
        again.close()
        with psycopg.connect(PG_DSN) as con:
            types = dict(con.execute("SELECT column_name, data_type FROM information_schema.columns "
                                     "WHERE table_schema = %s AND table_name = 'events'", [schema]).fetchall())
        assert types["value"] == "double precision" and types["loaded_at"] == "timestamp with time zone"
    finally:
        s.close()
        with psycopg.connect(PG_DSN, autocommit=True) as con:
            con.execute(f"DROP SCHEMA {schema} CASCADE")


@pytest.mark.skipif(not PG_DSN, reason="set SHP_TEST_POSTGRES_DSN to run against Postgres")
def test_unreachable_postgres_is_reported_without_the_password():
    with pytest.raises(SinkUnavailable) as e:
        PostgresSink("postgresql://u:hunter2@127.0.0.1:1/db", "t", COLS, TYPES, "event_id", connect_timeout=2)
    assert "hunter2" not in str(e.value)


def test_redact_hides_passwords():
    assert "s3cret" not in _redact(Exception("could not connect to postgresql://bob:s3cret@db:5432/x"))


class FlakySink:
    """Wraps a real sink and fails its first `fail` writes, like a warehouse restarting."""

    def __init__(self, inner, fail: int):
        self.inner, self.fail, self.attempts = inner, fail, 0

    def write(self, df):
        self.attempts += 1
        if self.fail > 0:
            self.fail -= 1
            raise SinkUnavailable("postgres unreachable: connection refused")
        return self.inner.write(df)

    def __getattr__(self, name):
        return getattr(self.inner, name)


def test_processor_waits_out_a_sink_outage_without_losing_rows(tmp_path):
    root = make_repo(tmp_path / "repo", git=False)
    spec = load_spec(root)
    bus = MemoryBus()
    bus.ensure_topics([spec.source_topic, spec.dlq_topic])
    seen = []
    proc = StreamProcessor(bus, root, Metrics(), data_dir=tmp_path / "data",
                           on_event=lambda kind, data: seen.append(kind))
    proc.sink = FlakySink(proc.sink, fail=3)
    proc.start()
    try:
        bus.publish_many(spec.source_topic, events(30), key_field="event_id")
        end = time.time() + 15
        while proc.sink.count() < 30 and time.time() < end:
            time.sleep(0.1)
        assert proc.sink.count() == 30 and proc.dlq_depth() == 0
        assert proc.sink.attempts >= 4
        assert seen.count("sink_unavailable") == 1 and "sink_recovered" in seen
        assert proc._consumer.committed() == 30
    finally:
        proc.stop()
