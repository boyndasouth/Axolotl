"""The streaming pipeline: bus, processor (DLQ routing, idempotent sink, hot reload,
replay), detector (one incident per root cause, re-arming, silence)."""

from __future__ import annotations

import time

import pytest

from bench.oracle import ROBUST_MAPPING, ROBUST_TRANSFORM
from bench.workdir import make_workdir
from chaos.catalog import load_scenario
from chaos.drifts import apply_drift
from chaos.generator import generate_orders
from stream.bus import TOPIC_DLQ, TOPIC_ORDERS, TOPICS, MemoryBus
from stream.detector import SILENCE, Detector
from stream.metrics import Metrics
from stream.processor import LiveCode, StreamProcessor, normalize_message, process_records


def drifted(scenario: str, n: int = 25, seed: int = 11) -> list[dict]:
    sc = load_scenario(scenario)
    return apply_drift(generate_orders(n=n, seed=seed), sc["drift"], sc.get("params"), seed=seed)


@pytest.fixture
def wd(tmp_path):
    return make_workdir(tmp_path / "wd", git=False)


@pytest.fixture
def bus():
    b = MemoryBus()
    b.ensure_topics(TOPICS)
    yield b
    b.close()


def apply_fix(wd):
    (wd / "pipeline" / "transforms" / "orders.py").write_text(ROBUST_TRANSFORM, encoding="utf-8")
    (wd / "pipeline" / "mappings.yaml").write_text(ROBUST_MAPPING, encoding="utf-8")


def wait_for(pred, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.05)
    return False


# ---- bus ------------------------------------------------------------------------------

def test_memory_bus_resumes_from_committed_offset(bus):
    for i in range(5):
        bus.publish(TOPIC_ORDERS, {"i": i})
    c = bus.consumer("g", TOPIC_ORDERS)
    assert [m.value["i"] for m in c.poll(3, timeout=0.5)] == [0, 1, 2]
    c.commit()
    c2 = bus.consumer("g", TOPIC_ORDERS)            # a restart picks up where the commit left off
    assert [m.value["i"] for m in c2.poll(10, timeout=0.5)] == [3, 4]


def test_kafka_backend_on_mock_cluster():
    """The Kafka/Redpanda code path, against librdkafka's built-in mock broker."""
    pytest.importorskip("confluent_kafka")
    from confluent_kafka.admin import AdminClient

    from stream.bus import KafkaBus
    mock = AdminClient({"bootstrap.servers": "unused:9092", "test.mock.num.brokers": 1, "log_level": 3})
    broker = next(iter(mock.list_topics(timeout=5).brokers.values()))
    kb = KafkaBus(f"{broker.host}:{broker.port}")
    try:
        kb.publish_many(TOPIC_ORDERS, [{"order_id": f"O{i}"} for i in range(4)], key_field="order_id")
        c = kb.consumer("g", TOPIC_ORDERS)
        got = []
        assert wait_for(lambda: got.extend(c.poll(10, timeout=0.5)) or len(got) >= 4, timeout=15)
        assert [m.value["order_id"] for m in got] == ["O0", "O1", "O2", "O3"]
        c.commit()
        assert c.committed() == 4 and kb.high_watermark(TOPIC_ORDERS) == 4
        c.close()
    finally:
        kb.close()


# ---- micro-batch processing ---------------------------------------------------------------

@pytest.mark.parametrize("scenario,expect_bad", [
    ("rename_column", "all"), ("currency_string", "all"), ("date_format", "all"),
    ("nested_amount", "all"), ("status_case", "all"), ("bad_rows", "some"),
    ("duplicate_rows", "some"), ("additive_field", "none"),
])
def test_bad_records_are_quarantined_good_ones_load(wd, scenario, expect_bad):
    res = process_records(drifted(scenario), LiveCode(wd))
    n_bad, n_good = len(res.failed), 0 if res.good is None else len(res.good)
    if expect_bad == "all":
        assert n_bad == res.rows_in and n_good == 0
    elif expect_bad == "none":
        assert n_bad == 0 and n_good == res.rows_in
    else:
        assert 0 < n_bad < res.rows_in and n_good + n_bad == res.rows_in
    for _, reason in res.failed:
        assert reason["signature"].startswith(("transform:", "contract:"))


def test_the_fix_handles_old_and_new_shapes_in_one_batch(wd):
    apply_fix(wd)
    mixed = generate_orders(n=10, seed=1) + drifted("rename_column", n=10, seed=2)
    res = process_records(mixed, LiveCode(wd))
    assert not res.failed and len(res.good) == 20


def test_signatures_ignore_the_values_that_vary():
    a = normalize_message("could not convert string to float: '$12.50'")
    b = normalize_message("could not convert string to float: '€1,234.00'")
    assert a == b == "could not convert string to float"
    assert normalize_message('time data "10/01/2026" doesn\'t match format "%Y"') == "time data … doesn't match format …"


# ---- processor + replay -------------------------------------------------------------------

def test_quarantine_then_hot_reload_then_replay_recovers_everything(wd, bus):
    m = Metrics()
    proc = StreamProcessor(bus, wd, m).start()
    try:
        bus.publish_many(TOPIC_ORDERS, generate_orders(n=20, seed=5), key_field="order_id")
        bus.publish_many(TOPIC_ORDERS, drifted("rename_column", n=20, seed=6), key_field="order_id")
        assert wait_for(lambda: m.totals["in"] == 40)
        assert proc.sink.count() == 20 and proc.dlq_depth() == 20

        apply_fix(wd)                                    # what a merged PR does
        stats = proc.replay()
        assert stats["replayed"] == 20 and stats["loaded"] == 20 and stats["still_failing"] == 0
        assert proc.code.version == 2 and proc.dlq_depth() == 0 and proc.sink.count() == 40

        assert proc.replay()["replayed"] == 0            # nothing left; replay is idempotent
        bus.publish_many(TOPIC_ORDERS, drifted("rename_column", n=10, seed=7), key_field="order_id")
        assert wait_for(lambda: proc.sink.count() == 50)  # the live stream now runs the fix
    finally:
        proc.stop()


def test_sink_ignores_resent_orders(wd, bus):
    m = Metrics()
    proc = StreamProcessor(bus, wd, m).start()
    try:
        page = generate_orders(n=10, seed=9)
        bus.publish_many(TOPIC_ORDERS, page, key_field="order_id")
        assert wait_for(lambda: proc.sink.count() == 10)
        bus.publish_many(TOPIC_ORDERS, page, key_field="order_id")     # upstream retry, next micro-batch
        assert wait_for(lambda: m.totals["in"] == 20)
        assert proc.sink.count() == 10 and m.totals["sink_dupes"] == 10
    finally:
        proc.stop()


def test_rows_that_still_fail_go_back_to_the_dlq(wd, bus):
    m = Metrics()
    proc = StreamProcessor(bus, wd, m).start()
    try:
        bus.publish_many(TOPIC_ORDERS, drifted("status_case", n=10), key_field="order_id")
        assert wait_for(lambda: proc.dlq_depth() == 10)
        stats = proc.replay(signatures={"contract:status"})               # no fix applied yet
        assert stats["still_failing"] == 10 and stats["still_failing_related"] == 10
        assert proc.dlq_depth() == 10 and bus.high_watermark(TOPIC_DLQ) == 20
    finally:
        proc.stop()


# ---- detector ---------------------------------------------------------------------------

def test_one_incident_per_root_cause_and_rearm_after_it_clears(wd, bus):
    m = Metrics()
    proc = StreamProcessor(bus, wd, m).start()
    det = Detector(proc, m, window_s=2, min_bad=3, silence_s=60)
    try:
        bus.publish_many(TOPIC_ORDERS, generate_orders(n=20, seed=3), key_field="order_id")
        assert wait_for(lambda: m.totals["in"] == 20)
        assert det.check() is None

        bus.publish_many(TOPIC_ORDERS, drifted("rename_column", n=20, seed=4), key_field="order_id")
        assert wait_for(lambda: m.totals["dlq"] == 20)
        report = det.check()
        assert report and report["failed_check"] == "transform" and report["quarantined"] == 20
        assert report["schema_diff"]["added"] == ["client_id"] and report["error"]["type"] == "KeyError"
        assert report["first_failure"] <= time.time()
        assert det.check() is None                       # same root cause: no second incident

        bus.publish_many(TOPIC_ORDERS, generate_orders(n=5, seed=8), key_field="order_id")
        assert wait_for(lambda: m.totals["in"] == 45)
        # the live detector polls every 0.5 s; once the failure has been absent a full window it re-arms
        assert wait_for(lambda: det.check() is None and not det.disarmed, timeout=8)
    finally:
        proc.stop()


def test_silence_is_an_incident(wd, bus):
    m = Metrics()
    proc = StreamProcessor(bus, wd, m)
    det = Detector(proc, m, window_s=5, silence_s=3)
    m.add(ts=time.time() - 10, **{"in": 25}, loaded=25)
    report = det.check()
    assert report["signatures"] == [SILENCE] and report["failed_check"] == "min_rows" and report["rows_in"] == 0
    proc.sink.close()


def test_busy_detector_waits(wd, bus):
    m = Metrics()
    proc = StreamProcessor(bus, wd, m)
    det = Detector(proc, m, window_s=5, silence_s=3)
    m.add(ts=time.time() - 10, **{"in": 25}, loaded=25)
    assert det.check(busy=True) is None
    assert det.check() is not None                       # fires once the open incident closes
    proc.sink.close()
