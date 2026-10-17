"""Message bus: Kafka (Redpanda) in production, an in-process log for tests and demos.

Both backends expose the same small interface, so the producer, the stream
processor and the replayer never know which one they are talking to:

    bus = make_bus()                                   # SHP_BUS=auto|kafka|memory
    bus.publish("orders", {"order_id": ...}, key="ORD-1")
    c = bus.consumer("shp-processor", "orders")       # resumes from the group's committed offset
    for msg in c.poll(max_messages=500, timeout=0.5): ...
    c.commit()                                         # at-least-once: commit after the sink write

Every topic has a single partition. That keeps ordering simple and is plenty for
one machine; scaling out would mean keying by order_id across partitions.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field

# The demo's topic names. The healer itself takes them from healer.yaml (spec.source_topic,
# spec.dlq_topic); these stay for the demo producer, tests and benchmarks.
TOPIC_ORDERS = "orders"
TOPIC_DLQ = "orders.dlq"
TOPICS = [TOPIC_ORDERS, TOPIC_DLQ]

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")


@dataclass
class Message:
    topic: str
    offset: int
    value: dict
    key: str | None = None
    timestamp: float = field(default_factory=time.time)


def _encode(value: dict) -> bytes:
    return json.dumps(value, default=str, ensure_ascii=False).encode("utf-8")


def _decode(raw: bytes | None) -> dict:
    if not raw:
        return {}
    try:
        out = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"_undecodable": raw[:500].decode("utf-8", "replace")}
    return out if isinstance(out, dict) else {"_value": out}


# ---- In-memory backend ------------------------------------------------------------

class MemoryConsumer:
    def __init__(self, bus: "MemoryBus", group: str, topic: str, start: str):
        self.bus, self.group, self.topic = bus, group, topic
        committed = bus._offsets.get((group, topic))
        if committed is not None:
            self._pos = committed
        else:
            self._pos = 0 if start == "earliest" else bus.high_watermark(topic)

    def poll(self, max_messages: int = 500, timeout: float = 0.5, linger: float = 0.05) -> list[Message]:
        log = self.bus._log(self.topic)
        deadline = time.time() + timeout
        with self.bus._cond:
            while len(log) <= self._pos and not self.bus._closed:
                left = deadline - time.time()
                if left <= 0:
                    return []
                self.bus._cond.wait(left)
        time.sleep(linger)           # let the rest of an upstream page arrive
        with self.bus._cond:
            out = log[self._pos:self._pos + max_messages]
            self._pos += len(out)
        return list(out)

    def position(self) -> int:
        return self._pos

    def commit(self) -> None:
        with self.bus._cond:
            self.bus._offsets[(self.group, self.topic)] = self._pos

    def committed(self) -> int:
        return self.bus._offsets.get((self.group, self.topic), 0)

    def close(self) -> None:
        pass


class MemoryBus:
    """A single-process stand-in for Kafka: append-only logs plus committed offsets per group."""

    name = "memory"

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._topics: dict[str, list[Message]] = {}
        self._offsets: dict[tuple[str, str], int] = {}
        self._closed = False

    def _log(self, topic: str) -> list[Message]:
        return self._topics.setdefault(topic, [])

    def ensure_topics(self, topics: list[str]) -> None:
        with self._cond:
            for t in topics:
                self._log(t)

    def publish(self, topic: str, value: dict, key: str | None = None) -> None:
        with self._cond:
            log = self._log(topic)
            # round-trip through JSON so both backends hand consumers identical data
            log.append(Message(topic, len(log), _decode(_encode(value)), key))
            self._cond.notify_all()

    def publish_many(self, topic: str, values: list[dict], key_field: str | None = None) -> None:
        with self._cond:
            log = self._log(topic)
            for v in values:
                k = str(v.get(key_field)) if key_field and isinstance(v, dict) else None
                log.append(Message(topic, len(log), _decode(_encode(v)), k))
            self._cond.notify_all()

    def flush(self, timeout: float = 5.0) -> None:
        pass

    def consumer(self, group: str, topic: str, start: str = "earliest") -> MemoryConsumer:
        return MemoryConsumer(self, group, topic, start)

    def high_watermark(self, topic: str) -> int:
        with self._cond:
            return len(self._log(topic))

    def describe(self) -> str:
        return "in-memory (single process)"

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()


# ---- Kafka / Redpanda backend -----------------------------------------------------

class KafkaConsumer:
    """Assigned (not subscribed) to partition 0, so it starts instantly with no group rebalance.
    Offsets are still committed under the group id, so a restart resumes where it stopped."""

    def __init__(self, bootstrap: str, group: str, topic: str, start: str):
        from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, Consumer, TopicPartition

        self._TP = TopicPartition
        self.topic = topic
        self._c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "log_level": 3,
                            "enable.auto.commit": False, "auto.offset.reset": start,
                            "enable.partition.eof": False})
        tp = TopicPartition(topic, 0)
        committed = self._c.committed([tp], timeout=10)[0].offset
        if committed < 0:
            committed = OFFSET_BEGINNING if start == "earliest" else OFFSET_END
        self._c.assign([TopicPartition(topic, 0, committed)])
        self._pos: int | None = None

    def poll(self, max_messages: int = 500, timeout: float = 0.5, linger: float = 0.0) -> list[Message]:
        out = []
        for m in self._c.consume(num_messages=max_messages, timeout=timeout):
            if m.error():
                continue
            key = m.key().decode("utf-8", "replace") if m.key() else None
            ts = m.timestamp()[1] / 1000 if m.timestamp() and m.timestamp()[1] > 0 else time.time()
            out.append(Message(m.topic(), m.offset(), _decode(m.value()), key, ts))
            self._pos = m.offset() + 1
        return out

    def position(self) -> int:
        if self._pos is not None:
            return self._pos
        return self.committed()

    def commit(self) -> None:
        if self._pos is not None:
            self._c.commit(offsets=[self._TP(self.topic, 0, self._pos)], asynchronous=False)

    def committed(self) -> int:
        off = self._c.committed([self._TP(self.topic, 0)], timeout=10)[0].offset
        return max(off, 0)

    def close(self) -> None:
        self._c.close()


class KafkaBus:
    name = "kafka"

    def __init__(self, bootstrap: str = KAFKA_BOOTSTRAP, **extra):
        from confluent_kafka import Consumer, Producer
        from confluent_kafka.admin import AdminClient

        self.bootstrap = bootstrap
        self._extra = extra                      # e.g. {"test.mock.num.brokers": 1} in tests
        conf = {"bootstrap.servers": bootstrap, "log_level": 3, **extra}
        self._admin = AdminClient(conf)
        self._producer = Producer({**conf, "linger.ms": 5, "enable.idempotence": True})
        self._meta = Consumer({**conf, "group.id": "shp-metadata", "enable.auto.commit": False})
        self._lock = threading.Lock()
        self._meta_lock = threading.Lock()    # one metadata query at a time on the shared handle

    def ensure_topics(self, topics: list[str]) -> None:
        from confluent_kafka.admin import NewTopic

        existing = set(self._admin.list_topics(timeout=10).topics)
        new = [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics if t not in existing]
        if not new:
            return
        for t, fut in self._admin.create_topics(new, operation_timeout=10).items():
            try:
                fut.result(timeout=15)
            except Exception as e:
                # already there, or a broker that only auto-creates on first use: carry on
                if "TOPIC_ALREADY_EXISTS" not in str(e):
                    print(f"[bus] could not create topic {t!r} ({type(e).__name__}); relying on auto-create")

    def publish(self, topic: str, value: dict, key: str | None = None) -> None:
        with self._lock:
            self._producer.produce(topic, value=_encode(value), key=key.encode() if key else None, partition=0)
            self._producer.poll(0)

    def publish_many(self, topic: str, values: list[dict], key_field: str | None = None) -> None:
        with self._lock:
            for v in values:
                k = str(v.get(key_field)) if key_field and isinstance(v, dict) else None
                self._producer.produce(topic, value=_encode(v), key=k.encode() if k else None, partition=0)
            self._producer.poll(0)
        self.flush()

    def flush(self, timeout: float = 5.0) -> None:
        self._producer.flush(timeout)

    def consumer(self, group: str, topic: str, start: str = "earliest") -> KafkaConsumer:
        conf_bootstrap = self.bootstrap
        c = KafkaConsumer(conf_bootstrap, group, topic, start)
        return c

    def high_watermark(self, topic: str) -> int:
        from confluent_kafka import KafkaException, TopicPartition
        with self._meta_lock:
            try:
                _, high = self._meta.get_watermark_offsets(TopicPartition(topic, 0), timeout=5, cached=False)
            except KafkaException:      # topic not created yet
                return 0
        return max(high, 0)

    def describe(self) -> str:
        return f"Kafka API at {self.bootstrap}"

    def close(self) -> None:
        try:
            self._producer.flush(5)
            with self._meta_lock:
                self._meta.close()
        except Exception:
            pass


def kafka_reachable(bootstrap: str = KAFKA_BOOTSTRAP, timeout: float = 3.0) -> bool:
    try:
        from confluent_kafka.admin import AdminClient
    except ImportError:
        return False
    try:
        AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": int(timeout * 1000),
                     "log_level": 0}).list_topics(timeout=timeout)
        return True
    except Exception:
        return False


def make_bus(kind: str | None = None, bootstrap: str | None = None, topics: list[str] | None = None):
    """SHP_BUS=kafka (needs Redpanda), memory (no broker), or auto (kafka if reachable).
    `topics` are created if missing (default: the demo's)."""
    kind = kind or os.environ.get("SHP_BUS", "auto")
    bootstrap = bootstrap or KAFKA_BOOTSTRAP
    if kind == "auto":
        kind = "kafka" if kafka_reachable(bootstrap) else "memory"
    bus = KafkaBus(bootstrap) if kind == "kafka" else MemoryBus()
    bus.ensure_topics(topics or TOPICS)
    return bus
