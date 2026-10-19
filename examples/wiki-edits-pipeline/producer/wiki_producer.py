"""Relay live Wikipedia edits into a Kafka (Redpanda) topic: the "upstream" the healer watches.

    python producer/wiki_producer.py                          # all wikis -> wiki.edits on localhost:19092
    python producer/wiki_producer.py --wikis enwiki,dewiki    # fewer events per second
    python producer/wiki_producer.py --dry-run                # print instead of publishing (check the feed)
    python producer/wiki_producer.py --record recordings/edits.jsonl   # also save what arrives
    python producer/wiki_producer.py --replay recordings/edits.jsonl   # offline: loop a recording

Source: Wikimedia EventStreams (Server-Sent Events), stream `recentchange`. Wikimedia asks
for a descriptive User-Agent with a way to contact you: pass --contact or set WIKI_CONTACT.

Each event is published as it arrived, with three changes a real ingestion layer makes:
  - `event_id` is stamped from meta.id, so the healer has a top-level key to upsert on
  - `log_params` and `parsedcomment` are dropped (bulky, and log_params changes shape per log type)
  - canary events (meta.domain == "canary", Wikimedia's own health checks) are skipped
Then the drift switch (producer/drift.py) is applied, so a break starts mid-stream.

Needs httpx and confluent-kafka, which the healer project's venv already has.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import drifts  # noqa: E402

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
DROP_FIELDS = ("log_params", "parsedcomment")


# ---- reading the feed ----------------------------------------------------------------------

def parse_sse(lines):
    """Server-Sent Events -> (event id, data dict). Tolerates comments, multi-line data and junk."""
    data, last_id = [], None
    for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            if data:
                try:
                    yield last_id, json.loads("\n".join(data))
                except ValueError:
                    pass
            data = []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "data":
            data.append(value)
        elif field == "id":
            last_id = value


def live_events(user_agent: str, stop):
    """Yield events forever, reconnecting with Last-Event-ID so a dropped connection
    resumes where it left off instead of skipping or repeating edits."""
    import httpx

    last_id, backoff = None, 1.0
    timeout = httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=15.0)
    while not stop():
        headers = {"User-Agent": user_agent, "Accept": "text/event-stream"}
        if last_id:
            headers["Last-Event-ID"] = last_id
        try:
            with httpx.Client(timeout=timeout, follow_redirects=True) as client, \
                    client.stream("GET", STREAM_URL, headers=headers) as resp:
                if resp.status_code != 200:
                    raise RuntimeError(f"HTTP {resp.status_code}: {resp.read()[:200]!r}")
                backoff = 1.0
                for event_id, event in parse_sse(resp.iter_lines()):
                    if event_id:
                        last_id = event_id
                    yield event
                    if stop():
                        return
        except Exception as e:      # network blips are normal on a long-lived stream
            print(f"[producer] stream interrupted ({type(e).__name__}: {str(e)[:120]}); "
                  f"reconnecting in {backoff:.0f}s", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


def replay_events(path: Path, rate: float, stop):
    """Loop a recording at `rate` events/s. Each pass gets fresh ids, or the sink would ignore them."""
    events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not events:
        raise SystemExit(f"{path} has no events")
    loop = 0
    while not stop():
        for e in events:
            e = dict(e)
            base = (e.get("meta") or {}).get("id") or e.get("event_id")
            e["meta"] = {**(e.get("meta") or {}), "id": f"{base}-r{loop}"}
            yield e
            time.sleep(1.0 / rate)
            if stop():
                return
        loop += 1


# ---- shaping -------------------------------------------------------------------------------

def prepare(event: dict, wikis: set[str] | None) -> dict | None:
    meta = event.get("meta") or {}
    if meta.get("domain") == "canary":
        return None
    if wikis and event.get("wiki") not in wikis:
        return None
    if not meta.get("id"):
        return None
    out = {k: v for k, v in event.items() if k not in DROP_FIELDS}
    out["event_id"] = meta["id"]
    return out


# ---- publishing ----------------------------------------------------------------------------

class KafkaSink:
    def __init__(self, bootstrap: str, topic: str):
        from confluent_kafka import Producer
        self.topic = topic
        self.p = Producer({"bootstrap.servers": bootstrap, "linger.ms": 50, "log_level": 3})

    def send(self, events: list[dict]) -> None:
        for e in events:
            self.p.produce(self.topic, key=str(e["event_id"]).encode(),
                           value=json.dumps(e, ensure_ascii=False).encode())
        self.p.poll(0)

    def close(self) -> None:
        self.p.flush(10)


class PrintSink:
    def send(self, events):
        for e in events:
            print(json.dumps({k: e.get(k) for k in ("event_id", "wiki", "type", "title", "user", "timestamp")},
                             ensure_ascii=False), flush=True)

    def close(self):
        pass


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--topic", default=os.environ.get("WIKI_TOPIC", "wiki.edits"))
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"))
    p.add_argument("--wikis", default="", help="comma-separated wiki ids, e.g. enwiki,dewiki (default: all)")
    p.add_argument("--contact", default=os.environ.get("WIKI_CONTACT", ""),
                   help="email or URL for the User-Agent, as Wikimedia's policy asks")
    p.add_argument("--dry-run", action="store_true", help="print events instead of publishing")
    p.add_argument("--record", type=Path, help="append every event (before drift) to this JSONL file")
    p.add_argument("--replay", type=Path, help="loop this JSONL recording instead of the live feed")
    p.add_argument("--rate", type=float, default=25.0, help="events per second when replaying")
    p.add_argument("--max-events", type=int, default=0, help="stop after this many (0 = run forever)")
    a = p.parse_args(argv)

    contact = a.contact or "set --contact or WIKI_CONTACT"
    user_agent = f"self-healing-pipeline-demo/0.2 (data pipeline research; {contact}) python-httpx"
    if not a.contact and not a.replay:
        print("[producer] tip: pass --contact you@example.com; Wikimedia asks for contact info in the "
              "User-Agent and may block anonymous clients", flush=True)
    wikis = {w.strip() for w in a.wikis.split(",") if w.strip()} or None
    sink = PrintSink() if a.dry_run else KafkaSink(a.bootstrap, a.topic)
    recorder = None
    if a.record:
        a.record.parent.mkdir(parents=True, exist_ok=True)
        recorder = a.record.open("a", encoding="utf-8")

    sent = 0
    stop = lambda: bool(a.max_events) and sent >= a.max_events  # noqa: E731
    source = replay_events(a.replay, a.rate, stop) if a.replay else live_events(user_agent, stop)
    rng = random.Random()
    buf: list[dict] = []
    drift, drift_checked = drifts.current(), 0.0
    last_flush = last_report = time.time()
    window = 0
    where = "stdout" if a.dry_run else f"'{a.topic}' on {a.bootstrap}"
    print(f"[producer] {'replaying ' + str(a.replay) if a.replay else 'live Wikipedia edits'} -> {where}"
          + (f" (wikis: {', '.join(sorted(wikis))})" if wikis else ""), flush=True)
    try:
        for raw in source:
            event = prepare(raw, wikis)
            if event is None:
                continue
            now = time.time()
            if now - drift_checked > 1.0:               # the switch can flip mid-stream
                new = drifts.current()
                if new != drift:
                    print(f"[producer] drift {'-> ' + new if new else 'off'}", flush=True)
                drift, drift_checked = new, now
            if recorder:
                recorder.write(json.dumps(event, ensure_ascii=False) + "\n")
            buf.append(drifts.apply(drift, event, rng))
            sent += 1
            window += 1
            if len(buf) >= 200 or now - last_flush >= 0.5:
                sink.send(buf)
                buf, last_flush = [], now
            if now - last_report >= 10 and not a.dry_run:
                print(f"[producer] {window / (now - last_report):.0f} events/s, {sent} total"
                      + (f", drift: {drift}" if drift else ""), flush=True)
                window, last_report = 0, now
            if stop():
                break
    except KeyboardInterrupt:
        pass
    finally:
        if buf:
            sink.send(buf)
        sink.close()
        if recorder:
            recorder.close()
        print(f"[producer] stopped after {sent} events", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
