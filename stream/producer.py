"""The upstream, as a stream: publishes order records to the `orders` topic, page by page.

    python -m stream.producer --rate 25          # standalone, against Redpanda (SHP_BUS=kafka)

Upstream sends a page of records about once a second (many real feeds are paged
like this). The drift switch is checked before every page, so when you inject a
scenario the change starts mid-stream, exactly like a real upstream deploy.
"""

from __future__ import annotations

import argparse
import threading
import time

from chaos.catalog import load_scenario
from chaos.control import FileDrift
from chaos.drifts import apply_drift
from chaos.generator import generate_orders
from stream.bus import TOPIC_ORDERS, make_bus


class Producer:
    def __init__(self, bus, drift=None, rate: float = 25.0, page_size: int = 25,
                 max_amount: float = 2000.0, metrics=None, topic: str = TOPIC_ORDERS):
        self.bus = bus
        self.topic = topic
        self.drift = drift or FileDrift()
        self.rate = rate
        self.page_size = page_size
        self.max_amount = max_amount     # some amounts over 1,000, so "€1,234.50" shows up live
        self.metrics = metrics
        # page seeds grow with wall-clock time, so a restarted producer never reuses order ids
        self._seed = int(time.time() * 10) % 10**9
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.pages = 0

    def next_page(self) -> list[dict]:
        self._seed += 1
        records = generate_orders(n=self.page_size, seed=self._seed, max_amount=self.max_amount)
        scenario = self.drift.get().get("scenario")
        if scenario:
            sc = load_scenario(scenario)
            records = apply_drift(records, sc["drift"], sc.get("params"), seed=self._seed)
        return records

    def publish_page(self) -> int:
        records = self.next_page()
        if records:
            self.bus.publish_many(self.topic, records, key_field="order_id")
        self.pages += 1
        if self.metrics is not None:
            self.metrics.add(produced=len(records))
        return len(records)

    def run(self) -> None:
        next_t = time.time()
        while not self._stop.is_set():
            try:
                self.publish_page()
            except Exception as e:          # a broker hiccup must not kill upstream
                print(f"[producer] publish failed: {type(e).__name__}: {e}")
            next_t += self.page_size / max(self.rate, 0.1)
            self._stop.wait(max(0.0, next_t - time.time()))
            if time.time() - next_t > 5:    # fell far behind (laptop slept): don't burst
                next_t = time.time()

    def start(self) -> "Producer":
        self._thread = threading.Thread(target=self.run, name="producer", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=float, default=25.0, help="records per second")
    p.add_argument("--page-size", type=int, default=25)
    p.add_argument("--bus", default="kafka", choices=["kafka", "memory", "auto"])
    a = p.parse_args()
    bus = make_bus(a.bus)
    prod = Producer(bus, rate=a.rate, page_size=a.page_size)
    print(f"producing ~{a.rate:g} orders/s to '{TOPIC_ORDERS}' on {bus.describe()} (Ctrl+C to stop)")
    try:
        prod.run()
    except KeyboardInterrupt:
        pass
    finally:
        bus.close()


if __name__ == "__main__":
    main()
