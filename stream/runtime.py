"""Everything that runs continuously, wired together in one process:

    producer (optional) ─► bus ─► processor ─► sink / DLQ
                                       │
                         detector (every 0.5s, rolling window)
                                       │ report
                         incident manager ─► agent graph ─► PR ─► human decision
                                                                        │
                         rollout hook: apply patch ─► hot reload ─► replay DLQ ─► live check

The control-plane server (stream/server.py) and the streaming benchmark
(bench/stream_harness.py) both drive this same object.
"""

from __future__ import annotations

import json
import queue
import sqlite3
import subprocess
import threading
import time
import uuid
from pathlib import Path

from langgraph.types import Command

import agent.nodes.pr as pr_node
from agent.cli import _line
from agent.config import MAX_ATTEMPTS, REPO_ROOT
from agent.graph import build_graph
from agent.nodes.rollout import set_rollout_hook
from chaos.catalog import list_scenarios, load_scenario
from chaos.control import FileDrift
from healer.spec import load_spec
from stream.bus import make_bus
from stream.detector import Detector
from stream.metrics import Metrics
from stream.processor import StreamProcessor
from stream.producer import Producer

WAITING = "pr_open"
# "merged" is not final here: the rollout (deploy, replay, live check) runs right after it
TERMINAL = {"healthy", "resolved", "rollout_failed", "rejected", "escalated", "interrupted", "error"}


# ---- event + incident store -----------------------------------------------------------

class EventStore:
    """SQLite log of everything that happened, so a refreshed page (or a restarted
    server) can show the full story, and live viewers can resume from an event id."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._con = sqlite3.connect(str(path), check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._con.executescript("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, incident_id TEXT,
                    kind TEXT, data TEXT);
                CREATE INDEX IF NOT EXISTS events_incident ON events(incident_id);
                CREATE TABLE IF NOT EXISTS incidents (
                    id TEXT PRIMARY KEY, created_at REAL, updated_at REAL, status TEXT,
                    kind TEXT, summary TEXT, signatures TEXT, scenario TEXT);
            """)

    def add_event(self, incident_id: str | None, kind: str, data: dict | None = None) -> dict:
        ts = time.time()
        body = json.dumps(data or {}, default=str)
        with self._lock:
            cur = self._con.execute("INSERT INTO events (ts, incident_id, kind, data) VALUES (?,?,?,?)",
                                    (ts, incident_id, kind, body))
            self._con.commit()
            eid = cur.lastrowid
        return {"id": eid, "ts": ts, "incident_id": incident_id, "kind": kind, "data": data or {}}

    def events(self, incident_id: str | None = None, after_id: int = 0, limit: int = 500) -> list[dict]:
        q = "SELECT id, ts, incident_id, kind, data FROM events WHERE id > ?"
        args: list = [after_id]
        if incident_id:
            q += " AND incident_id = ?"
            args.append(incident_id)
        q += " ORDER BY id DESC LIMIT ?"
        args.append(limit)
        with self._lock:
            rows = self._con.execute(q, args).fetchall()
        return [{"id": r[0], "ts": r[1], "incident_id": r[2], "kind": r[3], "data": json.loads(r[4])}
                for r in reversed(rows)]

    def upsert_incident(self, inc: dict) -> None:
        with self._lock:
            self._con.execute("""
                INSERT INTO incidents (id, created_at, updated_at, status, kind, summary, signatures, scenario)
                VALUES (:id, :created_at, :updated_at, :status, :kind, :summary, :signatures, :scenario)
                ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at, status=excluded.status,
                    kind=COALESCE(excluded.kind, kind), summary=COALESCE(excluded.summary, summary)""",
                {"kind": None, "summary": None, "signatures": "[]", "scenario": None, **inc})
            self._con.commit()

    def set_status(self, incident_id: str, status: str) -> None:
        with self._lock:
            self._con.execute("UPDATE incidents SET status=?, updated_at=? WHERE id=?",
                              (status, time.time(), incident_id))
            self._con.commit()

    def incidents(self, limit: int = 100) -> list[dict]:
        with self._lock:
            rows = self._con.execute("SELECT id, created_at, updated_at, status, kind, summary, signatures, "
                                     "scenario FROM incidents ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [{"id": r[0], "created_at": r[1], "updated_at": r[2], "status": r[3], "kind": r[4],
                 "summary": r[5], "signatures": json.loads(r[6] or "[]"), "scenario": r[7]} for r in rows]

    def incident(self, incident_id: str) -> dict | None:
        return next((i for i in self.incidents(1000) if i["id"] == incident_id), None)


# ---- the runtime ----------------------------------------------------------------------

class Runtime:
    def __init__(self, workdir: Path = REPO_ROOT, bus=None, drift=None, data_dir: Path | None = None,
                 embedded_producer: bool | None = None, rate: float = 25.0, sandbox_backend: str | None = None,
                 checkpointer=None, publish=None, live_check_s: float = 5.0,
                 detector_kwargs: dict | None = None):
        """embedded_producer=None follows healer.yaml's `source.demo_producer` (off unless set),
        so pointing the healer at a real topic never publishes fake records into it."""
        self.workdir = Path(workdir)
        self.spec = load_spec(self.workdir)
        if data_dir is None:     # the live checkout honours SHP_DATA_DIR, like the batch pipeline
            from pipeline.config import DATA_DIR
            data_dir = DATA_DIR if self.workdir.resolve() == REPO_ROOT.resolve() else self.workdir / "data"
        self.data_dir = Path(data_dir)
        self.bus = bus or make_bus(topics=[self.spec.source_topic, self.spec.dlq_topic])
        self.drift = drift or FileDrift()
        self.metrics = Metrics()
        self.publish = publish or (lambda event: None)
        self.store = EventStore(self.data_dir / "control_plane.sqlite")
        self.processor = StreamProcessor(self.bus, self.workdir, self.metrics, data_dir=self.data_dir,
                                         on_event=self._stream_event)
        self.detector = Detector(self.processor, self.metrics, **(detector_kwargs or {}))
        if embedded_producer is None:
            embedded_producer = self.spec.demo_producer
        self.producer = Producer(self.bus, drift=self.drift, rate=rate, metrics=self.metrics,
                                 topic=self.spec.source_topic) if embedded_producer else None
        self.sandbox_backend = sandbox_backend
        self.live_check_s = live_check_s
        if checkpointer is None:
            from agent.graph import persistent_checkpointer
            live = self.workdir.resolve() == REPO_ROOT.resolve()
            checkpointer = persistent_checkpointer(None if live else self.data_dir / "agent_checkpoints.sqlite")
        self.graph = build_graph(checkpointer=checkpointer)
        self._jobs: queue.Queue = queue.Queue()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self.started_at = time.time()
        self.detection_paused = False

    # -- lifecycle --
    def start(self) -> "Runtime":
        set_rollout_hook(self._rollout)
        self._recover_after_restart()
        self.processor.start()
        if self.producer:
            self.producer.start()
        for target, name in ((self._detector_loop, "detector"), (self._agent_loop, "agent")):
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads.append(t)
        self.emit(None, "runtime_started", {"bus": self.bus.describe(), "workdir": str(self.workdir),
                                            "pipeline": self.spec.name, "sink": self.processor.sink.describe()})
        return self

    def stop(self) -> None:
        self._stop.set()
        self._jobs.put(None)
        if self.producer:
            self.producer.stop()
        self.processor.stop()
        for t in self._threads:
            t.join(timeout=5)
        set_rollout_hook(None)
        self.bus.close()

    def _recover_after_restart(self) -> None:
        """Incidents that were mid-run when the process stopped can't resume; ones waiting
        for a human can (their graph state is checkpointed in SQLite)."""
        for inc in self.store.incidents(1000):
            if inc["status"] in TERMINAL:
                continue
            snap = self.graph.get_state({"configurable": {"thread_id": inc["id"]}})
            if inc["status"] == WAITING and snap.next:
                continue
            self.store.set_status(inc["id"], "interrupted")

    # -- events --
    def emit(self, incident_id: str | None, kind: str, data: dict | None = None) -> dict:
        ev = self.store.add_event(incident_id, kind, data)
        try:
            self.publish({"type": "event", **ev})
        except Exception:
            pass
        return ev

    def _stream_event(self, kind: str, data: dict) -> None:
        self.emit(None, kind, data)

    # -- chaos --
    def set_drift(self, scenario: str | None) -> dict:
        state = self.drift.set(scenario) if scenario else self.drift.clear()
        info = load_scenario(scenario) if scenario else {}
        self.emit(None, "drift_changed", {"scenario": scenario, "description": info.get("description"),
                                          "expected_outcome": info.get("expected_outcome")})
        return state

    # -- detection --
    def busy(self) -> bool:
        return any(i["status"] not in TERMINAL for i in self.store.incidents(50))

    def _detector_loop(self) -> None:
        while not self._stop.wait(0.5):
            if self.detection_paused:
                continue
            try:
                report = self.detector.check(busy=self.busy())
                if report:
                    self.open_incident(report)
            except Exception as e:
                self.emit(None, "detector_error", {"error": f"{type(e).__name__}: {e}"})

    def open_incident(self, report: dict, scenario: str | None = None) -> str:
        incident_id = time.strftime("inc-%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:4]
        now = time.time()
        batch = self.detector.snapshot_batch(report, self.data_dir / "raw" / f"{self.spec.name}_{incident_id}.json")
        live = self.processor.live_sample.save()      # real rows the fix must keep producing
        report["raw_snapshot"] = batch
        drift = self.drift.get().get("scenario")
        scenario = scenario or drift
        # the held-out golden check needs to know the scenario; real incidents don't have one
        scenario = scenario if scenario in list_scenarios() else None

        from agent.nodes.monitor import classify
        anomaly = classify(report)
        self.store.upsert_incident({"id": incident_id, "created_at": now, "updated_at": now,
                                    "status": "detected", "kind": anomaly["kind"] if anomaly else None,
                                    "summary": anomaly["summary"] if anomaly else None,
                                    "signatures": json.dumps(report.get("signatures", [])),
                                    "scenario": scenario})
        self.emit(incident_id, "incident_opened", {
            "summary": anomaly["summary"] if anomaly else "", "kind": anomaly["kind"] if anomaly else None,
            "signatures": report.get("signatures"), "quarantined": report.get("quarantined"),
            "rows_in": report.get("rows_in"), "window_s": report.get("window_s"),
            "detect_latency_s": round(now - report["first_failure"], 2) if report.get("first_failure") else None})
        first = report.get("first_failure") or now
        init = {"incident_id": incident_id, "workdir": str(self.workdir), "source": "stream",
                "scenario_id": scenario, "sandbox_backend": self.sandbox_backend, "attempts": 0,
                "max_attempts": MAX_ATTEMPTS, "history": [], "run_report": report,
                "incident_batch": batch, "live_sample": str(live) if live else None,
                "llm_cost_usd": 0.0, "llm_calls": 0,
                "timestamps": {"started": first, "first_failure": first, "detected": now}}
        self._jobs.put(("start", incident_id, init))
        return incident_id

    # -- the agent worker: one graph run at a time --
    def decide(self, incident_id: str, merged: bool) -> None:
        cfg = {"configurable": {"thread_id": incident_id}}
        row = self.store.incident(incident_id)
        if not row or row["status"] != WAITING or not self.graph.get_state(cfg).next:
            raise ValueError(f"incident {incident_id} is not waiting for a decision")
        self.emit(incident_id, "decision", {"merged": merged})
        self.store.set_status(incident_id, "merging" if merged else "rejecting")
        self._jobs.put(("resume", incident_id, {"merged": merged}))

    def _agent_loop(self) -> None:
        while not self._stop.is_set():
            job = self._jobs.get()
            if job is None:
                return
            kind, incident_id, payload = job
            cfg = {"configurable": {"thread_id": incident_id}, "recursion_limit": 50}
            inp = payload if kind == "start" else Command(resume=payload)
            try:
                for mode, chunk in self.graph.stream(inp, cfg, stream_mode=["updates", "custom"]):
                    self._on_graph_chunk(incident_id, mode, chunk)
                status = self.graph.get_state(cfg).values.get("status", "error")
                self.store.set_status(incident_id, status)
                self.emit(incident_id, "status", {"status": status})
            except Exception as e:
                self.store.set_status(incident_id, "error")
                self.emit(incident_id, "agent_error", {"error": f"{type(e).__name__}: {e}"[:800]})

    def _on_graph_chunk(self, incident_id: str, mode: str, chunk) -> None:
        if mode == "custom":
            self.emit(incident_id, "progress", chunk if isinstance(chunk, dict) else {"value": chunk})
            return
        for node, upd in chunk.items():
            if node == "__interrupt__":
                self.store.set_status(incident_id, WAITING)
                self.emit(incident_id, "awaiting_decision", {})
                continue
            if not upd:
                continue
            try:
                summary = _line(node, upd)
            except Exception:
                summary = ""
            data = {"node": node, "summary": summary, "status": upd.get("status")}
            if node == "verify":
                data["passed"] = upd["verification"]["passed"]
            if node == "open_pr":
                data["pr_url"] = upd.get("pr_url")
            if upd.get("status"):
                self.store.set_status(incident_id, upd["status"])
            self.emit(incident_id, "node", data)

    # -- rollout: called by the graph's rollout node after a merge --
    def _apply_fix(self, state: dict) -> dict:
        files = state["patch"]["files"]
        incident_id = state["incident_id"]
        if pr_node.PR_MODE == "github":
            p = subprocess.run(["git", "pull", "--ff-only"], cwd=self.workdir, capture_output=True, text=True)
            on_disk = all((self.workdir / rel).exists() and
                          (self.workdir / rel).read_text(encoding="utf-8") == content
                          for rel, content in files.items())
            if p.returncode == 0 and on_disk:
                return {"method": "git pull", "files": sorted(files)}
        for rel, content in files.items():
            (self.workdir / rel).write_text(content, encoding="utf-8")
        method = "written to the working tree"
        if (self.workdir / ".git").exists():
            title = (state.get("diagnosis") or {}).get("fix_plan", "")[:60] or "verified fix"
            msg = f"self-heal: {title}\n\nIncident {incident_id}. Verified in the sandbox; approved by a human."
            p = subprocess.run(["git", "-c", "user.name=self-heal agent", "-c", "user.email=self-heal@localhost",
                                "commit", "-m", msg, "--", *sorted(files)],
                               cwd=self.workdir, capture_output=True, text=True)
            if p.returncode == 0:
                method = "committed to the local repo"
        return {"method": method, "files": sorted(files)}

    def _rollout(self, state: dict) -> dict:
        incident_id = state["incident_id"]
        sigs = set((state.get("run_report") or {}).get("signatures") or [])
        t0 = time.time()
        applied = self._apply_fix(state)
        self.emit(incident_id, "fix_applied", applied)
        replay = self.processor.replay(signatures=sigs or None)
        deployed = self.processor.code.loaded_at or t0
        self.emit(incident_id, "dlq_replayed", replay)
        if self.processor.code.load_error:
            return {"healthy": False, "applied": applied, "replay": replay, "deployed_at": deployed,
                    "error": f"the merged code failed to load: {self.processor.code.load_error}"}

        # watch the live stream on the new code for a few seconds
        self._stop.wait(self.live_check_s)
        live = self.metrics.window(0, since=deployed + 1)
        live_check = {"seconds": self.live_check_s, "in": live["in"], "loaded": live["loaded"],
                      "quarantined": live["dlq"], "reasons": live["reasons"]}
        healthy = replay["still_failing_related"] == 0 and live["dlq"] == 0
        self.emit(incident_id, "live_check", {**live_check, "healthy": healthy})
        return {"healthy": healthy, "applied": applied, "replay": replay, "live_check": live_check,
                "deployed_at": deployed, "recovered_at": time.time(),
                "code_version": self.processor.code.version}

    def replay_now(self) -> dict:
        return self.processor.replay()

    # -- what the console shows --
    def status(self) -> dict:
        w = self.detector.last.get("window") or self.metrics.window(self.detector.window_s)
        drift = self.drift.get()
        return {
            "bus": self.bus.name, "bus_detail": self.bus.describe(),
            "producer": {"embedded": self.producer is not None,
                         "rate": self.producer.rate if self.producer else None},
            "drift": drift, "window_s": self.detector.window_s, "window": w,
            "firing": self.detector.last.get("firing", []),
            "disarmed": sorted(self.detector.disarmed),
            "totals": dict(self.metrics.totals),
            "dlq_depth": self.processor.dlq_depth(),
            "warehouse_rows": self.processor.sink.count(),
            "pipeline": self.spec.name, "source_topic": self.spec.source_topic,
            "sink": self.processor.sink.describe(),
            "live_sample_rows": len(self.processor.live_sample),
            "topics": {self.spec.source_topic: self.bus.high_watermark(self.spec.source_topic),
                       self.spec.dlq_topic: self.bus.high_watermark(self.spec.dlq_topic)},
            "code_version": self.processor.code.version, "code_error": self.processor.code.load_error,
            "busy": self.busy(), "detection_paused": self.detection_paused,
            "pr_mode": pr_node.PR_MODE, "uptime_s": round(time.time() - self.started_at),
            "last_record_age_s": round(time.time() - self.metrics.last_record_ts, 1)
            if self.metrics.last_record_ts else None,
        }
