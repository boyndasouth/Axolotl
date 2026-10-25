"""Control plane: runs the live pipeline and the agents, and serves the console.

    python -m stream.server                  # http://localhost:8100

One process, one worker (the live fan-out to browsers is in memory). It starts the
stream runtime (producer, processor, detector, agent worker), streams everything
that happens to the browser over Server-Sent Events, and takes the human decisions:
inject chaos, approve or reject a fix, replay the dead-letter queue.

GET  /api/status                  pipeline health right now
GET  /api/metrics?seconds=300     per-second throughput history
GET  /api/stream                  SSE: "metrics" every second, "event" for everything else
GET  /api/incidents               list
GET  /api/incidents/{id}          full story: anomaly, diagnosis, attempts, PR, rollout, events
POST /api/incidents/{id}/decision {"merged": true|false}
GET  /api/chaos                   scenarios and the active one
POST /api/chaos                   {"scenario": "rename_column"}   DELETE /api/chaos to clear
POST /api/producer                {"rate": 40}
POST /api/replay                  replay the DLQ through the current code
GET  /api/dlq  /api/warehouse     peek at quarantined records / loaded rows
GET  /api/bench                   benchmark summary (bench/results.duckdb)
POST /github                      GitHub webhook: resumes an incident when its PR closes
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from agent import llm
from agent.config import DIAGNOSIS_MODEL, REPAIR_MODEL, REPO_ROOT, REPORTS_DIR
from chaos.catalog import list_scenarios, load_scenario

WEB_DIST = REPO_ROOT / "web" / "dist"
FAKE_LLM = os.environ.get("SHP_FAKE_LLM", "").lower()


def clean(obj):
    """JSON-safe copy: NaN/inf become null, anything exotic becomes a string."""
    if isinstance(obj, float):
        return None if math.isnan(obj) or math.isinf(obj) else obj
    if isinstance(obj, dict):
        return {str(k): clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [clean(v) for v in obj]
    if obj is None or isinstance(obj, (str, int, bool)):
        return obj
    return str(obj)


class Hub:
    """Fans events out from worker threads to every connected browser."""

    def __init__(self):
        self.loop: asyncio.AbstractEventLoop | None = None
        self.subs: set[asyncio.Queue] = set()

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=2000)
        self.subs.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self.subs.discard(q)

    def _fanout(self, item: dict) -> None:
        for q in list(self.subs):
            try:
                q.put_nowait(item)
            except asyncio.QueueFull:
                pass

    def publish(self, item: dict) -> None:
        """Safe to call from any thread."""
        if self.loop and not self.loop.is_closed():
            self.loop.call_soon_threadsafe(self._fanout, clean(item))


hub = Hub()
state: dict = {}


def rt():
    if "runtime" not in state:
        raise HTTPException(503, "the pipeline is still starting")
    return state["runtime"]


def _full_status(r) -> dict:
    s = r.status()
    scripted = "scripted stand-in (no LLM)"
    s["models"] = {"diagnosis": scripted if FAKE_LLM == "oracle" else DIAGNOSIS_MODEL,
                   "repair": scripted if FAKE_LLM == "oracle" else REPAIR_MODEL}
    return s


async def _metrics_pump() -> None:
    while True:
        await asyncio.sleep(1.0)
        try:
            r = state.get("runtime")
            if r is None:
                continue
            status = await asyncio.to_thread(_full_status, r)
            point = r.metrics.series(1, now=time.time() - 1)[-1]
            hub._fanout(clean({"type": "metrics", "status": status, "point": point}))
        except Exception as e:
            print(f"[server] metrics pump: {type(e).__name__}: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    from stream.runtime import Runtime

    hub.loop = asyncio.get_running_loop()
    if FAKE_LLM == "oracle":
        from bench.oracle import OracleLLM
        llm.set_fake_llm(OracleLLM())
        print("[server] SHP_FAKE_LLM=oracle: a scripted stand-in replaces the LLM (demo mode)")

    def build():
        return Runtime(workdir=Path(os.environ.get("SHP_WORKDIR") or REPO_ROOT), publish=hub.publish,
                       embedded_producer=(os.environ["SHP_EMBEDDED_PRODUCER"] != "0")
                       if os.environ.get("SHP_EMBEDDED_PRODUCER") else None,
                       rate=float(os.environ.get("SHP_RATE", "25")),
                       sandbox_backend=os.environ.get("SHP_SANDBOX_BACKEND") or None).start()

    runtime = await asyncio.to_thread(build)
    state["runtime"] = runtime
    print(f"[server] stream running on {runtime.bus.describe()}; console at http://localhost:"
          f"{os.environ.get('SHP_PORT', '8100')}")
    pump = asyncio.create_task(_metrics_pump())
    try:
        yield
    finally:
        pump.cancel()
        await asyncio.to_thread(runtime.stop)


app = FastAPI(title="Self-healing pipeline control plane", lifespan=lifespan)


# ---- read ---------------------------------------------------------------------------

@app.get("/api/status")
async def status():
    return clean(await asyncio.to_thread(_full_status, rt()))


@app.get("/api/metrics")
async def metrics(seconds: int = 300):
    return clean(rt().metrics.series(min(max(seconds, 10), 900)))


@app.get("/api/events")
async def events(incident_id: str | None = None, after: int = 0, limit: int = 200):
    return clean(rt().store.events(incident_id=incident_id, after_id=after, limit=min(limit, 1000)))


@app.get("/api/incidents")
async def incidents(limit: int = 50):
    return clean(rt().store.incidents(limit))


def _attempt(h: dict) -> dict:
    p, v = h.get("patch") or {}, h.get("verification") or {}
    return {"attempt": h.get("attempt"),
            "patch": {k: p.get(k) for k in ("diff", "rationale", "files_changed", "error")},
            "verification": {k: v.get(k) for k in ("passed", "summary", "unit_tests", "schema_checks",
                                                   "golden_diff_rows", "regressions", "duration_s", "backend")}}


@app.get("/api/incidents/{incident_id}")
async def incident(incident_id: str):
    r = rt()
    row = r.store.incident(incident_id)
    if not row:
        raise HTTPException(404, "no such incident")
    snap = await asyncio.to_thread(r.graph.get_state, {"configurable": {"thread_id": incident_id}})
    v = snap.values or {}
    report = (v.get("anomaly") or {}).get("report") or v.get("run_report") or {}
    pr_body = None
    url = v.get("pr_url") or ""
    if url.startswith("file:"):
        from urllib.parse import urlparse
        from urllib.request import url2pathname
        path = Path(url2pathname(urlparse(url).path))      # works for C:\ paths on Windows too
        if path.exists():
            pr_body = path.read_text(encoding="utf-8")
    esc = REPORTS_DIR / "escalations" / f"{incident_id}.md"
    return clean({
        **row,
        "waiting_for_decision": row["status"] == "pr_open" and bool(snap.next),
        "anomaly": {k: (v.get("anomaly") or {}).get(k) for k in ("kind", "failed_check", "summary", "run_id")},
        "report": {k: report.get(k) for k in ("rows_in", "rows_out", "quarantined", "dlq_rate", "window_s",
                                              "signatures", "schema_diff", "error", "failures",
                                              "failed_check", "silence_s", "raw_snapshot")},
        "diagnosis": v.get("diagnosis"),
        "attempts": [_attempt(h) for h in v.get("history", [])],
        "max_attempts": v.get("max_attempts"),
        "pr_url": v.get("pr_url"), "pr_error": v.get("pr_error"), "pr_body": pr_body,
        "escalation_reason": v.get("escalation_reason"),
        "escalation_body": esc.read_text(encoding="utf-8") if esc.exists() else None,
        "rollout": v.get("rollout"),
        "timestamps": v.get("timestamps", {}),
        "llm_calls": v.get("llm_calls", 0), "llm_cost_usd": v.get("llm_cost_usd", 0.0),
        "events": r.store.events(incident_id=incident_id, limit=500),
    })


@app.get("/api/chaos")
async def chaos():
    out = []
    for sid in list_scenarios():
        sc = load_scenario(sid)
        out.append({"id": sid, "description": sc.get("description", ""),
                    "expected_outcome": sc.get("expected_outcome")})
    return {"active": rt().drift.get(), "scenarios": out}


@app.get("/api/dlq")
async def dlq(limit: int = 50):
    return clean(rt().processor.peek_dlq(min(limit, 500)))


@app.get("/api/warehouse")
async def warehouse(limit: int = 20):
    r = rt()
    rows = await asyncio.to_thread(r.processor.sink.recent, min(limit, 200))
    return clean(rows)


@app.get("/api/bench")
async def bench():
    db = REPO_ROOT / "bench" / "results.duckdb"
    if not db.exists():
        return {"runs": []}

    def read():
        import duckdb
        con = duckdb.connect(str(db), read_only=True)
        try:
            return con.execute("SELECT * FROM runs ORDER BY started_at DESC LIMIT 500").df().to_dict("records")
        finally:
            con.close()
    try:
        return clean({"runs": await asyncio.to_thread(read)})
    except Exception as e:
        return {"runs": [], "error": f"{type(e).__name__}: {e}"}


# ---- live stream ----------------------------------------------------------------------

def _sse(item: dict) -> str:
    if item.get("type") == "event":
        return f"id: {item['id']}\nevent: event\ndata: {json.dumps(item)}\n\n"
    return f"event: {item.get('type', 'message')}\ndata: {json.dumps(item)}\n\n"


@app.get("/api/stream")
async def stream(request: Request, after: int = 0, last_event_id: str | None = Header(default=None)):
    r = rt()
    start = int(last_event_id) if last_event_id and last_event_id.isdigit() else after
    q = hub.subscribe()

    async def gen():
        try:
            yield "retry: 2000\n\n"
            if start:     # catch up on what this viewer missed while disconnected
                for ev in r.store.events(after_id=start, limit=1000):
                    yield _sse(clean({"type": "event", **ev}))
            while True:
                if await request.is_disconnected():
                    break
                try:
                    item = await asyncio.wait_for(q.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
                yield _sse(item)
        finally:
            hub.unsubscribe(q)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ---- act ------------------------------------------------------------------------------

class Decision(BaseModel):
    merged: bool


@app.post("/api/incidents/{incident_id}/decision")
async def decide(incident_id: str, d: Decision):
    try:
        await asyncio.to_thread(rt().decide, incident_id, d.merged)
    except ValueError as e:
        raise HTTPException(409, str(e))
    return {"incident_id": incident_id, "merged": d.merged}


class ChaosRequest(BaseModel):
    scenario: str


@app.post("/api/chaos")
async def inject(req: ChaosRequest):
    if req.scenario not in list_scenarios():
        raise HTTPException(404, f"unknown scenario {req.scenario!r}")
    return rt().set_drift(req.scenario)


@app.delete("/api/chaos")
async def clear_chaos():
    return rt().set_drift(None)


class ProducerRequest(BaseModel):
    rate: float


@app.post("/api/producer")
async def producer(req: ProducerRequest):
    r = rt()
    if not r.producer:
        raise HTTPException(409, "the producer runs as its own process (SHP_EMBEDDED_PRODUCER=0)")
    r.producer.rate = max(1.0, min(req.rate, 500.0))
    r.emit(None, "producer_rate", {"rate": r.producer.rate})
    return {"rate": r.producer.rate}


@app.post("/api/replay")
async def replay():
    r = rt()
    stats = await asyncio.to_thread(r.replay_now)
    return clean(stats)


@app.post("/github")
async def github(request: Request, x_hub_signature_256: str | None = Header(default=None),
                 x_github_event: str | None = Header(default=None)):
    from agent.webhook import BRANCH_PREFIX, _verify
    body = await request.body()
    _verify(body, x_hub_signature_256)
    if x_github_event != "pull_request":
        return {"ignored": "not a pull_request event"}
    payload = json.loads(body)
    pr = payload.get("pull_request", {})
    branch = pr.get("head", {}).get("ref", "")
    if payload.get("action") != "closed" or not branch.startswith(BRANCH_PREFIX):
        return {"ignored": "not a closed self-heal PR"}
    incident_id = branch[len(BRANCH_PREFIX):]
    try:
        await asyncio.to_thread(rt().decide, incident_id, bool(pr.get("merged")))
    except ValueError as e:
        return {"ignored": str(e)}
    return {"incident_id": incident_id, "merged": bool(pr.get("merged"))}


# ---- the console ----------------------------------------------------------------------

if WEB_DIST.exists():
    app.mount("/", StaticFiles(directory=WEB_DIST, html=True), name="console")
else:
    @app.get("/", response_class=HTMLResponse)
    async def no_console():
        return ("<h1>Self-healing pipeline</h1><p>The console hasn't been built. Run "
                "<code>cd web && npm install && npm run build</code>, or use the API at "
                "<a href='/docs'>/docs</a>.</p>")


def main() -> None:
    import uvicorn
    uvicorn.run(app, host=os.environ.get("SHP_HOST", "127.0.0.1"),
                port=int(os.environ.get("SHP_PORT", "8100")), workers=1, log_level="warning")


if __name__ == "__main__":
    main()
