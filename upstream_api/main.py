"""Fake upstream orders API. You control it, so you can make it drift on demand.

    uvicorn upstream_api.main:app --port 8000

GET    /orders            current batch (with the active drift applied)
GET    /admin/drift       which scenario is active
POST   /admin/drift       {"scenario": "rename_column"} to start drifting
DELETE /admin/drift       back to the original format
"""

from __future__ import annotations

import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from chaos.catalog import list_scenarios, load_scenario
from chaos.control import FileDrift
from chaos.drifts import apply_drift
from chaos.generator import generate_orders

DRIFT = FileDrift()      # the same switch the stream producer reads

app = FastAPI(title="Upstream Orders API (fake)")


def _active() -> str | None:
    return DRIFT.get().get("scenario")


@app.get("/orders")
def orders(n: int = 200):
    seed = int(time.time() // 300)        # a new batch every 5 minutes
    records = generate_orders(n=n, seed=seed)
    scenario = _active()
    if scenario:
        sc = load_scenario(scenario)
        records = apply_drift(records, sc["drift"], sc.get("params"), seed=seed)
    return records


class DriftRequest(BaseModel):
    scenario: str


@app.get("/admin/drift")
def get_drift():
    return {"scenario": _active(), "available": list_scenarios()}


@app.post("/admin/drift")
def set_drift(req: DriftRequest):
    if req.scenario not in list_scenarios():
        raise HTTPException(404, f"unknown scenario {req.scenario!r}")
    DRIFT.set(req.scenario)
    return {"scenario": req.scenario}


@app.delete("/admin/drift")
def clear_drift():
    DRIFT.clear()
    return {"scenario": None}
