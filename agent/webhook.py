"""GitHub webhook: resumes a paused incident when its PR is merged or closed.

    uvicorn agent.webhook:app --port 8100

Point a repository webhook (content type application/json, event "Pull requests")
at /github, using GITHUB_WEBHOOK_SECRET as the secret. For local testing,
expose the port with a tunnel such as ngrok or smee.io.
"""

from __future__ import annotations

import hashlib
import hmac
import os

from fastapi import FastAPI, Header, HTTPException, Request
from langgraph.types import Command

from agent.graph import build_graph, persistent_checkpointer

SECRET = os.environ.get("GITHUB_WEBHOOK_SECRET", "")
BRANCH_PREFIX = "self-heal/"

app = FastAPI(title="self-heal webhook")
_graph = None


def graph():
    global _graph
    if _graph is None:
        _graph = build_graph(checkpointer=persistent_checkpointer())
    return _graph


def _verify(body: bytes, signature: str | None) -> None:
    if not SECRET:
        raise HTTPException(500, "GITHUB_WEBHOOK_SECRET is not set")
    expected = "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    if not signature or not hmac.compare_digest(expected, signature):
        raise HTTPException(401, "bad signature")


@app.post("/github")
async def github(request: Request, x_hub_signature_256: str | None = Header(default=None),
                 x_github_event: str | None = Header(default=None)):
    body = await request.body()
    _verify(body, x_hub_signature_256)
    if x_github_event != "pull_request":
        return {"ignored": "not a pull_request event"}
    payload = await request.json()
    pr = payload.get("pull_request", {})
    branch = pr.get("head", {}).get("ref", "")
    if payload.get("action") != "closed" or not branch.startswith(BRANCH_PREFIX):
        return {"ignored": "not a closed self-heal PR"}

    incident_id = branch[len(BRANCH_PREFIX):]
    cfg = {"configurable": {"thread_id": incident_id}}
    if not graph().get_state(cfg).next:
        return {"ignored": f"incident {incident_id} is not waiting"}
    graph().invoke(Command(resume={"merged": bool(pr.get("merged"))}), cfg)
    return {"incident_id": incident_id, "status": graph().get_state(cfg).values["status"]}
