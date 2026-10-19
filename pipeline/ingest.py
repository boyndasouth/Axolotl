"""Fetch raw records from the upstream API or a JSON file, and snapshot what arrived."""

from __future__ import annotations

import json
from pathlib import Path

import httpx

from pipeline.config import DATA_DIR, UPSTREAM_URL


def load_records(source: str) -> list[dict]:
    """source: "api", "api:<url>", or "file:<path>"."""
    if source == "api" or source.startswith("api:"):
        url = source[4:] if source.startswith("api:") else UPSTREAM_URL
        resp = httpx.get(f"{url.rstrip('/')}/orders", timeout=10)
        resp.raise_for_status()
        return resp.json()
    if source.startswith("file:"):
        with open(source[5:], encoding="utf-8") as f:
            return json.load(f)
    raise ValueError(f"unknown source {source!r}; use 'api', 'api:<url>' or 'file:<path>'")


def snapshot(records: list[dict], run_id: str, data_dir: Path = DATA_DIR) -> Path:
    """Keep the exact raw batch for each run: the diagnosis agent samples it later."""
    out = data_dir / "raw" / f"orders_{run_id}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(records), encoding="utf-8")
    return out
